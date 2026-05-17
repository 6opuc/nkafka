using System.Buffers;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using nKafka.Client.Auth;
using nKafka.Contracts;
using nKafka.Contracts.Exceptions;
using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageSerializers;

namespace nKafka.Client;

public class Connection : IConnection
{
    private readonly ILogger _logger;
    private Socket? _socket;
    private SslStream? _sslStream;
    private readonly ConnectionConfig _config;

    private SocketWriterStream? _writerStream;
    private CancellationTokenSource? _stop;

    private Task _receiveBackgroundTask = default!;

    private ConcurrentDictionary<int, PendingRequest> _pendingRequests = new();

    private readonly ArrayPool<byte> _arrayPool = ArrayPool<byte>.Shared;
    private readonly int _bufferSize;

    private readonly SerializationContext _serializationContext;

    private Dictionary<ApiKey, short> _apiVersions = new();

    public Connection(ConnectionConfig config, ILoggerFactory loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(loggerFactory);
        _config = config;
        _bufferSize = Math.Max(_config.RequestBufferSize, _config.ResponseBufferSize);
        _logger = loggerFactory.CreateLogger<Connection>();
        _serializationContext = new SerializationContext(_arrayPool, _bufferSize)
        {
            Config = new SerializationConfig
            {
                ClientId = _config.ClientId,
                CheckCrcs = _config.CheckCrcs,
            }
        };
    }

    public async ValueTask OpenAsync(CancellationToken cancellationToken)
    {
        using var _ = BeginDefaultLoggingScope();
        await OpenSocketAsync(cancellationToken);
        await AuthenticateTlsAsync(cancellationToken);
        await AuthenticateSaslAsync(cancellationToken);
        StartReceiving();
        await RequestApiVersionsAsync(cancellationToken);
    }

    private IDisposable? BeginDefaultLoggingScope()
    {
        return _logger.BeginScope($"{_config.Host}:{_config.Port}");
    }

    private async ValueTask OpenSocketAsync(CancellationToken cancellationToken)
    {
        if (_config == null)
        {
            throw new InvalidOperationException("Connection is not configured.");
        }
        if (_socket != null)
        {
            throw new InvalidOperationException("Socket connection is already open.");
        }

        _logger.LogInformation("Opening socket connection.");

        var ip = await Dns.GetHostAddressesAsync(_config.Host, cancellationToken);
        if (ip.Length == 0)
        {
            throw new InvalidOperationException("Unable to resolve host.");
        }
        var endpoint = new IPEndPoint(ip.First(), _config.Port);
        _socket = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        _socket.ReceiveBufferSize = _config.ResponseBufferSize;
        _socket.SendBufferSize = _config.RequestBufferSize;
        _socket.NoDelay = true;
        await _socket.ConnectAsync(endpoint, cancellationToken);
    }

    private async ValueTask AuthenticateTlsAsync(CancellationToken cancellationToken)
    {
        var protocol = _config.Protocol;
        if (protocol != "SASL_SSL" && protocol != "SSL")
        {
            _writerStream = SocketWriterStream.FromSocket(_socket!);
            return;
        }

        _logger.LogInformation("Starting TLS handshake.");

        X509CertificateCollection? clientCertificates = null;
        X509RevocationMode revocationMode = X509RevocationMode.NoCheck;

        X509Certificate2Collection? caCerts = null;
        if (_config.SslCaCertPath != null)
        {
            caCerts = new X509Certificate2Collection();
            caCerts.ImportFromPemFile(_config.SslCaCertPath);
        }

        _sslStream = new SslStream(
            new NetworkStream(_socket!, false),
            false,
            (sender, certificate, chain, errors) =>
            {
                if (caCerts == null || caCerts.Count == 0)
                {
                    return true;
                }

                using var x509Chain = new X509Chain();
                x509Chain.ChainPolicy.ExtraStore.AddRange(caCerts);
                x509Chain.ChainPolicy.RevocationMode = revocationMode;
                x509Chain.ChainPolicy.VerificationFlags =
                    X509VerificationFlags.IgnoreInvalidName;

                return x509Chain.Build((X509Certificate2)certificate!);
            });

        await _sslStream.AuthenticateAsClientAsync(
            _config.Host,
            clientCertificates,
            SslProtocols.Tls12 | SslProtocols.Tls13,
            checkCertificateRevocation: false);

        _writerStream = new SocketWriterStream(_sslStream);
    }

    private async ValueTask AuthenticateSaslAsync(CancellationToken cancellationToken)
    {
        var mechanism = _config.SaslMechanism;
        if (string.IsNullOrEmpty(mechanism))
        {
            return;
        }

        _logger.LogInformation("Starting SASL authentication with {Mechanism}.", mechanism);

        // 1. SaslHandshake
        _logger.LogDebug("Sending SaslHandshakeRequest.");
        var handshakeRequest = new SaslHandshakeRequest
        {
            Mechanism = mechanism,
        };
        using var handshakeResponse = await SendAsync(handshakeRequest, cancellationToken);
        if (handshakeResponse.Message.ErrorCode != 0 && handshakeResponse.Message.ErrorCode != 0)
        {
            throw new Exception(
                $"SASL handshake failed with error code {handshakeResponse.Message.ErrorCode}");
        }

        var supportedMechanisms = handshakeResponse.Message.Mechanisms;
        if (supportedMechanisms == null ||
            !supportedMechanisms.Any(m => m == mechanism))
        {
            var supported = supportedMechanisms != null
                ? string.Join(", ", supportedMechanisms)
                : "none";
            throw new Exception(
                $"Server does not support {mechanism}. Supported: {supported}");
        }

        // 2. SASL Authenticate (SCRAM-SHA-512 exchange)
        HashAlgorithmName hashAlgorithm;
        if (mechanism == "SCRAM-SHA-512")
            hashAlgorithm = HashAlgorithmName.SHA512;
        else if (mechanism == "SCRAM-SHA-256")
            hashAlgorithm = HashAlgorithmName.SHA256;
        else
            throw new NotSupportedException($"SASL mechanism {mechanism} is not supported");

        var scramClient = new ScramClient(
            _config.SaslUsername!,
            _config.SaslPassword!,
            hashAlgorithm);

        // Round 1: Client-First → Server-First
        var clientFirstBytes = scramClient.GetClientFirstMessage();
        _logger.LogDebug("Sending SASL authenticate (client-first).");
        var serverFirstBytes = await SendSaslAuthenticateAsync(
            clientFirstBytes, cancellationToken);

        // Round 2: Client-Final → Server-Final
        var clientFinalBytes = scramClient.GetClientFinalMessage(serverFirstBytes);
        _logger.LogDebug("Sending SASL authenticate (client-final).");
        var serverFinalBytes = await SendSaslAuthenticateAsync(
            clientFinalBytes, cancellationToken);

        scramClient.VerifyServerFinalMessage(serverFinalBytes);
        _logger.LogInformation("SASL authentication successful.");
    }

    private async Task<byte[]> SendSaslAuthenticateAsync(
        byte[] authBytes, CancellationToken cancellationToken)
    {
        var request = new SaslAuthenticateRequest
        {
            AuthBytes = authBytes,
        };

        var correlationId = IdGenerator.Next();
        var apiVersion = _apiVersions.GetValueOrDefault((ApiKey)36, (short)0);

        var headerVersion = apiVersion >= 2 ? (short)2 : (short)1;
        var requestHeader = new RequestHeader
        {
            RequestApiKey = 36,
            RequestApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = _config.ClientId,
        };

        var rentSize = Math.Max(_config.RequestBufferSize, 4096);
        var buffer = _arrayPool.Rent(rentSize);
        try
        {
            using var output = new MemoryStream(buffer, 0, buffer.Length, true, true);

            PrimitiveSerializer.SerializeInt(output, 0); // placeholder for size
            var payloadStart = output.Position;

            RequestHeaderSerializer.Serialize(output, requestHeader, headerVersion, _serializationContext);
            SaslAuthenticateRequestSerializer.Serialize(output, request, apiVersion, _serializationContext);

            var payloadEnd = output.Position;
            var payloadSize = (int)(payloadEnd - payloadStart);
            output.Position = payloadStart - 4;
            PrimitiveSerializer.SerializeInt(output, payloadSize);
            output.Position = payloadEnd;

            await _writerStream!.WriteAsync(
                new ReadOnlyMemory<byte>(buffer, 0, (int)payloadEnd), cancellationToken);

            // Read response: size prefix + header + body
            var sizeBuf = new byte[4];
            var stream = GetReadStream();
            await stream.ReadAtLeastAsync(sizeBuf, 4, true, cancellationToken);
            var responseSize = (sizeBuf[0] << 24) | (sizeBuf[1] << 16) | (sizeBuf[2] << 8) | sizeBuf[3];

            var responseBuffer = _arrayPool.Rent(responseSize);
            try
            {
                await stream.ReadAtLeastAsync(
                    responseBuffer, responseSize, true, cancellationToken);

                using var input = new MemoryStream(
                    responseBuffer, 0, responseSize, false, true);
                var responseHeader = ResponseHeaderSerializer.Deserialize(
                    input, headerVersion, _serializationContext);

                if (responseHeader.CorrelationId != correlationId)
                {
                    throw new Exception(
                        $"Correlation ID mismatch: expected {correlationId}, got {responseHeader.CorrelationId}");
                }

                var response = SaslAuthenticateResponseSerializer.Deserialize(
                    input, apiVersion, _serializationContext);

                if (response.ErrorCode != 0)
                {
                    var errorMsg = response.ErrorMessage ?? $"Error code {response.ErrorCode}";
                    throw new Exception($"SASL authentication failed: {errorMsg}");
                }

                return response.AuthBytes?.ToArray() ?? [];
            }
            finally
            {
                _arrayPool.Return(responseBuffer);
            }
        }
        finally
        {
            _arrayPool.Return(buffer);
        }
    }

    private Stream GetReadStream()
    {
        return _sslStream ?? (Stream)new NetworkStream(_socket!, false);
    }

    private void StartReceiving()
    {
        if (_socket == null)
        {
            return;
        }

        _stop = new CancellationTokenSource();
        var cancellationToken = _stop.Token;

        var sizeBuffer = new byte[4];

        _receiveBackgroundTask = Task.Run(
            async () =>
            {
                try
                {
                    var stream = GetReadStream();
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        int bytesRead = await stream.ReadAtLeastAsync(
                            sizeBuffer,
                            sizeBuffer.Length,
                            true,
                            cancellationToken);

                        int? payloadSize = GetIntFromByteArray(sizeBuffer);
                        if (payloadSize == null)
                        {
                            throw new EndOfStreamException("Received unexpected response size.");
                        }

                        _logger.LogDebug("Receiving payload ({@payloadSize} bytes).", payloadSize);

                        byte[] payload = _arrayPool.Rent(payloadSize.Value);
                        try
                        {
                            int read = await stream.ReadAtLeastAsync(
                                payload,
                                payloadSize.Value,
                                true,
                                cancellationToken);
                            if (read != payloadSize)
                            {
                                throw new EndOfStreamException("Received unexpected end of stream.");
                            }

                            _logger.LogDebug("Read response payload ({@payloadSize} bytes).", payloadSize);

                            int? correlationId = GetIntFromByteArray(payload);
                            if (correlationId == null ||
                                !_pendingRequests.TryRemove(correlationId.Value, out var pendingRequest))
                            {
                                _arrayPool.Return(payload);
                                _logger.LogError(
                                    $"Received unexpected response: no pending request was found. {correlationId}");
                                continue;
                            }

                            using (BeginRequestLoggingScope(pendingRequest))
                            {
                                _logger.LogDebug("Deserializing response.");

                                try
                                {
                                    var buffer = new Memory<byte>(payload, 0, payloadSize.Value);
                                    var reader = new BufferReader(buffer);
                                    object response = pendingRequest.DeserializeResponse(ref reader, _serializationContext);

                                    if (reader.Remaining != 0)
                                    {
                                        throw new IncompleteMessageException(
                                            $"Response has unconsumed data: {reader.Remaining} bytes remaining after deserialization.");
                                    }

                                    _logger.LogDebug("Deserialized.");

                                    var disposableResponse =
                                        new MessageWithPooledPayload(response, _arrayPool, payload);
                                    pendingRequest.Response.TrySetResult(disposableResponse);
                                }
                                catch (Exception exception)
                                {
                                    _arrayPool.Return(payload);
                                    pendingRequest.Response.TrySetException(exception);
                                }
                            }
                        }
                        catch
                        {
                            _arrayPool.Return(payload);
                            throw;
                        }

                    }
                }
                catch when (cancellationToken.IsCancellationRequested)
                {
                    // Shutdown in progress.
                }
                catch (OperationCanceledException)
                {
                }
                catch (EndOfStreamException endOfStreamException)
                {
                    // Network stream is closed.
                    // Cancel all pending requests.

                    foreach (var pendingRequest in _pendingRequests.Values)
                    {
                        pendingRequest.Response.SetException(endOfStreamException);
                    }
                    _pendingRequests.Clear();

                    // TODO: introduce a state-machine and do a transition to a failed state,
                    // rejecting all incoming requests.
                }
                catch (Exception exception)
                {
                    _logger.LogError(exception, "Unhandled exception in receive loop.");
                }
            }, cancellationToken);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int? GetIntFromByteArray(byte[] payload)
    {
        if (payload.Length < 4)
        {
            return null;
        }
        return
            (payload[0] << 24) |
            (payload[1] << 16) |
            (payload[2] << 8) |
            payload[3];
    }

    private async Task RequestApiVersionsAsync(CancellationToken cancellationToken)
    {
        var clientApiVersions = ApiVersions.ValidVersions;
        _apiVersions = new(clientApiVersions.Select(x =>
            new KeyValuePair<ApiKey, short>(x.Key, x.Value.From ?? 0)));

        if (!_config.RequestApiVersionsOnOpen)
        {
            return;
        }

        _logger.LogInformation("Requesting API versions.");

        var request = new ApiVersionsRequest
        {
            ClientSoftwareName = "nKafka.Client",
            ClientSoftwareVersion = ClientVersionGetter.Version,
        };
        using var response = await SendAsync(request, cancellationToken);
        if (response.Message.ErrorCode != 0)
        {
            throw new ProtocolException($"Failed to choose API versions. Error code: {response.Message.ErrorCode}");
        }

        var brokerApiVersions = response.Message.ApiKeys;
        if (brokerApiVersions == null || !brokerApiVersions.Any())
        {
            throw new ProtocolException($"Failed to choose API versions. Empty ApiKeys collection in response.");
        }

        foreach (var clientApiVersionRange in clientApiVersions)
        {
            if (!brokerApiVersions.TryGetValue((short)clientApiVersionRange.Key, out var apiVersion))
            {
                continue;
            }

            var brokerApiVersionRange = new VersionRange(
                apiVersion.MinVersion!.Value, apiVersion.MaxVersion!.Value);
            var intersection = clientApiVersionRange.Value.Intersect(brokerApiVersionRange);
            if (!intersection.IsNone)
            {
                _apiVersions[clientApiVersionRange.Key] = intersection.To!.Value;
            }
        }
    }

    public async ValueTask<IDisposableMessage<TResponse>> SendAsync<TResponse>(
        IRequest<TResponse> request,
        CancellationToken cancellationToken)
    {
        if (_writerStream == null)
        {
            throw new InvalidOperationException("Socket connection is not open.");
        }

        using (BeginDefaultLoggingScope())
        {
            var completionPromise = new TaskCompletionSource<MessageWithPooledPayload>();
            var pendingRequest = new PendingRequest(
                request,
                completionPromise,
                cancellationToken,
                IdGenerator.Next(),
                _apiVersions.GetValueOrDefault(request.ApiKey, (short)0));
            await using var cancellationRegistration = cancellationToken.Register(
                () =>
                {
                    completionPromise.TrySetCanceled();
                    _pendingRequests.TryRemove(pendingRequest.CorrelationId, out _);
                });

            using (BeginRequestLoggingScope(pendingRequest))
            {
                _pendingRequests.TryAdd(pendingRequest.CorrelationId, pendingRequest);

                _logger.LogDebug("Serializing.");
                var writer = new BufferWriter(_arrayPool, _config.RequestBufferSize);
                try
                {
                    pendingRequest.SerializeRequest(ref writer, _serializationContext);

                    _logger.LogDebug("Sending {@size} bytes.", writer.Position);
                    await _writerStream.WriteAsync(writer.Memory.Slice(0, writer.Position),
                        pendingRequest.CancellationToken);
                    _logger.LogDebug("Sent.");
                }
                finally
                {
                    writer.Dispose();
                }
            }

            var response = await completionPromise.Task;
            return new DisposableMessage<TResponse>(response, pendingRequest.ApiVersion);
        }
    }

    private class DisposableMessage<T>(MessageWithPooledPayload message, short version) : IDisposableMessage<T>
    {
        public T Message => (T)message.Message;
        public short Version => version;

        public void Dispose()
        {
            message.Dispose();
        }
    }

    private IDisposable? BeginRequestLoggingScope(PendingRequest request)
    {
        return _logger.BeginScope($"request #{request.CorrelationId}({request.Payload.GetType().Name})");
    }

    public async ValueTask DisposeAsync()
    {
        if (_socket == null)
        {
            return;
        }

        using var _ = BeginDefaultLoggingScope();

        _logger.LogInformation("Closing socket connection.");

        if (_stop != null)
        {
            await _stop.CancelAsync();
        }
        await _receiveBackgroundTask;

        if (_writerStream != null)
        {
            await _writerStream.DisposeAsync();
        }

        if (_sslStream != null)
        {
            await _sslStream.DisposeAsync();
        }

        _socket.Shutdown(SocketShutdown.Both);
        _socket.Dispose();
        _socket = null;
    }

    private class SerializationContext(ArrayPool<byte> arrayPool, int bufferSize) : ISerializationContext
    {
        public required SerializationConfig Config { get; init; }

        public BufferWriter CreateWriter()
        {
            return new BufferWriter(arrayPool, bufferSize);
        }
    }
}
