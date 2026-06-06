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

public delegate void RequestWriterDelegate(ref BufferWriter writer, ISerializationContext context);
public delegate TResponse ResponseReaderDelegate<TResponse>(ref BufferReader reader, short version, ISerializationContext context);

public class Connection : IConnection
{
    private readonly ILogger _logger;
    private Socket? _socket;
    private SslStream? _sslStream;
    private Stream? _writeStream;
    private readonly ConnectionConfig _config;

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
        if (!_config.SkipSaslAuthOnOpen)
        {
            await AuthenticateSaslAsync(cancellationToken);
        }
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
            _writeStream = new NetworkStream(_socket!, false);
            return;
        }

        _logger.LogInformation("Starting TLS handshake.");

        X509Certificate2? caCert = null;
        if (_config.Ssl?.SslCaCertPath is string certPath)
        {
            caCert = X509CertificateLoader.LoadCertificateFromFile(certPath);
        }

        _sslStream = new SslStream(
            new NetworkStream(_socket!, false),
            false,
            (sender, certificate, chain, errors) =>
            {
                if (caCert == null)
                {
                    return true;
                }

                if (errors == SslPolicyErrors.None)
                {
                    return true;
                }

                using var x509Chain = new X509Chain();
                x509Chain.ChainPolicy.ExtraStore.Add(caCert);
                x509Chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                x509Chain.ChainPolicy.VerificationFlags =
                    X509VerificationFlags.AllowUnknownCertificateAuthority |
                    X509VerificationFlags.IgnoreInvalidName;

                return x509Chain.Build((X509Certificate2)certificate!);
            });

        var sslOptions = new SslClientAuthenticationOptions
        {
            TargetHost = _config.Host,
            EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
            CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
        };

        await _sslStream.AuthenticateAsClientAsync(sslOptions, cancellationToken);

        _logger.LogInformation("TLS handshake complete. Protocol={Protocol}, Cipher={Cipher}",
            _sslStream.SslProtocol, _sslStream.NegotiatedCipherSuite);

        _writeStream = _sslStream;
    }

    private async ValueTask AuthenticateSaslAsync(CancellationToken cancellationToken)
    {
        var mechanism = _config.Ssl?.SaslMechanism;
        if (string.IsNullOrEmpty(mechanism))
        {
            return;
        }

        _logger.LogInformation("Starting SASL authentication with {Mechanism}.", mechanism);

        // 1. SaslHandshake (inline — no receive loop yet)
        _logger.LogDebug("Sending SaslHandshakeRequest.");
        var handshakeResponse = await SendRequestInlineAsync(
            requestWriter: (ref BufferWriter buf, ISerializationContext ctx) =>
            {
                SaslHandshakeRequestSerializer.Serialize(ref buf, new SaslHandshakeRequest { Mechanism = mechanism }, 0, ctx);
            },
            responseReader: (ref BufferReader buf, short version, ISerializationContext ctx) =>
            {
                return SaslHandshakeResponseSerializer.Deserialize(ref buf, version, ctx);
            },
            apiKey: 17,
            cancellationToken,
            apiVersion: 1);

        if (handshakeResponse.ErrorCode != 0)
        {
            throw new Exception(
                $"SASL handshake failed with error code {handshakeResponse.ErrorCode}");
        }

        var supportedMechanisms = handshakeResponse.Mechanisms;
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
            _config.Ssl!.SaslUsername,
            _config.Ssl!.SaslPassword,
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

    private async Task<TResponse> SendRequestInlineAsync<TResponse>(
        RequestWriterDelegate requestWriter,
        ResponseReaderDelegate<TResponse> responseReader,
        short apiKey,
        CancellationToken cancellationToken,
        short apiVersion = 0)
    {
        var correlationId = IdGenerator.Next();
        var isFlexible = apiVersion >= 2;
        var requestHeaderVersion = isFlexible ? (short)2 : (short)1;
        var responseHeaderVersion = isFlexible ? (short)1 : (short)0;
        var requestHeader = new RequestHeader
        {
            RequestApiKey = apiKey,
            RequestApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = _config.ClientId,
        };

        var writer = new BufferWriter(_arrayPool, _config.RequestBufferSize);
        try
        {
            int start = writer.Position;
            writer.WriteInt(0);

            RequestHeaderSerializer.Serialize(ref writer, requestHeader, requestHeaderVersion, _serializationContext);
            requestWriter(ref writer, _serializationContext);

            int end = writer.Position;
            int size = end - start - 4;
            writer.Position = start;
            writer.WriteInt(size);
            writer.Position = end;

            await _writeStream!.WriteAsync(writer.Memory.Slice(0, writer.Position), cancellationToken);
            await _writeStream.FlushAsync(cancellationToken);
            _logger.LogDebug("Handshake request written ({PayloadSize} bytes). Hex: {Hex}",
                writer.Position, Convert.ToHexString(writer.Memory.Span.Slice(0, writer.Position)));

            var sizeBuf = new byte[4];
            var stream = GetReadStream();
            _logger.LogDebug("Reading handshake response size...");
            await stream.ReadAtLeastAsync(sizeBuf, 4, true, cancellationToken);
            var responseSize = (sizeBuf[0] << 24) | (sizeBuf[1] << 16) | (sizeBuf[2] << 8) | sizeBuf[3];
            _logger.LogDebug("Handshake response size: {ResponseSize}", responseSize);

            var responseBuffer = _arrayPool.Rent(responseSize);
            try
            {
                await stream.ReadAtLeastAsync(
                    responseBuffer, responseSize, true, cancellationToken);

                var buffer = new Memory<byte>(responseBuffer, 0, responseSize);
                var reader = new BufferReader(buffer);
                var responseHeader = ResponseHeaderSerializer.Deserialize(ref reader, responseHeaderVersion, _serializationContext);

                if (responseHeader.CorrelationId != correlationId)
                {
                    throw new Exception(
                        $"Correlation ID mismatch: expected {correlationId}, got {responseHeader.CorrelationId}");
                }

                return responseReader(ref reader, apiVersion, _serializationContext);
            }
            finally
            {
                _arrayPool.Return(responseBuffer);
            }
        }
        finally
        {
            writer.Dispose();
        }
    }

    private async Task<byte[]> SendSaslAuthenticateAsync(
        byte[] authBytes, CancellationToken cancellationToken)
    {
        _logger.LogDebug("SASL authenticate payload ({Len} bytes): {Hex}",
            authBytes.Length, Convert.ToHexString(authBytes));
        var request = new SaslAuthenticateRequest
        {
            AuthBytes = authBytes,
        };

        var correlationId = IdGenerator.Next();
        var apiVersion = _apiVersions.GetValueOrDefault((ApiKey)36, (short)0);

        var effectiveApiVersion = (short)0;
        var isFlexible = effectiveApiVersion >= 2;
        var requestHeaderVersion = isFlexible ? (short)2 : (short)1;
        var responseHeaderVersion = isFlexible ? (short)1 : (short)0;
        var requestHeader = new RequestHeader
        {
            RequestApiKey = 36,
            RequestApiVersion = effectiveApiVersion,
            CorrelationId = correlationId,
            ClientId = _config.ClientId,
        };

        var writer = new BufferWriter(_arrayPool, _config.RequestBufferSize);
        try
        {
            int start = writer.Position;
            writer.WriteInt(0);

            RequestHeaderSerializer.Serialize(ref writer, requestHeader, requestHeaderVersion, _serializationContext);
            SaslAuthenticateRequestSerializer.Serialize(ref writer, request, apiVersion, _serializationContext);

            int end = writer.Position;
            int size = end - start - 4;
            writer.Position = start;
            writer.WriteInt(size);
            writer.Position = end;

            await _writeStream!.WriteAsync(writer.Memory.Slice(0, writer.Position), cancellationToken);
            await _writeStream.FlushAsync(cancellationToken);
            _logger.LogDebug("Auth request written ({PayloadSize} bytes). Hex: {Hex}",
                writer.Position, Convert.ToHexString(writer.Memory.Span.Slice(0, writer.Position)));

            // Read response: size prefix + header + body
            var sizeBuf = new byte[4];
            var stream = GetReadStream();
            _logger.LogDebug("Reading auth response size...");
            try
            {
                await stream.ReadAtLeastAsync(sizeBuf, 4, true, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to read response size. Stream canRead={CanRead}, canWrite={CanWrite}",
                    stream.CanRead, stream.CanWrite);
                throw;
            }
            var responseSize = (sizeBuf[0] << 24) | (sizeBuf[1] << 16) | (sizeBuf[2] << 8) | sizeBuf[3];

            var responseBuffer = _arrayPool.Rent(responseSize);
            try
            {
                await stream.ReadAtLeastAsync(
                    responseBuffer, responseSize, true, cancellationToken);

                var buffer = new Memory<byte>(responseBuffer, 0, responseSize);
                var reader = new BufferReader(buffer);
                var responseHeader = ResponseHeaderSerializer.Deserialize(ref reader, responseHeaderVersion, _serializationContext);

                if (responseHeader.CorrelationId != correlationId)
                {
                    throw new Exception(
                        $"Correlation ID mismatch: expected {correlationId}, got {responseHeader.CorrelationId}");
                }

                var response = SaslAuthenticateResponseSerializer.Deserialize(ref reader, apiVersion, _serializationContext);

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
            writer.Dispose();
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

    public bool SupportsApiKeyVersion(ApiKey apiKey, short minVersion)
    {
        if (_apiVersions.TryGetValue(apiKey, out var maxVersion))
        {
            return maxVersion >= minVersion;
        }

        return false;
    }

    public async ValueTask<IDisposableMessage<TResponse>> SendAsync<TResponse>(
        IRequest<TResponse> request,
        CancellationToken cancellationToken)
    {
        if (_writeStream == null)
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
                    await _writeStream!.WriteAsync(
                        writer.Memory.Slice(0, writer.Position),
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
        if (_receiveBackgroundTask != null)
        {
            await _receiveBackgroundTask;
        }

        if (_writeStream != null)
        {
            await _writeStream.DisposeAsync();
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
