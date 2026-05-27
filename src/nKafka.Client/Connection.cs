using System.Buffers;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using nKafka.Contracts;
using nKafka.Contracts.Exceptions;
using nKafka.Contracts.MessageDefinitions;

namespace nKafka.Client;

public class Connection : IConnection
{
    private readonly ILogger _logger;
    private Socket? _socket;
    private readonly ConnectionConfig _config;

    private SocketWriterStream? _writerStream;
    private CancellationTokenSource? _stop;

    private Task _receiveBackgroundTask = default!;

    private ConcurrentDictionary<int, PendingRequest> _pendingRequests = new();

    private readonly ArrayPool<byte> _arrayPool = ArrayPool<byte>.Shared;

    private readonly SerializationContext _serializationContext;

    private Dictionary<ApiKey, short> _apiVersions = new();

    public Connection(ConnectionConfig config, ILoggerFactory loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(loggerFactory);
        _config = config;
        _logger = loggerFactory.CreateLogger<Connection>();
        _serializationContext = new SerializationContext(_arrayPool)
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
        _writerStream = new SocketWriterStream(_socket);
    }

    private void StartReceiving()
    {
        if (_socket == null)
        {
            return;
        }

        _stop = new CancellationTokenSource();
        var cancellationToken = _stop.Token;

        byte[] sizeBuffer = new byte[4];

        _receiveBackgroundTask = Task.Run(
            async () =>
            {
                try
                {
                    await using var stream = new NetworkStream(_socket);
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
            throw new Exception($"Failed to choose API versions. Error code: {response.Message.ErrorCode}");
        }

        var brokerApiVersions = response.Message.ApiKeys;
        if (brokerApiVersions == null || !brokerApiVersions.Any())
        {
            throw new Exception($"Failed to choose API versions. Empty ApiKeys collection in response.");
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
                byte[] payload = _arrayPool.Rent(_config.RequestBufferSize);
                try
                {
                    _pendingRequests.TryAdd(pendingRequest.CorrelationId, pendingRequest);

                    _logger.LogDebug("Serializing.");
                    var writer = new BufferWriter(new Memory<byte>(payload, 0, payload.Length));
                    pendingRequest.SerializeRequest(ref writer, _serializationContext);

                    _logger.LogDebug("Sending {@size} bytes.", writer.Position);
                    await _writerStream.WriteAsync(payload, 0, (int)writer.Position,
                        pendingRequest.CancellationToken);
                    _logger.LogDebug("Sent.");
                }
                catch (Exception exception)
                {
                    pendingRequest.Response.TrySetException(exception);
                }
                finally
                {
                    _arrayPool.Return(payload);
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

        _socket.Shutdown(SocketShutdown.Both);
        _socket.Dispose();
        _socket = null;
    }

    private class SerializationContext(ArrayPool<byte> arrayPool) : ISerializationContext
    {
        public required SerializationConfig Config { get; init; }

        public BufferWriter CreateWriter(int size = 4096)
        {
            return new BufferWriter(arrayPool, size);
        }
    }
}
