using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks.Dataflow;
using Microsoft.Extensions.Logging;
using nKafka.Contracts;

namespace nKafka.Client;

public class Connection : IConnection
{
    private readonly ILogger _logger;
    private Socket? _socket;
    private readonly ConnectionConfig _config;
    
    private SocketWriterStream? _writerStream;
    private CancellationTokenSource? _signalNoMoreDataToWrite;
    private CancellationTokenSource? _signalNoMoreDataToRead;

    private readonly Pipe _pipe = new Pipe(new PipeOptions(
        useSynchronizationContext: false));

    private Task _receiveBackgroundTask = default!;
    private Task _processResponseBackgroundTask = default!;

    private BufferBlock<PendingRequest> _requestQueue = new();
    private Task _sendBackgroundTask = default!;
    private ConcurrentQueue<PendingRequest> _pendingRequests = new();
    
    private readonly ArrayPool<byte> _arrayPool = ArrayPool<byte>.Shared;
    
    private readonly SerializationContext _serializationContext;
    
    private Dictionary<ApiKey, short> _apiVersions = new ();

    public Connection(ConnectionConfig config, ILoggerFactory loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(loggerFactory);
        _config = config;
        _logger = loggerFactory.CreateLogger<Connection>();
        var bufferSize = Math.Max(_config.RequestBufferSize, _config.ResponseBufferSize);
        _serializationContext = new SerializationContext(_arrayPool, bufferSize)
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
        
        StartProcessing();
        StartReceiving();
        StartSending();
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
        await _socket.ConnectAsync(endpoint, cancellationToken);
    }
    
    private void StartProcessing()
    {
        if (_socket == null)
        {
            return;
        }
        
        _signalNoMoreDataToRead = new CancellationTokenSource();
        var cancellationToken = _signalNoMoreDataToRead.Token;

        _processResponseBackgroundTask = Task.Run(
            async () =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var payloadSize = await _pipe.Reader.ReadIntAsync(cancellationToken);
                        _logger.LogDebug("Received response ({@payloadSize} bytes).", payloadSize);

                        var payload = _arrayPool.Rent(payloadSize);
                        try
                        {
                            var read = await _pipe.Reader.ReadAsync(payload, payloadSize, cancellationToken);
                            if (read != payloadSize)
                            {
                                throw new EndOfStreamException("Received unexpected end of stream.");
                            }
                            _logger.LogDebug("Read response payload ({@payloadSize} bytes).", payloadSize);

                            if (!_pendingRequests.TryDequeue(out var pendingRequest))
                            {
                                _arrayPool.Return(payload);
                                _logger.LogError("Received unexpected response: no pending requests.");
                                continue;
                            }

                            using (BeginRequestLoggingScope(pendingRequest.CorrelationId))
                            {
                                _logger.LogDebug("Deserializing response.");

                                try
                                {
                                    using var input = new MemoryStream(payload, 0, payloadSize, false, true);
                                    var response = pendingRequest.DeserializeResponse(input, _serializationContext);
                                    if (input.Length != input.Position)
                                    {
                                        _logger.LogError(
                                            "Received unexpected response length. Expected {@expectedLength}, but got {@actualLength}.",
                                            input.Position,
                                            input.Length);
                                    }
                                    _logger.LogDebug("Deserialized.");

                                    var disposableResponse =
                                        new MessageWithPooledPayload(response, _arrayPool, payload);
                                    pendingRequest.Response.SetResult(disposableResponse);
                                }
                                catch (Exception exception)
                                {
                                    _arrayPool.Return(payload);
                                    pendingRequest.Response.SetException(exception);
                                }
                            }
                        }
                        catch
                        {
                            _arrayPool.Return(payload);
                            throw;
                        }

                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
                _logger.LogDebug("Response processing was stopped.");
            }, cancellationToken);
    }

    private void StartReceiving()
    {
        if (_socket == null)
        {
            return;
        }
        
        _signalNoMoreDataToWrite = new CancellationTokenSource();
        var cancellationToken = _signalNoMoreDataToWrite.Token;

        _receiveBackgroundTask = Task.Run(
            async () =>
            {
                var writer = _pipe.Writer;
                try
                {
                    FlushResult result;
                    do
                    {
                        var memory = writer.GetMemory(_config.ResponseBufferSize);
                        var bytesRead = await _socket.ReceiveAsync(
                            memory,
                            SocketFlags.None,
                            cancellationToken);

                        if (bytesRead == 0)
                        {
                            break;
                        }

                        _logger.LogDebug("Received {bytesRead} bytes.", bytesRead);
                        writer.Advance(bytesRead);

                        result = await writer
                            .FlushAsync(cancellationToken);
                    } while (result.IsCanceled == false &&
                             result.IsCompleted == false);
                }
                catch when (_signalNoMoreDataToWrite.IsCancellationRequested)
                {
                    // Shutdown in progress
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception ex)
                {
                    await writer.CompleteAsync(ex);
                    throw;
                }
                finally
                {
                    await _signalNoMoreDataToWrite.CancelAsync();
                }

                await writer.CompleteAsync();
            }, cancellationToken);
    }

    private void StartSending()
    {
        if (_socket == null)
        {
            return;
        }
        
        _writerStream = new SocketWriterStream(_socket);

        _sendBackgroundTask = Task.Run(
            async () =>
            {
                while (!_requestQueue.Completion.IsCompleted)
                {
                    PendingRequest? request = null;
                    try
                    {
                        request = await _requestQueue.ReceiveAsync();
                    }
                    catch (InvalidOperationException)
                    {
                        _logger.LogDebug("No more requests to send.");
                        return;
                    }

                    var payload = _arrayPool.Rent(_config.RequestBufferSize);
                    try
                    {
                        using (BeginRequestLoggingScope(request.CorrelationId))
                        {
                            _logger.LogDebug("Processing.");
                            _pendingRequests.Enqueue(request);

                            _logger.LogDebug("Serializing.");
                            using var output = new MemoryStream(payload, 0, payload.Length, true, true);
                            request.SerializeRequest(output, _serializationContext);

                            _logger.LogDebug("Sending {@size} bytes.", output.Position);
                            await _writerStream.WriteAsync(output.GetBuffer(), 0, (int)output.Position, request.CancellationToken);
                            _logger.LogDebug("Sent.");
                        }
                    }
                    catch (Exception exception)
                    {
                        request.Response.SetException(exception);
                    }
                    finally
                    {
                        _arrayPool.Return(payload);
                    }
                }
            });
    }

    public async ValueTask<IDisposableMessage<TResponse>> SendAsync<TResponse>(
        IRequest<TResponse> request,
        CancellationToken cancellationToken)
    {
        using (BeginDefaultLoggingScope())
        {
            var completionPromise = new TaskCompletionSource<MessageWithPooledPayload>();
            
            var pendingRequest = new PendingRequest(
                request,
                completionPromise,
                cancellationToken,
                IdGenerator.Next(),
                _apiVersions.GetValueOrDefault(request.ApiKey, (short)0));
            using (BeginRequestLoggingScope(pendingRequest.CorrelationId))
            {
                _logger.LogDebug("Enqueueing {request}.", pendingRequest.Payload.GetType().Name);
                await _requestQueue.SendAsync(pendingRequest, cancellationToken);
                _logger.LogDebug("Enqueued.");
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
    
    private IDisposable? BeginRequestLoggingScope(int correlationId)
    {
        return _logger.BeginScope($"request #{correlationId}");
    }

    public async ValueTask DisposeAsync()
    {
        if (_socket == null)
        {
            return;
        }
        
        using var _ = BeginDefaultLoggingScope();
        
        _logger.LogInformation("Closing socket connection.");
        
        _requestQueue.Complete();
        await _requestQueue.Completion;
        await _sendBackgroundTask;

        if (_signalNoMoreDataToRead != null)
        {
            await _signalNoMoreDataToRead.CancelAsync();
        }
        if (_signalNoMoreDataToWrite != null)
        {
            await _signalNoMoreDataToWrite.CancelAsync();
        }

        await _processResponseBackgroundTask;
        await _receiveBackgroundTask;

        _socket.Shutdown(SocketShutdown.Both);
        _socket.Dispose();
        _socket = null;
    }

    private class SerializationContext(ArrayPool<byte> arrayPool, int bufferSize) : ISerializationContext
    {
        public required SerializationConfig Config { get; init; }
        
        public MemoryStream CreateBuffer()
        {
            var buffer = arrayPool.Rent(bufferSize);
            return new PooledMemoryStream(arrayPool, buffer);
        }
    }
}