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

    public Connection(ConnectionConfig config, ILoggerFactory loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(loggerFactory);
        _config = config;
        _logger = loggerFactory.CreateLogger<Connection>();
    }
    
    public async ValueTask OpenAsync(CancellationToken cancellationToken)
    {
        await OpenSocketAsync(cancellationToken);
        
        StartProcessing();
        StartReceiving();
        StartSending();
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
        
        _logger.LogInformation(
            "Opening socket connection to broker at {@host}:{@port}.",
            _config.Host,
            _config.Port);
        
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
                                _logger.LogError("Received unexpected response: no pending requests.");
                                continue;
                            }

                            _logger.LogDebug(
                                "Deserializing response for request {@correlationId}.",
                                pendingRequest.RequestClient.CorrelationId);

                            try
                            {
                                using var input = new MemoryStream(payload, 0, payloadSize, false, true);
                                var response = pendingRequest.RequestClient.DeserializeResponse(input);
                                if (input.Length != input.Position)
                                {
                                    _logger.LogError(
                                        "Received unexpected response length. Expected {@expectedLength}, but got {@actualLength}",
                                        input.Position,
                                        input.Length);
                                }

                                pendingRequest.Response.SetResult(response);
                            }
                            catch (Exception exception)
                            {
                                pendingRequest.Response.SetException(exception);
                            }
                        }
                        finally
                        {
                            #warning consider use of IDisposable in all responses: keep a reference to payload and do not copy byte arrays during deserialization.
                            _arrayPool.Return(payload);
                        }

                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
                _logger.LogDebug("Response processing was stopped");
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

                        _logger.LogDebug("Received {bytesRead} bytes", bytesRead);
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
                        _logger.LogDebug("Processing request {@correlationId}", request.RequestClient.CorrelationId);
                        _pendingRequests.Enqueue(request);

                        _logger.LogDebug("Building request {@correlationId}", request.RequestClient.CorrelationId);
                        using var output = new MemoryStream(payload, 0, payload.Length, true, true);
                        request.RequestClient.SerializeRequest(output);

                        _logger.LogDebug(
                            "Sending request {@correlationId}({@size} bytes)",
                            request.RequestClient.CorrelationId,
                            output.Position);
#warning cancellation, timeouts, other exceptions
                        await _writerStream.WriteAsync(output.GetBuffer(), 0, (int)output.Position);
                        _logger.LogDebug("Sent request {@correlationId}", request.RequestClient.CorrelationId);
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

    public async ValueTask<TResponse> SendAsync<TResponse>(
        RequestClient<TResponse> requestClient,
        CancellationToken cancellationToken)
    {
        var completionPromise = new TaskCompletionSource<object>();
        var pendingRequest = new PendingRequest(
            requestClient,
            completionPromise,
            cancellationToken);
        _logger.LogDebug("Queueing outgoing request {@correlationId}", pendingRequest.RequestClient.CorrelationId);
        await _requestQueue.SendAsync(pendingRequest, cancellationToken);
        _logger.LogDebug("Queued outgoing request {@correlationId}", pendingRequest.RequestClient.CorrelationId);

        var response = await completionPromise.Task;
        return (TResponse)response;
    }

    public async ValueTask DisposeAsync()
    {
        if (_socket == null)
        {
            return;
        }
        
        _logger.LogInformation(
            "Closing socket connection to broker at {@host}:{@port}.",
            _config.Host,
            _config.Port);
        
        _requestQueue.Complete();
        await _requestQueue.Completion;
        await _sendBackgroundTask;

        if (_signalNoMoreDataToWrite != null)
        {
            await _signalNoMoreDataToWrite.CancelAsync();
        }

        if (_signalNoMoreDataToRead != null)
        {
            await _signalNoMoreDataToRead.CancelAsync();
        }

        await _receiveBackgroundTask;
        await _processResponseBackgroundTask;

        _socket.Shutdown(SocketShutdown.Both);
        _socket.Dispose();
        _socket = null;
    }
}