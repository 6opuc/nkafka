using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks.Dataflow;
using Microsoft.Extensions.Logging;
using nKafka.Contracts;
using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageSerializers;

namespace nKafka.Client;

public class Connection : IConnection
{
    private readonly ILogger<Connection> _logger;
    private Socket? _socket;
    private ConnectionConfig? _config;
    
    private SocketWriterStream? _writerStream;
    private CancellationTokenSource? _signalNoMoreDataToWrite;
    private CancellationTokenSource? _signalNoMoreDataToRead;

    private readonly Pipe _pipe = new Pipe(new PipeOptions(
        useSynchronizationContext: false));

    private Task _receiveBackgroundTask = default!;
    private Task _processResponseBackgroundTask = default!;
    private const int MinimumBufferSize = 512 * 1024; // TODO: config

    private BufferBlock<PendingRequest> _requestQueue = new();
    private Task _sendBackgroundTask = default!;
    private ConcurrentQueue<PendingRequest> _pendingRequests = new();

    public Connection(ILogger<Connection> logger)
    {
        _logger = logger;
    }
    
    public async ValueTask OpenAsync(ConnectionConfig config, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(config);
        _config = config;
        
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
                        
                        var payload = await _pipe.Reader.ReadAsync(payloadSize, cancellationToken);
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
                            using var input = new MemoryStream(payload, 0, payload.Length, false, true);
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
                        var memory = writer.GetMemory(MinimumBufferSize);
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

                    try
                    {
                        _logger.LogDebug("Processing request {@correlationId}", request.RequestClient.CorrelationId);
                        _pendingRequests.Enqueue(request);

                        _logger.LogDebug("Building request {@correlationId}", request.RequestClient.CorrelationId);
#warning buffer pool
                        using var output = new MemoryStream();
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