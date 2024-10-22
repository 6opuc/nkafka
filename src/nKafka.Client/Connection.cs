using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks.Dataflow;
using Microsoft.Extensions.Logging;
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
                        _logger.LogDebug("Read response ({@payloadSize} bytes).", payloadSize);

                        if (!_pendingRequests.TryDequeue(out var pendingRequest))
                        {
                            _logger.LogError("Received unexpected response: no pending requests.");
                            continue;
                        }
                        using var input = new MemoryStream(payload);

                        var header = ResponseHeaderSerializer.Deserialize(input, pendingRequest.Request.HeaderVersion);
                        _logger.LogDebug("Received response for request {@correlationId}.", header.CorrelationId);

                        if (header.CorrelationId == null)
                        {
                            _logger.LogError("Received response with empty correlation id.");
                            continue;
                        }
                        
                        if (header.CorrelationId != pendingRequest.Request.CorrelationId)
                        {
                            _logger.LogError(
                                "Received response with incorrect correlation id. Expected {@expectedCorrelationId}, but got {@actualCorrelationId}.",
                                pendingRequest.Request.CorrelationId,
                                header.CorrelationId);
                            continue;
                        }

                        var response = pendingRequest.Request.DeserializeResponse(input);
                        #warning check if not in the end of input stream and throw?

                        pendingRequest.Response.SetResult(response);
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
                // TODO: cancellation
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
                    _logger.LogDebug("Processing request {@correlationId}", request.Request.CorrelationId);
                    _pendingRequests.Enqueue(request);
                    
                    _logger.LogDebug("Building request {@correlationId}", request.Request.CorrelationId);
                    #warning buffer pool
                    using var output = new MemoryStream();
                    request.Request.SerializeRequest(output);
                    
                    _logger.LogDebug("Sending request {@correlationId}", request.Request.CorrelationId);
                    await _writerStream.WriteAsync(output.GetBuffer(), 0, (int)output.Position);
                    _logger.LogDebug("Sent request {@correlationId}", request.Request.CorrelationId);
                }
            });
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