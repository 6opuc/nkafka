using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;

namespace nKafka.Client;

public sealed class NetworkStreamProvider : IStreamProvider
{
    private readonly ILogger _logger;
    private readonly string _host;
    private readonly int _port;
    private readonly int _responseBufferSize;
    private readonly int _requestBufferSize;
    private Socket? _socket;
    private Stream? _networkStream;

    public NetworkStreamProvider(
        string host,
        int port,
        int responseBufferSize,
        int requestBufferSize,
        ILoggerFactory loggerFactory)
    {
        _host = host;
        _port = port;
        _responseBufferSize = responseBufferSize;
        _requestBufferSize = requestBufferSize;
        _logger = loggerFactory.CreateLogger<NetworkStreamProvider>();
    }

    public Stream ReadStream => _networkStream!;
    public Stream WriteStream => _networkStream!;

    public async ValueTask OpenAsync(CancellationToken ct)
    {
        if (_socket != null)
        {
            throw new InvalidOperationException("Stream provider is already open.");
        }

        _logger.LogInformation("Opening socket connection to {Host}:{Port}.", _host, _port);

        var ip = await Dns.GetHostAddressesAsync(_host, ct);
        if (ip.Length == 0)
        {
            throw new InvalidOperationException("Unable to resolve host.");
        }

        var endpoint = new IPEndPoint(ip.First(), _port);
        _socket = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        _socket.ReceiveBufferSize = _responseBufferSize;
        _socket.SendBufferSize = _requestBufferSize;
        _socket.NoDelay = true;
        await _socket.ConnectAsync(endpoint, ct);

        _networkStream = new NetworkStream(_socket, true);
        _logger.LogInformation("Socket connected.");
    }

    public async ValueTask DisposeAsync()
    {
        if (_socket == null)
        {
            return;
        }

        _logger.LogInformation("Closing socket connection.");

        if (_networkStream != null)
        {
            await _networkStream.DisposeAsync();
        }

        try
        {
            _socket.Shutdown(SocketShutdown.Both);
        }
        catch
        {
            // Socket may already be closed
        }
        _socket.Dispose();
        _socket = null;
    }
}
