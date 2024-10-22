using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;

namespace nKafka.Client;

public class Connection : IConnection
{
    private readonly ILogger<Connection> _logger;
    private Socket? _socket;
    private ConnectionConfig? _config;

    public Connection(ILogger<Connection> logger)
    {
        _logger = logger;
    }
    
    public async ValueTask OpenAsync(ConnectionConfig config, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(config);
        _config = config;

        await OpenSocketAsync(cancellationToken);
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

    public async ValueTask DisposeAsync()
    {
        if (_socket == null)
        {
            return;
        }
        

        _socket.Shutdown(SocketShutdown.Both);
        _socket.Dispose();
        _socket = null;
    }
}