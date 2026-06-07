using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;

namespace nKafka.Client;

public sealed class NetworkStreamProvider : IStreamProvider
{
    private readonly ILogger _logger;
    private readonly ConnectionConfig _config;
    private Socket? _socket;
    private SslStream? _sslStream;
    private Stream? _readStream;
    private Stream? _writeStream;

    public NetworkStreamProvider(ConnectionConfig config, ILoggerFactory loggerFactory)
    {
        _config = config;
        _logger = loggerFactory.CreateLogger<NetworkStreamProvider>();
    }

    public Stream ReadStream => _readStream!;
    public Stream WriteStream => _writeStream!;

    public async ValueTask OpenAsync(CancellationToken ct)
    {
        if (_socket != null)
        {
            throw new InvalidOperationException("Stream provider is already open.");
        }

        _logger.LogInformation("Opening socket connection.");

        var ip = await Dns.GetHostAddressesAsync(_config.Host, ct);
        if (ip.Length == 0)
        {
            throw new InvalidOperationException("Unable to resolve host.");
        }

        var endpoint = new IPEndPoint(ip.First(), _config.Port);
        _socket = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        _socket.ReceiveBufferSize = _config.ResponseBufferSize;
        _socket.SendBufferSize = _config.RequestBufferSize;
        _socket.NoDelay = true;
        await _socket.ConnectAsync(endpoint, ct);

        string protocol = _config.Protocol;
        if (protocol != "SASL_SSL" && protocol != "SSL")
        {
            _readStream = new NetworkStream(_socket!, false);
            _writeStream = _readStream;
            return;
        }

        _logger.LogInformation("Starting TLS handshake.");

        X509Certificate2? caCert = null;
        if (_config.Tls?.CaCertPath is string certPath)
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

        await _sslStream.AuthenticateAsClientAsync(sslOptions, ct);

        _logger.LogInformation("TLS handshake complete. Protocol={Protocol}, Cipher={Cipher}",
            _sslStream.SslProtocol, _sslStream.NegotiatedCipherSuite);

        _readStream = _sslStream;
        _writeStream = _sslStream;
    }

    public async ValueTask DisposeAsync()
    {
        if (_socket == null)
        {
            return;
        }

        _logger.LogInformation("Closing socket connection.");

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
}
