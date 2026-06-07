using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;

namespace nKafka.Client;

public sealed class TlsStreamProvider : IStreamProvider
{
    private readonly ILogger _logger;
    private readonly IStreamProvider _inner;
    private readonly TlsConfig? _tlsConfig;
    private readonly string _host;
    private SslStream? _sslStream;
    private Stream? _readStream;
    private Stream? _writeStream;

    public TlsStreamProvider(
        IStreamProvider inner,
        string host,
        TlsConfig? tlsConfig,
        ILoggerFactory loggerFactory)
    {
        _inner = inner;
        _host = host;
        _tlsConfig = tlsConfig;
        _logger = loggerFactory.CreateLogger<TlsStreamProvider>();
    }

    public Stream ReadStream => _readStream!;
    public Stream WriteStream => _writeStream!;

    public async ValueTask OpenAsync(CancellationToken ct)
    {
        await _inner.OpenAsync(ct);

        _logger.LogInformation("Starting TLS handshake.");

        X509Certificate2? caCert = null;
        if (_tlsConfig?.CaCertPath is string certPath)
        {
            caCert = X509CertificateLoader.LoadCertificateFromFile(certPath);
        }

        _sslStream = new SslStream(
            _inner.ReadStream,
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
            TargetHost = _host,
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
        if (_readStream != null)
        {
            await _readStream.DisposeAsync();
        }

        if (_sslStream != null)
        {
            await _sslStream.DisposeAsync();
        }

        await _inner.DisposeAsync();
    }
}
