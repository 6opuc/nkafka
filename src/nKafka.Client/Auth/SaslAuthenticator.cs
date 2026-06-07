using System.Security.Cryptography;
using Microsoft.Extensions.Logging;
using nKafka.Client.Auth;
using nKafka.Contracts;
using nKafka.Contracts.MessageDefinitions;

namespace nKafka.Client;

public sealed class SaslAuthenticator : IAuthenticator
{
    private readonly ILogger _logger;
    private readonly ConnectionConfig _config;

    public SaslAuthenticator(ConnectionConfig config, ILoggerFactory loggerFactory)
    {
        _config = config;
        _logger = loggerFactory.CreateLogger<SaslAuthenticator>();
    }

    public async ValueTask AuthenticateAsync(IConnection connection, CancellationToken ct)
    {
        string? mechanism = _config.Sasl?.Mechanism;
        if (string.IsNullOrEmpty(mechanism))
        {
            return;
        }

        _logger.LogInformation("Starting SASL authentication with {Mechanism}.", mechanism);

        // 1. SaslHandshake
        _logger.LogDebug("Sending SaslHandshakeRequest.");
        using var handshakeResponse = await connection.SendAsync<SaslHandshakeResponse>(
            new SaslHandshakeRequest { Mechanism = mechanism, FixedVersion = 1 }, ct);

        if (handshakeResponse.Message.ErrorCode != 0)
        {
            throw new InvalidOperationException(
                $"SASL handshake failed with error code {handshakeResponse.Message.ErrorCode}");
        }

        var supportedMechanisms = handshakeResponse.Message.Mechanisms;
        if (supportedMechanisms == null ||
            !supportedMechanisms.Any(m => m == mechanism))
        {
            string supported = supportedMechanisms != null
                ? string.Join(", ", supportedMechanisms)
                : "none";
            throw new InvalidOperationException(
                $"Server does not support {mechanism}. Supported: {supported}");
        }

        // 2. SASL Authenticate (SCRAM-SHA exchange)
        HashAlgorithmName hashAlgorithm;
        if (mechanism == "SCRAM-SHA-512")
            hashAlgorithm = HashAlgorithmName.SHA512;
        else if (mechanism == "SCRAM-SHA-256")
            hashAlgorithm = HashAlgorithmName.SHA256;
        else
            throw new NotSupportedException($"SASL mechanism {mechanism} is not supported");

        string? username = _config.Sasl!.Username;
        string? password = _config.Sasl!.Password;
        if (string.IsNullOrEmpty(username) || string.IsNullOrEmpty(password))
        {
            throw new InvalidOperationException("SASL credentials (username, password) are required when SaslMechanism is configured.");
        }

        var scramClient = new ScramClient(username, password, hashAlgorithm);

        // Round 1: Client-First -> Server-First
        byte[] clientFirstBytes = scramClient.GetClientFirstMessage();
        _logger.LogDebug("Sending SASL authenticate (client-first).");
        using var serverFirstResponse = await connection.SendAsync<SaslAuthenticateResponse>(
            new SaslAuthenticateRequest { AuthBytes = clientFirstBytes }, ct);

        if (serverFirstResponse.Message.ErrorCode != 0)
        {
            string errorMsg = serverFirstResponse.Message.ErrorMessage ?? $"Error code {serverFirstResponse.Message.ErrorCode}";
            throw new InvalidOperationException($"SASL authentication failed at server-first: {errorMsg}");
        }

        ReadOnlySpan<byte> serverFirstSpan = serverFirstResponse.Message.AuthBytes is { } first ? first.Span : ReadOnlySpan<byte>.Empty;

        // Round 2: Client-Final -> Server-Final
        byte[] clientFinalBytes = scramClient.GetClientFinalMessage(serverFirstSpan);
        _logger.LogDebug("Sending SASL authenticate (client-final).");
        using var serverFinalResponse = await connection.SendAsync<SaslAuthenticateResponse>(
            new SaslAuthenticateRequest { AuthBytes = clientFinalBytes }, ct);

        if (serverFinalResponse.Message.ErrorCode != 0)
        {
            string errorMsg = serverFinalResponse.Message.ErrorMessage ?? $"Error code {serverFinalResponse.Message.ErrorCode}";
            throw new InvalidOperationException($"SASL authentication failed: {errorMsg}");
        }

        ReadOnlySpan<byte> serverFinalSpan = serverFinalResponse.Message.AuthBytes is { } final ? final.Span : ReadOnlySpan<byte>.Empty;
        scramClient.VerifyServerFinalMessage(serverFinalSpan);
        _logger.LogInformation("SASL authentication successful.");
    }
}
