using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using nKafka.Contracts;
using nKafka.Contracts.MessageDefinitions;

namespace nKafka.Client;

public sealed class SaslAuthenticator : IAuthenticator
{
    private const string ClientKeyConst = "Client Key";
    private const string ServerKeyConst = "Server Key";
    private static readonly byte[] ClientKeyBytes = Encoding.ASCII.GetBytes(ClientKeyConst);
    private static readonly byte[] ServerKeyBytes = Encoding.ASCII.GetBytes(ServerKeyConst);

    private readonly ILogger _logger;
    private readonly ConnectionConfig _config;

    private HashAlgorithmName _hashAlgorithmName;

    // SCRAM state
    private string? _clientNonce;
    private byte[]? _clientFirstMessageBare;
    private byte[]? _serverFirstMessage;
    private string? _clientFinalMessageWithoutProof;
    private byte[]? _saltedPassword;

    public SaslAuthenticator(ConnectionConfig config, ILoggerFactory loggerFactory)
    {
        _config = config;
        _logger = loggerFactory.CreateLogger<SaslAuthenticator>();
    }

    public async ValueTask AuthenticateAsync(IConnection connection, CancellationToken ct)
    {
        var mechanism = _config.Sasl?.Mechanism;
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
            var supported = supportedMechanisms != null
                ? string.Join(", ", supportedMechanisms)
                : "none";
            throw new InvalidOperationException(
                $"Server does not support {mechanism}. Supported: {supported}");
        }

        // 2. SASL Authenticate (SCRAM-SHA exchange)
        _hashAlgorithmName = mechanism switch
        {
            "SCRAM-SHA-512" => HashAlgorithmName.SHA512,
            "SCRAM-SHA-256" => HashAlgorithmName.SHA256,
            _ => throw new NotSupportedException($"SASL mechanism {mechanism} is not supported")
        };

        var username = _config.Sasl!.Username;
        var password = _config.Sasl!.Password;
        if (string.IsNullOrEmpty(username) || string.IsNullOrEmpty(password))
        {
            throw new InvalidOperationException("SASL credentials (username, password) are required when SaslMechanism is configured.");
        }

        var passwordBytes = Encoding.UTF8.GetBytes(password);
        var hashSize = _hashAlgorithmName switch
        {
            { Name: "SHA512" } => 64,
            { Name: "SHA256" } => 32,
            _ => throw new NotSupportedException()
        };

        // Round 1: Client-First -> Server-First
        _clientNonce = GenerateNonce();
        var bareLength = 5 + username.Length + _clientNonce!.Length;
        Span<char> bareChars = stackalloc char[bareLength];
        bareChars[0] = 'n';
        bareChars[1] = '=';
        username.AsSpan().CopyTo(bareChars.Slice(2));
        bareChars[2 + username.Length] = ',';
        bareChars[3 + username.Length] = 'r';
        bareChars[4 + username.Length] = '=';
        _clientNonce.AsSpan().CopyTo(bareChars.Slice(5 + username.Length));
        _clientFirstMessageBare = Encoding.ASCII.GetBytes(new string(bareChars));
        var clientFirstBytes = BuildClientFirstMessage();
        _logger.LogDebug("Sending SASL authenticate (client-first).");
        using var serverFirstResponse = await connection.SendAsync<SaslAuthenticateResponse>(
            new SaslAuthenticateRequest { AuthBytes = clientFirstBytes }, ct);

        if (serverFirstResponse.Message.ErrorCode != 0)
        {
            var errorMsg = serverFirstResponse.Message.ErrorMessage ?? $"Error code {serverFirstResponse.Message.ErrorCode}";
            throw new InvalidOperationException($"SASL authentication failed at server-first: {errorMsg}");
        }

        ReadOnlySpan<byte> serverFirstSpan = serverFirstResponse.Message.AuthBytes is { } first ? first.Span : ReadOnlySpan<byte>.Empty;

        // Round 2: Client-Final -> Server-Final
        var clientFinalBytes = BuildClientFinalMessage(serverFirstSpan, passwordBytes, hashSize);
        _logger.LogDebug("Sending SASL authenticate (client-final).");
        using var serverFinalResponse = await connection.SendAsync<SaslAuthenticateResponse>(
            new SaslAuthenticateRequest { AuthBytes = clientFinalBytes }, ct);

        if (serverFinalResponse.Message.ErrorCode != 0)
        {
            var errorMsg = serverFinalResponse.Message.ErrorMessage ?? $"Error code {serverFinalResponse.Message.ErrorCode}";
            throw new InvalidOperationException($"SASL authentication failed: {errorMsg}");
        }

        ReadOnlySpan<byte> serverFinalSpan = serverFinalResponse.Message.AuthBytes is { } final ? final.Span : ReadOnlySpan<byte>.Empty;
        VerifyServerFinalMessage(serverFinalSpan);
        _logger.LogInformation("SASL authentication successful.");
    }

    private byte[] BuildClientFirstMessage()
    {
        var result = new byte[3 + _clientFirstMessageBare!.Length];
        result[0] = (byte)'n';
        result[1] = (byte)',';
        result[2] = (byte)',';
        _clientFirstMessageBare.CopyTo(result.AsSpan(3));
        return result;
    }

    private byte[] BuildClientFinalMessage(ReadOnlySpan<byte> serverFirstMessageSpan, byte[] passwordBytes, int hashSize)
    {
        _serverFirstMessage = serverFirstMessageSpan.ToArray();
        ParseServerFirstMessage(_serverFirstMessage, out var salt, out var iterations, out var serverNonce);

        _saltedPassword = PBKDF2(passwordBytes, salt, iterations, hashSize);

        var clientKey = HMAC(_saltedPassword, ClientKeyBytes);
        var storedKey = _hashAlgorithmName.Name switch
        {
            "SHA256" => SHA256.HashData(clientKey),
            "SHA512" => SHA512.HashData(clientKey),
            _ => throw new NotSupportedException($"Unsupported hash algorithm: {_hashAlgorithmName}")
        };
        var serverKey = HMAC(_saltedPassword, ServerKeyBytes);

        _clientFinalMessageWithoutProof = $"c=biws,r={serverNonce}";

        var authMessageBytes = BuildAuthMessageBytes(_clientFirstMessageBare!, _serverFirstMessage!, _clientFinalMessageWithoutProof);
        var base64Proof = Convert.ToBase64String(XOR(clientKey, HMAC(storedKey, authMessageBytes)));
        var finalByteCount = _clientFinalMessageWithoutProof!.Length + 3 + base64Proof.Length;
        var result = new byte[finalByteCount];
        Encoding.ASCII.GetBytes(_clientFinalMessageWithoutProof, 0, _clientFinalMessageWithoutProof.Length, result, 0);
        result[_clientFinalMessageWithoutProof.Length] = (byte)',';
        result[_clientFinalMessageWithoutProof.Length + 1] = (byte)'p';
        result[_clientFinalMessageWithoutProof.Length + 2] = (byte)'=';
        Encoding.ASCII.GetBytes(base64Proof, 0, base64Proof.Length, result, _clientFinalMessageWithoutProof.Length + 3);
        return result;
    }

    private void VerifyServerFinalMessage(ReadOnlySpan<byte> serverFinalMessageSpan)
    {
        ReadOnlySpan<char> serverFinal = Encoding.ASCII.GetString(serverFinalMessageSpan);
        if (serverFinal.StartsWith("e="))
        {
            throw new InvalidOperationException($"SCRAM authentication failed: {serverFinal.Slice(2)}");
        }

        ReadOnlySpan<char> signatureSpan = serverFinal[2..];
        if (!serverFinal.StartsWith("v="))
        {
            throw new InvalidOperationException(
                $"Invalid SCRAM server-final message: {serverFinal}");
        }

        var expectedSignature = Convert.FromBase64String(signatureSpan.ToString());

        var serverKey = HMAC(_saltedPassword!, ServerKeyBytes);
        var authMessageBytes = BuildAuthMessageBytes(_clientFirstMessageBare!, _serverFirstMessage!, _clientFinalMessageWithoutProof!);
        var serverSignature = HMAC(serverKey, authMessageBytes);

        if (!CryptographicOperations.FixedTimeEquals(expectedSignature, serverSignature))
        {
            throw new InvalidOperationException("SCRAM server signature verification failed.");
        }
    }

    private static void ParseServerFirstMessage(
        ReadOnlySpan<byte> message,
        out byte[] salt,
        out int iterations,
        out string nonce)
    {
        salt = Array.Empty<byte>();
        iterations = 4096;
        nonce = "";

        var start = 0;
        for (var i = 0; i <= message.Length; i++)
        {
            if (i == message.Length || message[i] == (byte)',')
            {
                ReadOnlySpan<byte> part = message.Slice(start, i - start);
                if (part.Length > 2 && part[0] == (byte)'r' && part[1] == (byte)'=')
                {
                    nonce = Encoding.ASCII.GetString(part.Slice(2));
                }
                else if (part.Length > 2 && part[0] == (byte)'s' && part[1] == (byte)'=')
                {
                    salt = Convert.FromBase64String(Encoding.ASCII.GetString(part.Slice(2)));
                }
                else if (part.Length > 2 && part[0] == (byte)'i' && part[1] == (byte)'=')
                {
                    iterations = int.Parse(Encoding.ASCII.GetString(part.Slice(2)));
                }

                start = i + 1;
            }
        }

        if (salt.Length == 0)
        {
            throw new InvalidOperationException("SCRAM server-first message missing salt");
        }

        if (string.IsNullOrEmpty(nonce))
        {
            throw new InvalidOperationException("SCRAM server-first message missing nonce");
        }
    }

    private static string GenerateNonce()
    {
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        Span<byte> bytes = stackalloc byte[30];
        RandomNumberGenerator.Fill(bytes);
        Span<char> nonce = stackalloc char[30];
        for (var i = 0; i < bytes.Length; i++)
        {
            nonce[i] = chars[bytes[i] % chars.Length];
        }
        return new string(nonce);
    }

    private byte[] PBKDF2(byte[] passwordBytes, byte[] salt, int iterations, int outputLength)
    {
        return Rfc2898DeriveBytes.Pbkdf2(
            passwordBytes,
            salt,
            iterations,
            _hashAlgorithmName,
            outputLength);
    }

    private byte[] HMAC(byte[] key, ReadOnlySpan<char> data)
    {
        var buffer = new byte[Encoding.ASCII.GetByteCount(data)];
        Encoding.ASCII.GetBytes(data, buffer);
        return _hashAlgorithmName.Name switch
        {
            "SHA256" => HMACSHA256.HashData(key, buffer),
            "SHA512" => HMACSHA512.HashData(key, buffer),
            _ => throw new NotSupportedException($"Unsupported hash algorithm: {_hashAlgorithmName}")
        };
    }

    private byte[] HMAC(byte[] key, byte[] data)
    {
        return _hashAlgorithmName.Name switch
        {
            "SHA256" => HMACSHA256.HashData(key, data),
            "SHA512" => HMACSHA512.HashData(key, data),
            _ => throw new NotSupportedException($"Unsupported hash algorithm: {_hashAlgorithmName}")
        };
    }

    private static byte[] BuildAuthMessageBytes(ReadOnlySpan<byte> clientFirstMessageBare, ReadOnlySpan<byte> serverFirstMessage, string clientFinalMessageWithoutProof)
    {
        var clientFinalBytes = Encoding.ASCII.GetByteCount(clientFinalMessageWithoutProof);
        var authMessageLength = clientFirstMessageBare.Length + serverFirstMessage.Length + clientFinalBytes + 2;
        var authMessage = new byte[authMessageLength];
        clientFirstMessageBare.CopyTo(authMessage);
        authMessage[clientFirstMessageBare.Length] = (byte)',';
        serverFirstMessage.CopyTo(authMessage.AsSpan(clientFirstMessageBare.Length + 1));
        authMessage[clientFirstMessageBare.Length + 1 + serverFirstMessage.Length] = (byte)',';
        Encoding.ASCII.GetBytes(clientFinalMessageWithoutProof, 0, clientFinalMessageWithoutProof.Length, authMessage, clientFirstMessageBare.Length + 1 + serverFirstMessage.Length + 1);
        return authMessage;
    }

    private static byte[] XOR(byte[] a, byte[] b)
    {
        var result = new byte[a.Length];
        for (var i = 0; i < a.Length; i++)
        {
            result[i] = (byte)(a[i] ^ b[i]);
        }

        return result;
    }
}
