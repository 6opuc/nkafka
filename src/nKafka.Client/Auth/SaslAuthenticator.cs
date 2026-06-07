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

    // SCRAM state
    private string? _clientNonce;
    private string? _clientFirstMessageBare;
    private string? _serverFirstMessage;
    private string? _clientFinalMessageWithoutProof;
    private byte[]? _saltedPassword;

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

        byte[] passwordBytes = Encoding.UTF8.GetBytes(password);
        int hashSize = hashAlgorithm switch
        {
            { Name: "SHA512" } => 64,
            { Name: "SHA256" } => 32,
            _ => throw new NotSupportedException()
        };

        // Round 1: Client-First -> Server-First
        _clientNonce = GenerateNonce();
        int bareLength = 5 + username.Length + _clientNonce!.Length;
        char[] bareChars = new char[bareLength];
        bareChars[0] = 'n';
        bareChars[1] = '=';
        username.AsSpan().CopyTo(bareChars.AsSpan(2));
        bareChars[2 + username.Length] = ',';
        bareChars[3 + username.Length] = 'r';
        bareChars[4 + username.Length] = '=';
        _clientNonce.AsSpan().CopyTo(bareChars.AsSpan(5 + username.Length));
        _clientFirstMessageBare = new string(bareChars);
        byte[] clientFirstBytes = BuildClientFirstMessage();
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
        byte[] clientFinalBytes = BuildClientFinalMessage(serverFirstSpan, passwordBytes, hashSize);
        _logger.LogDebug("Sending SASL authenticate (client-final).");
        using var serverFinalResponse = await connection.SendAsync<SaslAuthenticateResponse>(
            new SaslAuthenticateRequest { AuthBytes = clientFinalBytes }, ct);

        if (serverFinalResponse.Message.ErrorCode != 0)
        {
            string errorMsg = serverFinalResponse.Message.ErrorMessage ?? $"Error code {serverFinalResponse.Message.ErrorCode}";
            throw new InvalidOperationException($"SASL authentication failed: {errorMsg}");
        }

        ReadOnlySpan<byte> serverFinalSpan = serverFinalResponse.Message.AuthBytes is { } final ? final.Span : ReadOnlySpan<byte>.Empty;
        VerifyServerFinalMessage(serverFinalSpan);
        _logger.LogInformation("SASL authentication successful.");
    }

    private byte[] BuildClientFirstMessage()
    {
        byte[] result = new byte[3 + _clientFirstMessageBare!.Length];
        result[0] = (byte)'n';
        result[1] = (byte)',';
        result[2] = (byte)',';
        Encoding.ASCII.GetBytes(_clientFirstMessageBare, 0, _clientFirstMessageBare.Length, result, 3);
        return result;
    }

    private byte[] BuildClientFinalMessage(ReadOnlySpan<byte> serverFirstMessageSpan, byte[] passwordBytes, int hashSize)
    {
        _serverFirstMessage = Encoding.ASCII.GetString(serverFirstMessageSpan);
        ParseServerFirstMessage(_serverFirstMessage.AsSpan(), out byte[] salt, out int iterations, out string serverNonce);

        _saltedPassword = PBKDF2(passwordBytes, salt, iterations, hashSize);

        byte[] clientKey = HMAC(_saltedPassword, ClientKeyBytes);
        byte[] storedKey = SHA512.HashData(clientKey);
        byte[] serverKey = HMAC(_saltedPassword, ServerKeyBytes);

        _clientFinalMessageWithoutProof = $"c=biws,r={serverNonce}";

        byte[] authMessageBytes = BuildAuthMessageBytes(_clientFirstMessageBare!, _serverFirstMessage, _clientFinalMessageWithoutProof);
        string base64Proof = Convert.ToBase64String(XOR(clientKey, HMAC(storedKey, authMessageBytes)));
        int finalByteCount = _clientFinalMessageWithoutProof!.Length + 3 + base64Proof.Length;
        byte[] result = new byte[finalByteCount];
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

        byte[] expectedSignature = Convert.FromBase64String(signatureSpan.ToString());

        byte[] serverKey = HMAC(_saltedPassword!, ServerKeyBytes);
        byte[] authMessageBytes = BuildAuthMessageBytes(_clientFirstMessageBare!, _serverFirstMessage!, _clientFinalMessageWithoutProof!);
        byte[] serverSignature = HMAC(serverKey, authMessageBytes);

        if (!CryptographicOperations.FixedTimeEquals(expectedSignature, serverSignature))
        {
            throw new InvalidOperationException("SCRAM server signature verification failed.");
        }
    }

    private static void ParseServerFirstMessage(
        ReadOnlySpan<char> message,
        out byte[] salt,
        out int iterations,
        out string nonce)
    {
        salt = Array.Empty<byte>();
        iterations = 4096;
        nonce = "";

        int start = 0;
        for (int i = 0; i <= message.Length; i++)
        {
            if (i == message.Length || message[i] == ',')
            {
                ReadOnlySpan<char> part = message.Slice(start, i - start);
                if (part.Length > 2 && part[0] == 'r' && part[1] == '=')
                    nonce = part.Slice(2).ToString();
                else if (part.Length > 2 && part[0] == 's' && part[1] == '=')
                    salt = Convert.FromBase64String(part.Slice(2).ToString());
                else if (part.Length > 2 && part[0] == 'i' && part[1] == '=')
                    iterations = int.Parse(part.Slice(2));

                start = i + 1;
            }
        }

        if (salt.Length == 0)
            throw new InvalidOperationException("SCRAM server-first message missing salt");
        if (string.IsNullOrEmpty(nonce))
            throw new InvalidOperationException("SCRAM server-first message missing nonce");
    }

    private static string GenerateNonce()
    {
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        Span<byte> bytes = stackalloc byte[30];
        RandomNumberGenerator.Fill(bytes);
        Span<char> nonce = stackalloc char[30];
        for (int i = 0; i < bytes.Length; i++)
        {
            nonce[i] = chars[bytes[i] % chars.Length];
        }
        return nonce.ToString();
    }

    private static byte[] PBKDF2(byte[] passwordBytes, byte[] salt, int iterations, int outputLength)
    {
        return Rfc2898DeriveBytes.Pbkdf2(
            passwordBytes,
            salt,
            iterations,
            HashAlgorithmName.SHA512,
            outputLength);
    }

    private static byte[] HMAC(byte[] key, ReadOnlySpan<char> data)
    {
        byte[] buffer = new byte[Encoding.ASCII.GetByteCount(data)];
        Encoding.ASCII.GetBytes(data, buffer);
        return HMACSHA512.HashData(key, buffer);
    }

    private static byte[] HMAC(byte[] key, byte[] data)
    {
        return HMACSHA512.HashData(key, data);
    }

    private static byte[] BuildAuthMessageBytes(string clientFirstMessageBare, string serverFirstMessage, string clientFinalMessageWithoutProof)
    {
        int clientFirstBytes = Encoding.ASCII.GetByteCount(clientFirstMessageBare);
        int serverFirstBytes = Encoding.ASCII.GetByteCount(serverFirstMessage);
        int clientFinalBytes = Encoding.ASCII.GetByteCount(clientFinalMessageWithoutProof);
        int authMessageLength = clientFirstBytes + serverFirstBytes + clientFinalBytes + 2;
        byte[] authMessage = new byte[authMessageLength];
        Encoding.ASCII.GetBytes(clientFirstMessageBare, 0, clientFirstMessageBare.Length, authMessage, 0);
        authMessage[clientFirstBytes] = (byte)',';
        Encoding.ASCII.GetBytes(serverFirstMessage, 0, serverFirstMessage.Length, authMessage, clientFirstBytes + 1);
        authMessage[clientFirstBytes + 1 + serverFirstBytes] = (byte)',';
        Encoding.ASCII.GetBytes(clientFinalMessageWithoutProof, 0, clientFinalMessageWithoutProof.Length, authMessage, clientFirstBytes + 1 + serverFirstBytes + 1);
        return authMessage;
    }

    private static byte[] XOR(byte[] a, byte[] b)
    {
        byte[] result = new byte[a.Length];
        for (int i = 0; i < a.Length; i++)
            result[i] = (byte)(a[i] ^ b[i]);
        return result;
    }
}
