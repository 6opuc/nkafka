using System.Security.Cryptography;
using System.Text;

namespace nKafka.Client.Auth;

public class ScramClient
{
    private const string ClientKeyConst = "Client Key";
    private const string ServerKeyConst = "Server Key";

    private readonly string _username;
    private readonly string _password;
    private readonly HashAlgorithmName _hashAlgorithm;

    private string? _clientNonce;
    private string? _clientFirstMessageBare;
    private string? _serverFirstMessage;
    private string? _clientFinalMessageWithoutProof;
    private byte[]? _saltedPassword;

    public ScramClient(string username, string password, HashAlgorithmName hashAlgorithm)
    {
        _username = username;
        _password = password;
        _hashAlgorithm = hashAlgorithm;
    }

    public string Mechanism => _hashAlgorithm switch
    {
        { Name: "SHA512" } => "SCRAM-SHA-512",
        { Name: "SHA256" } => "SCRAM-SHA-256",
        _ => throw new NotSupportedException($"Hash algorithm {_hashAlgorithm.Name} is not supported for SCRAM"),
    };

    private static int HashSize(HashAlgorithmName algorithm) => algorithm switch
    {
        { Name: "SHA512" } => 64,
        { Name: "SHA256" } => 32,
        _ => throw new NotSupportedException(),
    };

    public byte[] GetClientFirstMessage()
    {
        _clientNonce = GenerateNonce();
        _clientFirstMessageBare = $"n={_username},r={_clientNonce}";
        var gs2Header = "n,,";
        return Encoding.ASCII.GetBytes(gs2Header + _clientFirstMessageBare);
    }

    public byte[] GetClientFinalMessage(byte[] serverFirstMessageBytes)
    {
        _serverFirstMessage = Encoding.ASCII.GetString(serverFirstMessageBytes);
        ParseServerFirstMessage(_serverFirstMessage, out var saltBase64, out var iterations, out var serverNonce);

        var salt = Convert.FromBase64String(saltBase64);
        var hashSize = HashSize(_hashAlgorithm);

        _saltedPassword = PBKDF2(_password, salt, iterations, hashSize);

        var clientKey = HMAC(_saltedPassword, ClientKeyConst);
        var storedKey = Hash(clientKey);
        var serverKey = HMAC(_saltedPassword, ServerKeyConst);

        _clientFinalMessageWithoutProof = $"c=biws,r={serverNonce}";
        var authMessage = $"{_clientFirstMessageBare},{_serverFirstMessage},{_clientFinalMessageWithoutProof}";

        var clientSignature = HMAC(storedKey, authMessage);
        var clientProof = XOR(clientKey, clientSignature);

        var clientFinalMessage = $"{_clientFinalMessageWithoutProof},p={Convert.ToBase64String(clientProof)}";
        return Encoding.ASCII.GetBytes(clientFinalMessage);
    }

    public void VerifyServerFinalMessage(byte[] serverFinalMessageBytes)
    {
        var serverFinalMessage = Encoding.ASCII.GetString(serverFinalMessageBytes);
        if (serverFinalMessage.StartsWith("e="))
        {
            var error = serverFinalMessage[2..];
            throw new InvalidOperationException($"SCRAM authentication failed: {error}");
        }

        if (!serverFinalMessage.StartsWith("v="))
        {
            throw new InvalidOperationException(
                $"Invalid SCRAM server-final message: {serverFinalMessage}");
        }

        var expectedSignature = Convert.FromBase64String(serverFinalMessage[2..]);

        var authMessage = $"{_clientFirstMessageBare},{_serverFirstMessage},{_clientFinalMessageWithoutProof}";
        var serverKey = HMAC(_saltedPassword!, ServerKeyConst);
        var serverSignature = HMAC(serverKey, authMessage);

        if (!ConstantTimeEquals(expectedSignature, serverSignature))
        {
            throw new InvalidOperationException("SCRAM server signature verification failed.");
        }
    }

    private static void ParseServerFirstMessage(
        string message,
        out string salt,
        out int iterations,
        out string nonce)
    {
        salt = "";
        iterations = 4096;
        nonce = "";

        foreach (var part in message.Split(','))
        {
            if (part.StartsWith("r="))
                nonce = part[2..];
            else if (part.StartsWith("s="))
                salt = part[2..];
            else if (part.StartsWith("i="))
                iterations = int.Parse(part[2..]);
        }

        if (string.IsNullOrEmpty(salt))
            throw new InvalidOperationException("SCRAM server-first message missing salt");
        if (string.IsNullOrEmpty(nonce))
            throw new InvalidOperationException("SCRAM server-first message missing nonce");
    }

    private static string GenerateNonce()
    {
        var bytes = RandomNumberGenerator.GetBytes(24);
        return Convert.ToBase64String(bytes)
            .TrimEnd('=')
            .Replace('+', 'A')
            .Replace('/', 'B');
    }

    private static byte[] PBKDF2(string password, byte[] salt, int iterations, int outputLength)
    {
        return Rfc2898DeriveBytes.Pbkdf2(
            Encoding.UTF8.GetBytes(password),
            salt,
            iterations,
            HashAlgorithmName.SHA512,
            outputLength);
    }

    private static byte[] HMAC(byte[] key, string data)
    {
        return HMACSHA512.HashData(key, Encoding.ASCII.GetBytes(data));
    }

    private static byte[] Hash(byte[] data)
    {
        return SHA512.HashData(data);
    }

    private static byte[] XOR(byte[] a, byte[] b)
    {
        var result = new byte[a.Length];
        for (var i = 0; i < a.Length; i++)
            result[i] = (byte)(a[i] ^ b[i]);
        return result;
    }

    private static bool ConstantTimeEquals(byte[] a, byte[] b)
    {
        return CryptographicOperations.FixedTimeEquals(a, b);
    }
}
