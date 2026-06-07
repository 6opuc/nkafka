using System.Security.Cryptography;
using System.Text;

namespace nKafka.Client.Auth;

public class ScramClient
{
    private const string ClientKeyConst = "Client Key";
    private const string ServerKeyConst = "Server Key";
    private static readonly byte[] ClientKeyBytes = Encoding.ASCII.GetBytes(ClientKeyConst);
    private static readonly byte[] ServerKeyBytes = Encoding.ASCII.GetBytes(ServerKeyConst);

    private readonly string _username;
    private readonly byte[] _passwordBytes;
    private readonly HashAlgorithmName _hashAlgorithm;

    private string? _clientNonce;
    private string? _clientFirstMessageBare;
    private string? _serverFirstMessage;
    private string? _clientFinalMessageWithoutProof;
    private byte[]? _saltedPassword;

    public ScramClient(string username, string password, HashAlgorithmName hashAlgorithm)
    {
        _username = username;
        _passwordBytes = Encoding.UTF8.GetBytes(password);
        _hashAlgorithm = hashAlgorithm;
    }

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
        byte[] result = new byte[3 + _clientFirstMessageBare.Length];
        result[0] = (byte)'n';
        result[1] = (byte)',';
        result[2] = (byte)',';
        Encoding.ASCII.GetBytes(_clientFirstMessageBare, 0, _clientFirstMessageBare.Length, result, 3);
        return result;
    }

    public byte[] GetClientFinalMessage(ReadOnlySpan<byte> serverFirstMessageSpan)
    {
        _serverFirstMessage = Encoding.ASCII.GetString(serverFirstMessageSpan);
        ParseServerFirstMessage(_serverFirstMessage.AsSpan(), out string? saltBase64, out int iterations, out string? serverNonce);

        byte[] salt = Convert.FromBase64String(saltBase64);
        int hashSize = HashSize(_hashAlgorithm);

        _saltedPassword = PBKDF2(_passwordBytes, salt, iterations, hashSize);

        byte[] clientKey = HMAC(_saltedPassword, ClientKeyBytes);
        byte[] storedKey = Hash(clientKey);
        byte[] serverKey = HMAC(_saltedPassword, ServerKeyBytes);

        _clientFinalMessageWithoutProof = $"c=biws,r={serverNonce}";

        string base64Proof = Convert.ToBase64String(XOR(clientKey, HMAC(storedKey, $"{_clientFirstMessageBare},{_serverFirstMessage},{_clientFinalMessageWithoutProof}")));
        int finalByteCount = _clientFinalMessageWithoutProof.Length + 3 + base64Proof.Length;
        byte[] result = new byte[finalByteCount];
        Encoding.ASCII.GetBytes(_clientFinalMessageWithoutProof, 0, _clientFinalMessageWithoutProof.Length, result, 0);
        result[_clientFinalMessageWithoutProof.Length] = (byte)',';
        result[_clientFinalMessageWithoutProof.Length + 1] = (byte)'p';
        result[_clientFinalMessageWithoutProof.Length + 2] = (byte)'=';
        Encoding.ASCII.GetBytes(base64Proof, 0, base64Proof.Length, result, _clientFinalMessageWithoutProof.Length + 3);
        return result;
    }

    public void VerifyServerFinalMessage(ReadOnlySpan<byte> serverFinalMessageSpan)
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
        byte[] serverSignature = HMAC(serverKey, $"{_clientFirstMessageBare},{_serverFirstMessage},{_clientFinalMessageWithoutProof}");

        if (!ConstantTimeEquals(expectedSignature, serverSignature))
        {
            throw new InvalidOperationException("SCRAM server signature verification failed.");
        }
    }

    private static void ParseServerFirstMessage(
        ReadOnlySpan<char> message,
        out string salt,
        out int iterations,
        out string nonce)
    {
        salt = "";
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
                    salt = part.Slice(2).ToString();
                else if (part.Length > 2 && part[0] == 'i' && part[1] == '=')
                    iterations = int.Parse(part.Slice(2));

                start = i + 1;
            }
        }

        if (string.IsNullOrEmpty(salt))
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

    private static byte[] HMAC(byte[] key, string data)
    {
        return HMACSHA512.HashData(key, Encoding.ASCII.GetBytes(data));
    }

    private static byte[] HMAC(byte[] key, byte[] data)
    {
        return HMACSHA512.HashData(key, data);
    }

    private static byte[] Hash(byte[] data)
    {
        return SHA512.HashData(data);
    }

    private static byte[] XOR(byte[] a, byte[] b)
    {
        byte[] result = new byte[a.Length];
        for (int i = 0; i < a.Length; i++)
            result[i] = (byte)(a[i] ^ b[i]);
        return result;
    }

    private static bool ConstantTimeEquals(byte[] a, byte[] b)
    {
        return CryptographicOperations.FixedTimeEquals(a, b);
    }
}
