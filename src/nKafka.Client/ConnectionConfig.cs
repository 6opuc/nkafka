using System.Text.RegularExpressions;

namespace nKafka.Client;

public sealed record ConnectionConfig(
    string Protocol,
    string Host,
    int Port,
    string ClientId,
    int ResponseBufferSize = 512 * 1024,
    int RequestBufferSize = 512 * 1024,
    TlsConfig? Tls = null,
    SaslConfig? Sasl = null,
    bool CheckCrcs = false,
    bool RequestApiVersionsOnOpen = true,
    int RequestTimeoutMs = 60_000)
{
    private static readonly Regex _connectionStringRegex = new(
        @"^(?<proto>\S+)\:\/\/(?<host>\S+)\:(?<port>\S+)$", RegexOptions.Compiled);

    public static ConnectionConfig FromConnectionString(
        string connectionString,
        string clientId,
        int responseBufferSize = 512 * 1024,
        int requestBufferSize = 512 * 1024)
    {
        var match = _connectionStringRegex.Match(connectionString);
        if (!match.Success)
        {
            throw new ArgumentException($"Invalid connection string '{connectionString}'");
        }
        if (!int.TryParse(match.Groups["port"].Value, out int port))
        {
            throw new ArgumentException($"Invalid port '{match.Groups["port"].Value}'");
        }
        return new ConnectionConfig(
            match.Groups["proto"].Value,
            match.Groups["host"].Value,
            port,
            clientId,
            responseBufferSize,
            requestBufferSize);
    }
}
