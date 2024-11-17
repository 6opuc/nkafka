using System.Text.RegularExpressions;

namespace nKafka.Client;

public class ConnectionConfig
{
    public string Protocol { get; }
    public string Host { get; }
    public int Port { get; }
    public int ResponseBufferSize { get; }
    public int RequestBufferSize { get; }

    public ConnectionConfig(
        string protocol,
        string host,
        int port,
        int responseBufferSize = 512 * 1024,
        int requestBufferSize = 512 * 1024)
    {
        Protocol = protocol;
        Host = host;
        Port = port;
        ResponseBufferSize = responseBufferSize;
        RequestBufferSize = requestBufferSize;
    }

    private static readonly Regex _connectionStringRegex = new (
        @"^(?<proto>\S+)\:\/\/(?<host>\S+)\:(?<port>\S+)$", RegexOptions.Compiled);
    public ConnectionConfig(
        string connectionString,
        int responseBufferSize = 512 * 1024,
        int requestBufferSize = 512 * 1024)
    {
        var match = _connectionStringRegex.Match(connectionString);
        if (!match.Success)
        {
            throw new ArgumentException($"Invalid connection string '{connectionString}'");
        }
        Protocol = match.Groups["proto"].Value;
        Host = match.Groups["host"].Value;
        if (!int.TryParse(match.Groups["port"].Value, out int port))
        {
            throw new ArgumentException($"Invalid port '{match.Groups["port"].Value}'");
        }
        Port = port;
        ResponseBufferSize = responseBufferSize;
        RequestBufferSize = requestBufferSize;
    }
}