using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using nKafka.Contracts;

namespace nKafka.Client.Benchmarks;

internal static class BenchmarkHelper
{
    internal static readonly string CACertPath = ResolvePath();

    internal const int FetchMaxBytes = 100 * 1024 * 1024;
    internal const int PartitionMaxBytes = 1 * 1024 * 1024;
    internal const int ResponseBufferSize = 10 * 512 * 1024;

    internal static Confluent.Kafka.ConsumerConfig ConfigureProtocol(this Confluent.Kafka.ConsumerConfig config, string protocol)
    {
        if (protocol == "SASL_SSL")
        {
            config.SslCaLocation = CACertPath;
            config.SaslMechanism = Confluent.Kafka.SaslMechanism.ScramSha512;
            config.SaslUsername = "admin";
            config.SaslPassword = "admin-secret";
            config.SecurityProtocol = Confluent.Kafka.SecurityProtocol.SaslSsl;
        }
        config.FetchMaxBytes = FetchMaxBytes;
        return config;
    }

    internal static ConnectionConfig ConfigureProtocol(this ConnectionConfig config, string protocol)
    {
        if (protocol == "SASL_SSL")
        {
            return config with { Tls = new TlsConfig(CACertPath), Sasl = new SaslConfig("SCRAM-SHA-512", "admin", "admin-secret"), CheckCrcs = false };
        }
        return config;
    }

    internal static ConsumerConfig ConfigureProtocol(this ConsumerConfig config, string protocol)
    {
        if (protocol == "SASL_SSL")
        {
            return config with { Tls = new TlsConfig(CACertPath), Sasl = new SaslConfig("SCRAM-SHA-512", "admin", "admin-secret") };
        }
        return config;
    }

    internal static async Task<Connection> OpenConnectionAsync(string protocol)
    {
        var config = new ConnectionConfig(
            protocol,
            "localhost",
            BootstrapPort(protocol),
            "nKafka.Client.Benchmarks");
        var connection = new Connection(config, NullLoggerFactory.Instance);
        await connection.OpenAsync(CancellationToken.None);
        return connection;
    }

    internal static ConnectionConfig CreateConnectionConfig(
        string host, int port, string protocol,
        int responseBufferSize,
        int requestBufferSize)
    {
        var config = new ConnectionConfig(
            protocol, host, port, "nKafka.Client.Benchmarks",
            responseBufferSize, requestBufferSize);
        return config.ConfigureProtocol(protocol);
    }

    internal static int BootstrapPort(string protocol) =>
        protocol == "PLAINTEXT" ? 9193 : 9192;

    internal static string BootstrapServers(string protocol)
    {
        var port = BootstrapPort(protocol);
        return $"{protocol}://localhost:{port}, {protocol}://localhost:{port + 100}, {protocol}://localhost:{port + 200}";
    }

    private static string ResolvePath()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null && !dir.EnumerateDirectories("infra").Any())
        {
            dir = dir.Parent;
        }
        if (dir == null)
        {
            throw new InvalidOperationException(
                "Could not locate 'infra' directory to resolve CA cert path.");
        }
        return Path.Combine(dir.FullName, "infra", "secrets", "ca-cert.pem");
    }
}
