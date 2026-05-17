namespace nKafka.Client.Benchmarks;

internal static class BenchmarkHelper
{
    internal static readonly string CACertPath = ResolvePath();

    internal static string GetCACertPath() => CACertPath;

    internal static ConnectionConfig CreateConnectionConfig(
        string host, int port, string protocol,
        int responseBufferSize = 512 * 1024,
        int requestBufferSize = 512 * 1024)
    {
        var config = new ConnectionConfig(
            protocol, host, port, "nKafka.Client.Benchmarks",
            responseBufferSize, requestBufferSize)
        {
            RequestApiVersionsOnOpen = false,
        };
        if (protocol == "SASL_SSL")
        {
            config.SslCaCertPath = GetCACertPath();
            config.SaslMechanism = "SCRAM-SHA-512";
            config.SaslUsername = "admin";
            config.SaslPassword = "admin-secret";
            config.CheckCrcs = false;
        }
        return config;
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
