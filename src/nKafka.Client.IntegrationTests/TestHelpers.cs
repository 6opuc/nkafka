using nKafka.Client;

namespace nKafka.Client.IntegrationTests;

public static class TestHelpers
{
    public const string SaslMechanism = "SCRAM-SHA-512";
    public const string SaslUsername = "admin";
    public const string SaslPassword = "admin-secret";
    public const string BootstrapHost = "localhost";
    public const int SaslBootstrapPort = 9192;
    public const int PlainTextBootstrapPort = 9193;
    public const string Topic = "test_p12_m1M_s4B";

    private static readonly string SslCaCertPath = Path.Combine(
        TestContext.CurrentContext.TestDirectory,
        "../../../../../infra/secrets/ca-cert.pem");

    public static TlsConfig? CreateTlsConfig(string protocol)
    {
        if (protocol != "SASL_SSL")
            return null;
        return new TlsConfig(SslCaCertPath);
    }

    public static SaslConfig? CreateSaslConfig()
    {
        return new SaslConfig(SaslMechanism, SaslUsername, SaslPassword);
    }

    public static void ValidateSslInfrastructure()
    {
        if (!File.Exists(SslCaCertPath))
        {
            Assert.Ignore($"SASL/SSL infrastructure not available: CA cert not found at '{SslCaCertPath}'. Run 'infra/gen-certs.sh' and 'infra/init-cluster.sh' first.");
        }
    }

    public static ConnectionConfig CreateConnectionConfig(
        string protocol,
        int? port = null,
        string clientId = "nKafka.Client.IntegrationTests",
        bool checkCrcs = false,
        int? responseBufferSize = null,
        int? requestBufferSize = null,
        bool requestApiVersionsOnOpen = false)
    {
        return new ConnectionConfig(
               protocol,
               BootstrapHost,
               port ?? (protocol == "SASL_SSL" ? SaslBootstrapPort : PlainTextBootstrapPort),
               clientId!,
               responseBufferSize ?? 512 * 1024,
               requestBufferSize ?? 512 * 1024,
               CreateTlsConfig(protocol),
               protocol == "SASL_SSL" ? CreateSaslConfig() : null,
               checkCrcs || protocol == "SASL_SSL",
               requestApiVersionsOnOpen);
    }

    public static ConsumerConfig CreateConsumerConfig(
        string clientId,
        string groupId,
        string instanceId,
        string protocol,
        string topics = Topic,
        TimeSpan? maxWaitTime = null,
        bool checkCrcs = false)
    {
        string servers = protocol == "SASL_SSL"
            ? $"SASL_SSL://{BootstrapHost}:{SaslBootstrapPort}"
            : $"PLAINTEXT://{BootstrapHost}:{PlainTextBootstrapPort}";

        return new ConsumerConfig(
            servers,
            topics,
            clientId,
            groupId,
            instanceId,
            protocol,
            CheckCrcs: checkCrcs || protocol == "SASL_SSL",
            MaxWaitTime: maxWaitTime ?? TimeSpan.FromSeconds(1),
            Tls: CreateTlsConfig(protocol),
            Sasl: protocol == "SASL_SSL" ? CreateSaslConfig() : null);
    }
}
