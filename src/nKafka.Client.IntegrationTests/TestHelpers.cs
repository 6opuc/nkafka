using nKafka.Contracts.MessageDefinitions.ConsumerProtocolAssignmentNested;
using nKafka.Contracts.MessageDefinitions.FetchRequestNested;
using nKafka.Contracts.MessageDefinitions.JoinGroupRequestNested;
using nKafka.Contracts.MessageDefinitions.LeaveGroupRequestNested;
using nKafka.Contracts.MessageDefinitions.MetadataRequestNested;
using nKafka.Contracts.MessageDefinitions.OffsetFetchRequestNested;
using nKafka.Contracts.MessageDefinitions.SyncGroupRequestNested;

namespace nKafka.Client.IntegrationTests;

public static class TestHelpers
{
    public const string SaslMechanism = "SCRAM-SHA-512";
    public const string SaslUsername = "admin";
    public const string SaslPassword = "admin-secret";
    public const string SaslBootstrapHost = "localhost";
    public const int SaslBootstrapPort = 9192;
    public const string PlainTextTopic = "test_p12_m1M_s4B";
    public const string SaslTopic = "test_p12_m40K_s10KB";

    private static readonly string SslCaCertPath = Path.Combine(
        TestContext.CurrentContext.TestDirectory,
        "../../../../../infra/secrets/ca-cert.pem");

    public static SslConfig? CreateSslConfig(string protocol)
    {
        if (protocol != "SASL_SSL")
            return null;
        return new SslConfig(SaslMechanism, SaslUsername, SaslPassword, SslCaCertPath);
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
        string host,
        int port,
        string clientId,
        bool checkCrcs = false,
        int? responseBufferSize = null,
        int? requestBufferSize = null,
        bool requestApiVersionsOnOpen = false)
    {
        return new ConnectionConfig(
            protocol,
            host,
            port,
            clientId!,
            responseBufferSize ?? 512 * 1024,
            requestBufferSize ?? 512 * 1024,
            CreateSslConfig(protocol),
            checkCrcs || protocol == "SASL_SSL",
            requestApiVersionsOnOpen);
    }
}
