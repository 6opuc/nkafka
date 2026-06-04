using FluentAssertions;
using nKafka.Client;

namespace nKafka.Client.IntegrationTests;

public class SaslConsumerTests
{
    private const string BootstrapHost = "localhost";
    private const int BootstrapPort = 9192;
    private const string SaslMechanism = "SCRAM-SHA-512";
    private const string SaslUsername = "admin";
    private const string SaslPassword = "admin-secret";
    private const string Topic = "test_p12_m40K_s10KB";
    private static readonly string SslCaCertPath = Path.GetFullPath(
        Path.Combine(
            TestContext.CurrentContext.TestDirectory,
            "../../../../../infra/secrets/ca-cert.pem"));

    [SetUp]
    public void SetUp()
    {
        if (!File.Exists(SslCaCertPath))
        {
            Assert.Ignore($"SASL/SSL infrastructure not available: CA cert not found at '{SslCaCertPath}'. Run 'infra/gen-certs.sh' and 'infra/init-cluster.sh' first.");
        }
    }

    [Test]
    public async Task ConsumeGroup_SaslSsl_ShouldConsumeMessagesWithFetchSessions()
    {
        var config = new ConsumerConfig(
            $"SASL_SSL://{BootstrapHost}:{BootstrapPort}",
            Topic,
            $"sasl-fetch-session-test-{Guid.NewGuid()}",
            $"sasl-fetch-session-group-{Guid.NewGuid()}",
            $"sasl-fetch-session-instance-{Guid.NewGuid()}",
            "SASL_SSL")
        {
            MaxWaitTime = TimeSpan.FromSeconds(2),
            SslCaCertPath = SslCaCertPath,
            SaslMechanism = SaslMechanism,
            SaslUsername = SaslUsername,
            SaslPassword = SaslPassword,
        };

        var offsetStorage = new FixedOffsetStorage(0);
        var deserializer = new DummyDeserializer();

        await using var consumer = new Consumer<byte[]>(
            config, deserializer, offsetStorage, TestLoggerFactory.Instance);

        await consumer.JoinGroupAsync(CancellationToken.None);

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        int consumed = 0;

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        while (!cts.Token.IsCancellationRequested)
        {
            var batch = await consumer.ConsumeBatchAsync(cts.Token).ConfigureAwait(false);
            foreach (var record in batch)
            {
                consumed++;
            }

            if (consumed >= 1000)
                break;
        }

        stopwatch.Stop();

        consumed.Should().BeGreaterThan(0, "Should consume messages from SASL_SSL topic");
        stopwatch.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(10), "Should complete within reasonable time with fetch sessions");
    }

    [Test]
    public async Task ConsumeGroup_SaslSsl_ShouldHaveFetchStats()
    {
        var config = new ConsumerConfig(
            $"SASL_SSL://{BootstrapHost}:{BootstrapPort}",
            Topic,
            $"sasl-stats-test-{Guid.NewGuid()}",
            $"sasl-stats-group-{Guid.NewGuid()}",
            $"sasl-stats-instance-{Guid.NewGuid()}",
            "SASL_SSL")
        {
            MaxWaitTime = TimeSpan.FromSeconds(2),
            SslCaCertPath = SslCaCertPath,
            SaslMechanism = SaslMechanism,
            SaslUsername = SaslUsername,
            SaslPassword = SaslPassword,
        };

        var offsetStorage = new FixedOffsetStorage(0);
        var deserializer = new DummyDeserializer();

        await using var consumer = new Consumer<byte[]>(
            config, deserializer, offsetStorage, TestLoggerFactory.Instance);

        await consumer.JoinGroupAsync(CancellationToken.None);

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        int consumed = 0;

        while (!cts.Token.IsCancellationRequested)
        {
            var batch = await consumer.ConsumeBatchAsync(cts.Token).ConfigureAwait(false);
            foreach (var record in batch)
            {
                consumed++;
            }

            if (consumed >= 500)
                break;
        }

        var stats = consumer.Statistics;
        stats.P50FetchRoundTripMs.Should().BeGreaterThan(0, "Should have fetch RTT stats");
        stats.TotalBytesReceived.Should().BeGreaterThan(0, "Should have received bytes");
        stats.TotalMessagesConsumed.Should().BeGreaterThan(0, "Should have consumed messages");
    }
}
