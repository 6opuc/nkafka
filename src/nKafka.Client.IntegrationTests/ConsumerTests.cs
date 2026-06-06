using FluentAssertions;
using nKafka.Client;
using System.Collections;

namespace nKafka.Client.IntegrationTests;

public class ConsumerTests
{
    private const string SaslBootstrapHost = "localhost";
    private const int SaslBootstrapPort = 9192;
    private const string SaslMechanism = "SCRAM-SHA-512";
    private const string SaslUsername = "admin";
    private const string SaslPassword = "admin-secret";

    private const string PlainTextTopic = "test_p12_m1M_s4B";
    private const string SaslTopic = "test_p12_m40K_s10KB";

    private const long OffsetPastEndOfTopic = 1_000_000;
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(10);

    private static readonly string SslCaCertPath = Path.Combine(
        TestContext.CurrentContext.TestDirectory,
        "../../../../../infra/secrets/ca-cert.pem");

    [SetUp]
    public void SetUp()
    {
        if (!File.Exists(SslCaCertPath))
        {
            Assert.Ignore($"SASL/SSL infrastructure not available: CA cert not found at '{SslCaCertPath}'. Run 'infra/gen-certs.sh' and 'infra/init-cluster.sh' first.");
        }
    }

    public static IEnumerable Protocols
    {
        get { yield return "PLAINTEXT"; yield return "SASL_SSL"; }
    }

    public static IEnumerable ConsumerBootstrapServers
    {
        get
        {
            yield return new object[] { "PLAINTEXT", "PLAINTEXT://localhost:9193,PLAINTEXT://localhost:9293,PLAINTEXT://localhost:9393" };
            yield return new object[] { "SASL_SSL", $"SASL_SSL://{SaslBootstrapHost}:{SaslBootstrapPort}" };
        }
    }

    [Test]
    [TestCaseSource(nameof(ConsumerBootstrapServers))]
    public async Task ConsumeAsync_WithHighOffset_ShouldNotDeadlock(string protocol, string servers)
    {
        await using var consumer = await CreateConsumerAsync(
            "deadlock-test-client",
            $"deadlock-test-group-{Guid.NewGuid()}",
            $"deadlock-test-instance-{Guid.NewGuid()}",
            servers, PlainTextTopic, protocol);

        var cts = new CancellationTokenSource(TestTimeout);
        var consumeTask = consumer.ConsumeAsync(cts.Token).AsTask();

        var completedTask = await Task.WhenAny(consumeTask,
            Task.Delay(TestTimeout + TimeSpan.FromSeconds(2), CancellationToken.None));

        completedTask.Should().Be(consumeTask,
            "ConsumeAsync should complete within 10s when no messages are available.");

        var result = await consumeTask;
        result.Should().BeNull("no messages exist at the high offset");
    }

    [Test]
    [TestCaseSource(nameof(ConsumerBootstrapServers))]
    public async Task ConsumeBatchAsync_WithHighOffset_ShouldNotDeadlock(string protocol, string servers)
    {
        await using var consumer = await CreateConsumerAsync(
            "deadlock-batch-test-client",
            $"deadlock-batch-test-group-{Guid.NewGuid()}",
            $"deadlock-batch-test-instance-{Guid.NewGuid()}",
            servers, PlainTextTopic, protocol);

        var cts = new CancellationTokenSource(TestTimeout);
        var consumeTask = consumer.ConsumeBatchAsync(cts.Token).AsTask();

        var completedTask = await Task.WhenAny(consumeTask,
            Task.Delay(TestTimeout + TimeSpan.FromSeconds(2), CancellationToken.None));

        completedTask.Should().Be(consumeTask,
            "ConsumeBatchAsync should complete within 10s when no messages are available.");

        var results = await consumeTask;
        results.Should().BeEmpty("no messages exist at the high offset");
    }

    private static async Task<Consumer<byte[]>> CreateConsumerAsync(string clientId, string consumerGroup,
        string instanceId, string servers, string topic, string protocol)
    {
        var config = new ConsumerConfig(
            servers,
            topic,
            clientId,
            consumerGroup,
            instanceId,
            protocol,
            MaxWaitTime: TimeSpan.FromSeconds(1),
            Ssl: protocol == "SASL_SSL" ? new SslConfig(SaslMechanism, SaslUsername, SaslPassword, SslCaCertPath) : null);

        var offsetStorage = new FixedOffsetStorage(OffsetPastEndOfTopic);
        var deserializer = new DummyDeserializer();

        var consumer = new Consumer<byte[]>(config, deserializer, offsetStorage, TestLoggerFactory.Instance);
        await consumer.JoinGroupAsync(CancellationToken.None);
        return consumer;
    }

    [Test]
    public async Task ConsumeGroup_SaslSsl_ShouldConsumeMessagesWithFetchSessions()
    {
        var config = new ConsumerConfig(
            $"SASL_SSL://{SaslBootstrapHost}:{SaslBootstrapPort}",
            SaslTopic,
            $"sasl-fetch-session-test-{Guid.NewGuid()}",
            $"sasl-fetch-session-group-{Guid.NewGuid()}",
            $"sasl-fetch-session-instance-{Guid.NewGuid()}",
            "SASL_SSL",
            MaxWaitTime: TimeSpan.FromSeconds(2),
            Ssl: new SslConfig(SaslMechanism, SaslUsername, SaslPassword, SslCaCertPath));

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
            $"SASL_SSL://{SaslBootstrapHost}:{SaslBootstrapPort}",
            SaslTopic,
            $"sasl-stats-test-{Guid.NewGuid()}",
            $"sasl-stats-group-{Guid.NewGuid()}",
            $"sasl-stats-instance-{Guid.NewGuid()}",
            "SASL_SSL",
            MaxWaitTime: TimeSpan.FromSeconds(2),
            Ssl: new SslConfig(SaslMechanism, SaslUsername, SaslPassword, SslCaCertPath));

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
