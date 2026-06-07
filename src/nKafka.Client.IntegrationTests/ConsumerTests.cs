using FluentAssertions;
using nKafka.Client;
using System.Collections;

namespace nKafka.Client.IntegrationTests;

public class ConsumerTests
{
    private const long OffsetPastEndOfTopic = 1_000_000;
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(10);

    [SetUp]
    public void SetUp()
    {
        TestHelpers.ValidateSslInfrastructure();
    }

    public static IEnumerable Protocols
    {
        get { yield return "PLAINTEXT"; yield return "SASL_SSL"; }
    }

    [Test]
    [TestCaseSource(nameof(Protocols))]
    public async Task ConsumeAsync_WithHighOffset_ShouldNotDeadlock(string protocol)
    {
        await using var consumer = await CreateConsumerAsync(
            "deadlock-test-client",
            $"deadlock-test-group-{Guid.NewGuid()}",
            $"deadlock-test-instance-{Guid.NewGuid()}",
            protocol);

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
    [TestCaseSource(nameof(Protocols))]
    public async Task ConsumeBatchAsync_WithHighOffset_ShouldNotDeadlock(string protocol)
    {
        await using var consumer = await CreateConsumerAsync(
            "deadlock-batch-test-client",
            $"deadlock-batch-test-group-{Guid.NewGuid()}",
            $"deadlock-batch-test-instance-{Guid.NewGuid()}",
            protocol);

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
        string instanceId, string protocol)
    {
        var config = TestHelpers.CreateConsumerConfig(
            clientId,
            consumerGroup,
            instanceId,
            protocol);

        var offsetStorage = new FixedOffsetStorage(OffsetPastEndOfTopic);
        var deserializer = new DummyDeserializer();

        var consumer = new Consumer<byte[]>(config, deserializer, offsetStorage, TestLoggerFactory.Instance);
        await consumer.JoinGroupAsync(CancellationToken.None);
        return consumer;
    }

    [Test]
    [TestCaseSource(nameof(Protocols))]
    public async Task ConsumeAsync_WithMessages_ShouldConsumeWithFetchSessions(string protocol)
    {
        var config = TestHelpers.CreateConsumerConfig(
            $"fetch-session-test-{Guid.NewGuid()}",
            $"fetch-session-group-{Guid.NewGuid()}",
            $"fetch-session-instance-{Guid.NewGuid()}",
            protocol,
            maxWaitTime: TimeSpan.FromSeconds(2),
            checkCrcs: true);

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

        consumed.Should().BeGreaterThan(0, $"Should consume messages from {protocol} topic");
        stopwatch.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(10), "Should complete within reasonable time with fetch sessions");
    }

    [Test]
    [TestCaseSource(nameof(Protocols))]
    public async Task ConsumeBatchAsync_WithMessages_ShouldHaveFetchStats(string protocol)
    {
        var config = TestHelpers.CreateConsumerConfig(
            $"stats-test-{Guid.NewGuid()}",
            $"stats-group-{Guid.NewGuid()}",
            $"stats-instance-{Guid.NewGuid()}",
            protocol,
            maxWaitTime: TimeSpan.FromSeconds(2),
            checkCrcs: true);

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
