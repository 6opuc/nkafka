using System.Collections;
using System.Threading.Channels;
using FluentAssertions;
using nKafka.Client;

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
        var batchTask = consumer.ConsumeBatchAsync(cts.Token);

        var completedTask = await Task.WhenAny(batchTask.AsTask(),
            Task.Delay(TestTimeout + TimeSpan.FromSeconds(2), CancellationToken.None));

        completedTask.Should().Be(batchTask.AsTask(),
            "ConsumeBatchAsync should complete within 10s when no messages are available.");

        using var results = await batchTask;
        results.Should().BeEmpty("no messages exist at the high offset");
    }

    [Test]
    [TestCaseSource(nameof(Protocols))]
    public async Task ConsumeAsync_WithMessages_ShouldConsumeFromTopic(string protocol)
    {
        var config = TestHelpers.CreateConsumerConfig(
            $"consume-test-{Guid.NewGuid()}",
            $"consume-group-{Guid.NewGuid()}",
            $"consume-instance-{Guid.NewGuid()}",
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
            using var batch = await consumer.ConsumeBatchAsync(cts.Token).ConfigureAwait(false);
            foreach (var record in batch)
            {
                consumed++;
            }

            if (consumed >= 1000)
                break;
        }

        stopwatch.Stop();

        consumed.Should().BeGreaterThan(0, $"Should consume messages from {protocol} topic");
        stopwatch.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(10), "Should complete within reasonable time");
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
            using var batch = await consumer.ConsumeBatchAsync(cts.Token).ConfigureAwait(false);
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
    public async Task ConsumeBatchAsync_WithRebalance_ShouldHandleRebalanceGracefully(string protocol)
    {
        string group = $"rebalance-group-{Guid.NewGuid()}";
        var configA = TestHelpers.CreateConsumerConfig(
            "rebalance-test-client",
            group,
            "rebalance-test-instance",
            protocol,
            maxWaitTime: TimeSpan.FromSeconds(2));

        var configWithHeartbeat = configA with { HeartbeatIntervalMs = 1000 };
        var offsetStorage = new FixedOffsetStorage(0);
        var deserializer = new DummyDeserializer();

        await using var consumerA = new Consumer<byte[]>(configWithHeartbeat, deserializer, offsetStorage, TestLoggerFactory.Instance);
        await consumerA.JoinGroupAsync(CancellationToken.None);

        CancellationTokenSource testCts = new(TimeSpan.FromSeconds(30));
        long consumedByA = 0;

        var consumeTaskA = Task.Run(async () =>
        {
            while (!testCts.Token.IsCancellationRequested)
            {
                try
                {
                    using var batch = await consumerA.ConsumeBatchAsync(testCts.Token).ConfigureAwait(false);
                    foreach (var _ in batch)
                    {
                        Interlocked.Increment(ref consumedByA);
                    }
                }
                catch (ObjectDisposedException)
                {
                    break;
                }
                catch (ChannelClosedException)
                {
                    break;
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        });

        // Start consumer B immediately — Kafka's JoinGroupAsync takes ~1-2s for group
        // coordination, so consumer A will be actively fetching when rebalance triggers.
        // This ensures consumer A doesn't finish before consumer B starts.
        await using var consumerB = new Consumer<byte[]>(
            TestHelpers.CreateConsumerConfig(
                "rebalance-test-client-b",
                group,
                "rebalance-test-instance-b",
                protocol,
                maxWaitTime: TimeSpan.FromSeconds(2)) with
            { HeartbeatIntervalMs = 1000 },
            deserializer,
            offsetStorage,
            TestLoggerFactory.Instance);

        await consumerB.JoinGroupAsync(CancellationToken.None);

        long consumedByB = 0;

        var consumeTaskB = Task.Run(async () =>
        {
            while (!testCts.Token.IsCancellationRequested)
            {
                try
                {
                    using var batch = await consumerB.ConsumeBatchAsync(testCts.Token).ConfigureAwait(false);
                    foreach (var _ in batch)
                    {
                        Interlocked.Increment(ref consumedByB);
                    }
                }
                catch (ObjectDisposedException)
                {
                    break;
                }
                catch (ChannelClosedException)
                {
                    break;
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        });

        // Wait for both consumers to finish consuming
        await Task.Delay(5000);

        testCts.Cancel();
        await Task.WhenAll(consumeTaskA, consumeTaskB);

        long totalConsumed = Interlocked.Read(ref consumedByA) + Interlocked.Read(ref consumedByB);
        totalConsumed.Should().BeGreaterThan(0, "At least one consumer should have received messages");
        Interlocked.Read(ref consumedByB).Should().BeGreaterThan(0, "Consumer B should have received messages");
        Interlocked.Read(ref consumedByA).Should().BeGreaterThan(0, "Consumer A should have consumed before rebalance interrupted it");
    }
}
