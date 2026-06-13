using System.Collections;
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

        consumed.Should().BeGreaterThanOrEqualTo(500, "Should have consumed at least 500 messages");
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
    public async Task ConsumeBatchAsync_WithRebalance_ShouldHandleGracefully(string protocol)
    {
        if (protocol == "SASL_SSL")
            TestHelpers.ValidateSslInfrastructure();

        string groupId = $"rebalance-group-{Guid.NewGuid()}";
        var offsetStorage = new FixedOffsetStorage(0);
        var deserializer = new DummyDeserializer();

        var consumerAConfig = TestHelpers.CreateConsumerConfig(
            "rebalance-test-client-a",
            groupId,
            "rebalance-test-instance-a",
            protocol,
            maxWaitTime: TimeSpan.FromSeconds(2));

        var consumerBConfig = TestHelpers.CreateConsumerConfig(
            "rebalance-test-client-b",
            groupId,
            "rebalance-test-instance-b",
            protocol,
            maxWaitTime: TimeSpan.FromSeconds(2));

        await using var consumerA = new Consumer<byte[]>(consumerAConfig, deserializer, offsetStorage, TestLoggerFactory.Instance);
        await consumerA.JoinGroupAsync(CancellationToken.None);

        long consumedByA = 0;
        long consumedByB = 0;
        Exception? exceptionOnA = null;
        Exception? exceptionOnB = null;
        int generationAtRebalance = 0;
        var rebalanceDetected = new TaskCompletionSource<bool>();
        var stopA = new CancellationTokenSource();
        int prevGen = consumerA.GenerationId;

        // Consumer A consumes continuously for test duration, detecting generation change
        var consumeTaskA = Task.Run(async () =>
        {
            while (!stopA.Token.IsCancellationRequested && exceptionOnA == null)
            {
                try
                {
                    using var batch = await consumerA.ConsumeBatchAsync(stopA.Token).ConfigureAwait(false);
                    foreach (var _ in batch)
                    {
                        Interlocked.Increment(ref consumedByA);
                    }

                    int currentGen = consumerA.GenerationId;
                    if (currentGen != prevGen && Interlocked.CompareExchange(ref generationAtRebalance, currentGen, 0) == 0)
                    {
                        rebalanceDetected.SetResult(true);
                    }
                    prevGen = currentGen;
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Interlocked.Exchange(ref exceptionOnA, ex);
                    break;
                }
            }
        });

        // Wait for consumer A to consume at least 1 batch
        while (Interlocked.Read(ref consumedByA) == 0 && exceptionOnA == null)
        {
            await Task.Delay(100);
        }

        // Consumer B joins, triggering rebalance
        await using var consumerB = new Consumer<byte[]>(consumerBConfig, deserializer, offsetStorage, TestLoggerFactory.Instance);
        await consumerB.JoinGroupAsync(CancellationToken.None);

        // Wait for A to detect the generation change (rebalance complete)
        await rebalanceDetected.Task.WaitAsync(TimeSpan.FromSeconds(30));

        // Consumer B consumes 5 batches after rebalance
        var consumeTaskB = Task.Run(async () =>
        {
            for (int i = 0; i < 5 && exceptionOnB == null; i++)
            {
                try
                {
                    using var batch = await consumerB.ConsumeBatchAsync(CancellationToken.None).ConfigureAwait(false);
                    foreach (var _ in batch)
                    {
                        Interlocked.Increment(ref consumedByB);
                    }
                }
                catch (Exception ex)
                {
                    Interlocked.Exchange(ref exceptionOnB, ex);
                    break;
                }
            }
        });

        // Let A and B consume more, then stop
        await Task.WhenAny(consumeTaskB, Task.Delay(TimeSpan.FromSeconds(10)));
        stopA.Cancel();
        await Task.WhenAll(consumeTaskA, consumeTaskB);

        generationAtRebalance.Should().BeGreaterThan(0, "Rebalance should have been detected via generation change");
        exceptionOnA.Should().BeNull("Consumer A should not throw exceptions during rebalance");
        exceptionOnB.Should().BeNull("Consumer B should not throw exceptions during rebalance");
        Interlocked.Read(ref consumedByB).Should().BeGreaterThan(0, "Consumer B should have consumed messages after rebalance");
    }
}
