using FluentAssertions;

namespace nKafka.Client.IntegrationTests;

public class ConsumerTests
{
    private const string Topic = "test_p12_m1M_s4B";

    private const string BootstrapServers =
        "PLAINTEXT://localhost:9192,PLAINTEXT://localhost:9292,PLAINTEXT://localhost:9392";

    private const long OffsetPastEndOfTopic = 1_000_000;
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// When starting from an offset past the end of the topic,
    /// ConsumeAsync should return gracefully instead of blocking indefinitely.
    /// </summary>
    [Test]
    public async Task ConsumeAsync_WithHighOffset_ShouldNotDeadlock()
    {
        await using var consumer = await CreateConsumerAsync(
            "deadlock-test-client",
            $"deadlock-test-group-{Guid.NewGuid()}",
            $"deadlock-test-instance-{Guid.NewGuid()}");

        var cts = new CancellationTokenSource(TestTimeout);
        var consumeTask = consumer.ConsumeAsync(cts.Token).AsTask();

        var completedTask = await Task.WhenAny(consumeTask,
            Task.Delay(TestTimeout + TimeSpan.FromSeconds(2), CancellationToken.None));

        completedTask.Should().Be(consumeTask,
            "ConsumeAsync should complete within 10s when no messages are available.");

        var result = await consumeTask;
        result.Should().BeNull("no messages exist at the high offset");
    }

    /// <summary>
    /// When starting from an offset past the end of the topic,
    /// ConsumeBatchAsync should return gracefully instead of blocking indefinitely.
    /// </summary>
    [Test]
    public async Task ConsumeBatchAsync_WithHighOffset_ShouldNotDeadlock()
    {
        await using var consumer = await CreateConsumerAsync(
            "deadlock-batch-test-client",
            $"deadlock-batch-test-group-{Guid.NewGuid()}",
            $"deadlock-batch-test-instance-{Guid.NewGuid()}");

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
        string instanceId)
    {
        var config = new ConsumerConfig(
            BootstrapServers,
            Topic,
            clientId,
            consumerGroup,
            instanceId,
            "PLAINTEXT")
        {
            MaxWaitTime = TimeSpan.FromSeconds(1),
        };

        var offsetStorage = new FixedOffsetStorage(OffsetPastEndOfTopic);
        var deserializer = new DummyDeserializer();

        var consumer = new Consumer<byte[]>(config, deserializer, offsetStorage, TestLoggerFactory.Instance);
        await consumer.JoinGroupAsync(CancellationToken.None);
        return consumer;
    }
}