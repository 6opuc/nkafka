using Microsoft.Extensions.Logging.Abstractions;

namespace nKafka.Client.Benchmarks;

public static class NKafkaBatchConsumeBytesTest
{
    public static async Task Test(FetchScenario scenario)
    {
        var consumerConfig = new ConsumerConfig(
            "PLAINTEXT://kafka-1:9192, PLAINTEXT://kafka-2:9292, PLAINTEXT://kafka-3:9392",
            scenario.TopicName,
            "test-consumer-group",
            $"testapp-{DateTime.UtcNow.Date:yyyyMMdd}-{Guid.NewGuid():N}",
            "PLAINTEXT",
            "nKafka.Client.Benchmarks");
        
        await using var consumer = new Consumer<Memory<byte>?>(
            consumerConfig,
            new DummyBytesMessageDeserializer(),
            new DummyOffsetStorage(),
            NullLoggerFactory.Instance);
        await consumer.JoinGroupAsync(CancellationToken.None);

        var counter = 0;
        while (counter < scenario.MessageCount)
        {
            var consumeResults = await consumer.ConsumeBatchAsync(CancellationToken.None);
            foreach (var consumeResult in consumeResults)
            {
                if (consumeResult.Message == null)
                {
                    continue;
                }

                counter += 1;
            }
        }

        Console.WriteLine(counter);
    }

    private class DummyBytesMessageDeserializer : IMessageDeserializer<Memory<byte>?>
    {
        public Memory<byte>? Deserialize(MessageDeserializationContext context)
        {
            if (context.Value != null &&
                context.Value.Value.Length > 0)
            {
                return context.Value;
            }

            return null;
        }
    }

    private class DummyOffsetStorage : IOffsetStorage
    {
        public ValueTask<long> GetAsync(
            IConnection connection,
            string consumerGroup,
            string topic,
            int partition,
            CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(0L);
        }

        public ValueTask SetAsync(
            IConnection connection,
            string consumerGroup,
            string topic,
            int partition,
            long offset,
            CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
        }
    }
}