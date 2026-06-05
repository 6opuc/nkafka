using Microsoft.Extensions.Logging.Abstractions;

namespace nKafka.Client.Benchmarks;

public static class NKafkaBatchConsumeBytesTest
{
    public static async Task Test(FetchScenario scenario, string protocol)
    {
        var consumerConfig = new ConsumerConfig(
            BenchmarkHelper.BootstrapServers(protocol),
            scenario.TopicName,
            $"testapp-{DateTime.UtcNow.Date:yyyyMMdd}-{Guid.NewGuid():N}",
            $"test-consumer-group-{Guid.NewGuid():N}",
            $"test-instance-{Guid.NewGuid():N}",
            protocol)
            .ConfigureProtocol(protocol);
        
        await using var consumer = new Consumer<Memory<byte>?>(
            consumerConfig,
            new DummyBytesMessageDeserializer(),
            new DummyOffsetStorage(),
            NullLoggerFactory.Instance);
        await consumer.JoinGroupAsync(CancellationToken.None);

        int counter = 0;
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
