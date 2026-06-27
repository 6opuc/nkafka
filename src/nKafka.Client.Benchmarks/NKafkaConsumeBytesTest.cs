using Microsoft.Extensions.Logging.Abstractions;

namespace nKafka.Client.Benchmarks;

public static class NKafkaConsumeBytesTest
{
    public static async Task Test(FetchScenario scenario, string protocol)
    {
        var consumerConfig = new ConsumerConfig(
            BenchmarkHelper.BootstrapServers(protocol),
            scenario.TopicName,
            $"testapp-{DateTime.UtcNow.Date:yyyyMMdd}-{Guid.NewGuid():N}",
            $"test-consumer-group-{Guid.NewGuid():N}",
            $"test-instance-{Guid.NewGuid():N}",
            protocol,
            MaxWaitTime: TimeSpan.FromMilliseconds(100))
            .ConfigureProtocol(protocol);

        Consumer<Memory<byte>?>? consumer = null;
        try
        {
            consumer = new Consumer<Memory<byte>?>(
                consumerConfig,
                new DummyBytesMessageDeserializer(),
                new DummyOffsetStorage(),
                NullLoggerFactory.Instance);
            await consumer.JoinGroupAsync(CancellationToken.None);

            var counter = 0;
            while (counter < scenario.MessageCount)
            {
                var consumeResult = await consumer.ConsumeAsync(CancellationToken.None);
                if (consumeResult?.Message == null)
                {
                    continue;
                }

                counter += 1;
            }

            await consumer.DisposeAsync();
        }
        finally
        {
            if (consumer != null)
            {
                await consumer.DisposeAsync();
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
