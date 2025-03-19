using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace nKafka.Client.Benchmarks;

public static class NKafkaConsumeStringTest
{
    private static ILoggerFactory _loggerFactory = LoggerFactory.Create(builder => builder
        .SetMinimumLevel(LogLevel.Debug)
        .AddSimpleConsole(o => o.IncludeScopes = true));
    
    public static async Task Test(FetchScenario scenario)
    {
        var consumerConfig = new ConsumerConfig(
            "PLAINTEXT://kafka-1:9192, PLAINTEXT://kafka-2:9292, PLAINTEXT://kafka-3:9392",
            scenario.TopicName,
            "test-consumer-group",
            $"testapp-{DateTime.UtcNow.Date:yyyyMMdd}-{Guid.NewGuid():N}",
            "PLAINTEXT",
            "nKafka.Client.Benchmarks");
        
        await using var consumer = new Consumer<string>(
            consumerConfig,
            new DummyStringMessageDeserializer(),
            new DummyOffsetStorage(),
            NullLoggerFactory.Instance/*loggerFactory*/);
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

        Console.WriteLine($"{DateTime.UtcNow}: {counter}");
    }

    private class DummyStringMessageDeserializer : IMessageDeserializer<string>
    {
        public string? Deserialize(MessageDeserializationContext context)
        {
            if (context.Value != null &&
                context.Value.Value.Length > 0)
            {
                return Encoding.UTF8.GetString(context.Value.Value.Span);
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