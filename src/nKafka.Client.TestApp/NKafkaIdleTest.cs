using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using nKafka.Client.Benchmarks;

namespace nKafka.Client.TestApp;

public static class NKafkaIdleTest
{
    public static async Task Test(FetchScenario scenario)
    {
        var loggerFactory = LoggerFactory.Create(builder => builder
            .SetMinimumLevel(LogLevel.Debug)
            .AddSimpleConsole(o => o.IncludeScopes = true));
        
        var consumerConfig = new ConsumerConfig(
            "PLAINTEXT://kafka-1:9192, PLAINTEXT://kafka-2:9292, PLAINTEXT://kafka-3:9392",
            scenario.TopicName,
            "test-consumer-group",
            $"testapp-{DateTime.UtcNow.Ticks}",
            "PLAINTEXT",
            "nKafka.Client.Benchmarks");
        
        await using var consumer = new Consumer<DummyStringMessage>(
            consumerConfig,
            new DummyStringMessageDeserializer(),
            new DummyOffsetStorage(),
            NullLoggerFactory.Instance/*loggerFactory*/);
        await consumer.JoinGroupAsync(CancellationToken.None);

        var counter = 0;
        while (true)
        {
            if (counter >= scenario.MessageCount)
            {
                Console.WriteLine($"{DateTime.UtcNow}: {counter} of {scenario.MessageCount}");
            }
            var consumeResult = await consumer.ConsumeAsync(CancellationToken.None);
            if (consumeResult.Message == null)
            {
                continue;
            }

            counter += 1;
        }
    }
    
    private class DummyStringMessage
    {
        public string? Value { get; set; }
    }
    
    private class DummyStringMessageDeserializer : IMessageDeserializer<DummyStringMessage>
    {
        public DummyStringMessage? Deserialize(MessageDeserializationContext context)
        {
            var result = new DummyStringMessage();
            if (context.Value != null &&
                context.Value.Value.Length > 0)
            {
                result.Value = Encoding.UTF8.GetString(context.Value.Value.Span);
            }

            return result;
        }
    }
    
    private class DummyOffsetStorage : IOffsetStorage
    {
        public ValueTask<long> GetOffset(string consumerGroup, string topic, int partition)
        {
            return ValueTask.FromResult(0L);
        }
    }
}