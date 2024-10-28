using Confluent.Kafka;
using MoreLinq;

namespace nKafka.Client.Benchmarks;

public class TopicInitializer
{
    public static async Task Initialize()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "PLAINTEXT://kafka-1:9192, PLAINTEXT://kafka-2:9292, PLAINTEXT://kafka-3:9392",
            MessageTimeoutMs = 5000,
            Debug = "protocol",
        };

        using var producer = new ProducerBuilder<Null, string>(config).Build();

        var messages = Enumerable.Range(1, 1_000_000)
            .Select(x => new Message<Null, string> { Value = x.ToString() });
        foreach (var batch in messages.Batch(100))
        {
            var tasks = batch
                .Select(x => producer.ProduceAsync("test", x));
            await Task.WhenAll(tasks);
        }
    }
}