using Confluent.Kafka;
using Confluent.Kafka.Admin;
using MoreLinq;

namespace nKafka.Client.Benchmarks;

public class TopicInitializer
{
    public static async Task InitializeTestTopic(FetchScenario scenario)
    {
        var bootstrapServers = "PLAINTEXT://kafka-1:9192, PLAINTEXT://kafka-2:9292, PLAINTEXT://kafka-3:9392";
        var adminClientConfig = new AdminClientConfig { BootstrapServers = bootstrapServers };
        using var adminClient = new AdminClientBuilder(adminClientConfig).Build();
        
        var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
        if (metadata.Topics.Any(x => x.Topic == scenario.TopicName))
        {
            return;
        }
        
        await adminClient.CreateTopicsAsync(new[]
        {
            new TopicSpecification
            {
                Configs = new Dictionary<string, string>
                {
                    { "retention.ms", "-1"},
                    { "retention.bytes", "-1"},
                    { "min.insync.replicas", "2"},
                },
                Name = scenario.TopicName,
                NumPartitions = scenario.PartitionCount,
                ReplicationFactor = 2
            }
        });
        
        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            MessageTimeoutMs = 5000,
            Debug = "protocol",
        };

        using var producer = new ProducerBuilder<Null, byte[]>(config).Build();

        var random = new Random();
        var messages = Enumerable.Range(1, scenario.MessageCount)
            .Select(_ =>
            {
                var value = new byte[scenario.MessageSize];
                random.NextBytes(value);
                return new Message<Null, byte[]>
                {
                    Value = value,
                };
            });
        foreach (var batch in messages.Batch(100))
        {
            var tasks = batch
                .Select(x => producer.ProduceAsync(scenario.TopicName, x));
            await Task.WhenAll(tasks);
        }
    }
}