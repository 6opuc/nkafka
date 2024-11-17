using Confluent.Kafka;

namespace nKafka.Client.Benchmarks;

public class ConfluentFetchTest
{
    public static Task Test(FetchScenario scenario)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "PLAINTEXT://kafka-1:9192, PLAINTEXT://kafka-2:9292, PLAINTEXT://kafka-3:9392",
            GroupId = Guid.NewGuid().ToString(),
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<Null, byte[]>(config).Build();
        consumer.Subscribe(scenario.TopicName);

        var counter = 0;
        while (counter < scenario.MessageCount)
        {
            var consumeResult = consumer.Consume(CancellationToken.None);
            if (consumeResult.Message == null ||
                consumeResult.Message.Value == null)
            {
                continue;
            }

            counter += 1;
        }

        consumer.Close();

        Console.WriteLine(counter);

        return Task.CompletedTask;
    }
}