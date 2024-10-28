using Confluent.Kafka;

namespace nKafka.Client.Benchmarks;

public class ConfluentFetchTest
{
    public static Task Test()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "PLAINTEXT://kafka-1:9192, PLAINTEXT://kafka-2:9292, PLAINTEXT://kafka-3:9392",
            GroupId = Guid.NewGuid().ToString(),
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        consumer.Subscribe("test");

        var counter = 0;
        while (counter < 1_000_000)
        {
            var consumeResult = consumer.Consume(CancellationToken.None);

            counter += 1;
        }

        consumer.Close();

        Console.WriteLine(counter);

        return Task.CompletedTask;
    }
}