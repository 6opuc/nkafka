using Confluent.Kafka;
using nKafka.Client.Benchmarks;

namespace nKafka.Client.TestApp;

public static class ConfluentIdleTest
{
    public static Task Test(FetchScenario scenario)
    {
        var config = new Confluent.Kafka.ConsumerConfig
        {
            BootstrapServers = "PLAINTEXT://kafka-1:9192, PLAINTEXT://kafka-2:9292, PLAINTEXT://kafka-3:9392",
            GroupId = Guid.NewGuid().ToString(),
            AutoOffsetReset = AutoOffsetReset.Earliest,
            CheckCrcs = false,
        };

        using var consumer = new ConsumerBuilder<Null, string>(config).Build();
        consumer.Subscribe(scenario.TopicName);

        var counter = 0;
        TimeSpan maxWaitTime = TimeSpan.Zero;
        while (true)
        {
            if (counter >= scenario.MessageCount)
            {
                maxWaitTime = TimeSpan.FromSeconds(5);
                Console.WriteLine($"{DateTime.UtcNow}: {counter} of {scenario.MessageCount}");
            }
            var consumeResult = consumer.Consume(maxWaitTime);
            if (consumeResult == null ||
                consumeResult.Message == null ||
                consumeResult.Message.Value == null)
            {
                continue;
            }

            counter += 1;
        }
    }
}