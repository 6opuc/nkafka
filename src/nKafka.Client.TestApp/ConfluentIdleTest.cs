using Confluent.Kafka;
using nKafka.Client.Benchmarks;

namespace nKafka.Client.TestApp;

public static class ConfluentIdleTest
{
    public static Task Test(FetchScenario scenario)
    {
        var task = Task.Factory.StartNew(() =>
            {
                var config = new Confluent.Kafka.ConsumerConfig
                {
                    BootstrapServers = "PLAINTEXT://localhost:9192, PLAINTEXT://localhost:9292, PLAINTEXT://localhost:9392",
                    GroupId = Guid.NewGuid().ToString(),
                    AutoOffsetReset = AutoOffsetReset.Latest,
                    CheckCrcs = false,
                };

                using var consumer = new ConsumerBuilder<Null, string>(config).Build();
                consumer.Subscribe(scenario.TopicName);

                var maxWaitTime = TimeSpan.FromSeconds(5);
                while (true)
                {
                    consumer.Consume(maxWaitTime);
                }
            },
            TaskCreationOptions.LongRunning
        );

        return task;
    }
}
