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
                    BootstrapServers = "PLAINTEXT://kafka-1:9192, PLAINTEXT://kafka-2:9292, PLAINTEXT://kafka-3:9392",
                    GroupId = Guid.NewGuid().ToString(),
                    AutoOffsetReset = AutoOffsetReset.Latest,
                    CheckCrcs = false,
                };

                using var consumer = new ConsumerBuilder<Null, string>(config).Build();
                consumer.Subscribe(scenario.TopicName);

                TimeSpan maxWaitTime = TimeSpan.FromSeconds(5);
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