using Confluent.Kafka;

namespace nKafka.Client.Benchmarks;

public class ConfluentConsumeStringTest
{
    public static Task Test(FetchScenario scenario, string protocol)
    {
        var config = new Confluent.Kafka.ConsumerConfig
        {
            BootstrapServers = BenchmarkHelper.BootstrapServers(protocol),
            GroupId = Guid.NewGuid().ToString(),
            AutoOffsetReset = AutoOffsetReset.Earliest,
            CheckCrcs = false,
        }.ConfigureProtocol(protocol);

        config.FetchQueueBackoffMs = 10;

        using var consumer = new ConsumerBuilder<Null, string>(config).Build();
        consumer.Subscribe(scenario.TopicName);

        int counter = 0;
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

        return Task.CompletedTask;
    }
}
