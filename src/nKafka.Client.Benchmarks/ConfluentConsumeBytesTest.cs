using Confluent.Kafka;

namespace nKafka.Client.Benchmarks;

public class ConfluentConsumeBytesTest
{
    public static Task Test(FetchScenario scenario, string protocol)
    {
        var config = new Confluent.Kafka.ConsumerConfig
        {
            BootstrapServers = BenchmarkHelper.BootstrapServers(protocol),
            GroupId = Guid.NewGuid().ToString(),
            AutoOffsetReset = AutoOffsetReset.Earliest,
            CheckCrcs = false,
        };

        if (protocol == "SASL_SSL")
        {
            config.SslCaLocation = BenchmarkHelper.GetCACertPath();
            config.SaslMechanism = SaslMechanism.ScramSha512;
            config.SaslUsername = "admin";
            config.SaslPassword = "admin-secret";
            config.SecurityProtocol = SecurityProtocol.SaslSsl;
        }

        using var consumer = new ConsumerBuilder<Null, byte[]>(config).Build();
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
