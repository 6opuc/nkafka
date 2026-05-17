using Confluent.Kafka;

namespace nKafka.Client.Benchmarks;

public class ConfluentConsumeBytesTest
{
    public static Task Test(FetchScenario scenario)
    {
        var config = new Confluent.Kafka.ConsumerConfig
        {
            BootstrapServers = "SASL_SSL://localhost:9192, SASL_SSL://localhost:9292, SASL_SSL://localhost:9392",
            GroupId = Guid.NewGuid().ToString(),
            AutoOffsetReset = AutoOffsetReset.Earliest,
            CheckCrcs = false,
            SslCaLocation = BenchmarkHelper.GetCACertPath(),
            SaslMechanism = SaslMechanism.ScramSha512,
            SaslUsername = "admin",
            SaslPassword = "admin-secret",
            SecurityProtocol = SecurityProtocol.SaslSsl,
        };

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
