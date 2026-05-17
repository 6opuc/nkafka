using Confluent.Kafka;

// Parse CLI args
var bootstrapServers = args.ElementAtOrDefault(0) ?? "localhost:9192";
var saslUsername = args.ElementAtOrDefault(1) ?? "admin";
var saslPassword = args.ElementAtOrDefault(2) ?? "admin-secret";
var caCertPath = args.ElementAtOrDefault(3) ?? "../../../infra/secrets/ca-cert.pem";

var topicName = $"test-sasl-{Guid.NewGuid():N}";

Console.WriteLine($"=== SASL_SSL + SCRAM-SHA-512 Test ===");
Console.WriteLine($"Bootstrap: {bootstrapServers}");
Console.WriteLine($"User:      {saslUsername}");
Console.WriteLine($"CA cert:   {caCertPath}");
Console.WriteLine();

// Admin client: create a test topic
Console.WriteLine("Creating test topic...");
var adminConfig = new AdminClientConfig
{
    BootstrapServers = bootstrapServers,
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.ScramSha512,
    SaslUsername = saslUsername,
    SaslPassword = saslPassword,
    SslCaLocation = caCertPath,
};

using var adminClient = new AdminClientBuilder(adminConfig).Build();
try
{
    await adminClient.CreateTopicsAsync([new TopicSpecification
    {
        Name = topicName,
        NumPartitions = 1,
        ReplicationFactor = 1,
    }]);
    Console.WriteLine($"  Topic '{topicName}' created.");
}
catch (CreateTopicsException ex) when (ex.Results.Any(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
{
    Console.WriteLine($"  Topic '{topicName}' already exists.");
}

// Producer: send a message
Console.WriteLine("Producing message...");
var producerConfig = new ProducerConfig
{
    BootstrapServers = bootstrapServers,
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.ScramSha512,
    SaslUsername = saslUsername,
    SaslPassword = saslPassword,
    SslCaLocation = caCertPath,
};

using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();
var deliveryResult = await producer.ProduceAsync(topicName,
    new Message<Null, string> { Value = "Hello SASL_SSL!" });
Console.WriteLine($"  Produced to partition {deliveryResult.Partition} @ offset {deliveryResult.Offset}");

// Consumer: read the message back
Console.WriteLine("Consuming message...");
var consumerConfig = new ConsumerConfig
{
    BootstrapServers = bootstrapServers,
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.ScramSha512,
    SaslUsername = saslUsername,
    SaslPassword = saslPassword,
    SslCaLocation = caCertPath,
    GroupId = $"test-group-{Guid.NewGuid():N}",
    AutoOffsetReset = AutoOffsetReset.Earliest,
};

using var consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build();
consumer.Subscribe(topicName);

var consumeResult = consumer.Consume(TimeSpan.FromSeconds(10));
if (consumeResult?.Message?.Value != null)
{
    Console.WriteLine($"  Consumed: '{consumeResult.Message.Value}'");
    Console.WriteLine();
    Console.WriteLine("=== SUCCESS: SASL_SSL + SCRAM-SHA-512 works! ===");
}
else
{
    Console.WriteLine("  ERROR: No message consumed within timeout.");
    return 1;
}

// Cleanup
consumer.Close();

try
{
    await adminClient.DeleteTopicsAsync([topicName]);
    Console.WriteLine("  Topic cleaned up.");
}
catch
{
    // ignore
}

return 0;
