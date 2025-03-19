// See https://aka.ms/new-console-template for more information

using Microsoft.Extensions.Logging;
using nKafka.Client;
using nKafka.Client.TestAppConsumerGroup;

var loggerFactory = LoggerFactory.Create(builder => builder
    .SetMinimumLevel(LogLevel.Information)
    .AddSimpleConsole(o => o.IncludeScopes = true));
var logger = loggerFactory.CreateLogger<Program>();

var consumerConfig = new ConsumerConfig(
    "PLAINTEXT://kafka-1:9192, PLAINTEXT://kafka-2:9292, PLAINTEXT://kafka-3:9392",
    "test_p12_m40K_s10KB",
    $"testapp-{DateTime.UtcNow.Ticks}",
    "test-consumer-group",
    Guid.NewGuid().ToString("N"),
    "PLAINTEXT");

await using var consumer = new Consumer<Memory<byte>?>(
    consumerConfig,
    new DummyBytesMessageDeserializer(),
    new DummyOffsetStorage(),
    loggerFactory);
await consumer.JoinGroupAsync(CancellationToken.None);

var counter = 0;
while (true)
{
    var consumeResult = await consumer.ConsumeAsync(CancellationToken.None);
    if (consumeResult?.Message != null)
    {
        counter += 1;
        logger.LogDebug("Consumed message: {partition}:{offset}", consumeResult.Value.Partition, consumeResult.Value.Offset);
        await consumer.CommitAsync(consumeResult.Value, CancellationToken.None);
    }
    
    await Task.Delay(TimeSpan.FromMilliseconds(300));
}

