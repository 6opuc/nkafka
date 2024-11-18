// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using Microsoft.Extensions.Logging;
using nKafka.Client;
using nKafka.Client.Benchmarks;
using nKafka.Client.TestApp;


var benchmarks = new FetchBenchmarks();
var scenario = benchmarks.Scenarios
    .First(x => x.MessageSize == 40 * 1024);

var stopwatch = Stopwatch.StartNew();

//await NKafkaFetchTest.Test(scenario);
//await ConfluentFetchTest.Test(scenario);


var consumerConfig = new ConsumerConfig(
    "PLAINTEXT://kafka-1:9192, PLAINTEXT://kafka-2:9292, PLAINTEXT://kafka-3:9392",
    scenario.TopicName,
    "test-consumer-group",
    $"testapp-{DateTime.UtcNow.Ticks}",
    "PLAINTEXT");
using var loggerFactory = LoggerFactory.Create(loggingBuilder => loggingBuilder
    .SetMinimumLevel(LogLevel.Error)
    .AddSimpleConsole(options =>
    {
        options.IncludeScopes = true;
        options.SingleLine = true;
        options.TimestampFormat = "HH:mm:ss ";
    }));
{
    await using var consumer = new Consumer<DummyStringMessage>(
        consumerConfig,
        new DummyStringMessageDeserializer(),
        loggerFactory);
    await consumer.JoinGroupAsync(CancellationToken.None);
    
    var counter = 0;
    while (counter < scenario.MessageCount)
    {
        var consumeResult = await consumer.ConsumeAsync(CancellationToken.None);
        if (consumeResult == null ||
            consumeResult.Value.Message == null)
        {
            continue;
        }

        counter += 1;
    }
    Console.WriteLine(counter);
    

    //await Task.Delay(TimeSpan.FromSeconds(60));
}
stopwatch.Stop();

Console.WriteLine($"Elapsed time: {stopwatch.ElapsedMilliseconds}ms.");
