using System.Diagnostics;
using Microsoft.Extensions.Logging;
using nKafka.Client;
using nKafka.Client.Benchmarks;
using nKafka.Client.TestApp;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

using var meterProvider = Sdk.CreateMeterProviderBuilder()
    .AddNKafka()
    .AddConsoleExporter()
    .Build();

using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("testapp"))
    .AddNKafka()
    .AddOtlpExporter(o =>
    {
        o.Endpoint = new Uri("http://otel-collector:4318");
    })
    .AddConsoleExporter()
    .Build();

var benchmarks = new FetchBenchmarks();
var scenario = benchmarks.Scenarios
    .First(x => x.MessageSize == 10 * 1024);

var stopwatch = Stopwatch.StartNew();

var threads = 1;
var iterationsPerThread = 3;
var tasks = new List<Task>(threads);
for (var t = 0; t < tasks.Capacity; t++)
{
    var t1 = t;
    tasks.Add(Task.Run(
        async () =>
        {
            for (var i = 0; i < iterationsPerThread; i++)
            {
                var id = $"[{t1}:{i}]";
                var stopwatchInner = Stopwatch.StartNew();
                //await ConfluentConsumeBytesTest.Test(scenario);
                //await NKafkaFetchBytesSeqSinglePartTest.Test(scenario, "SASL_SSL");
                //await NKafkaFetchBytesSeqMultiPartTest.Test(scenario, "SASL_SSL");
                //await NKafkaFetchBytesParallelMultiPartTest.Test(scenario, "SASL_SSL");
                //await NKafkaConsumeBytesTest.Test(scenario, "SASL_SSL");
                await NKafkaBatchConsumeBytesTest.Test(scenario, "SASL_SSL");

                //await ConfluentIdleTest.Test(scenario);
                //await NKafkaIdleTest.Test(scenario);

                //await ConfluentConsumeStringTest.Test(scenario);
                //await NKafkaFetchStringParallelMultiPartTest.Test(scenario, "SASL_SSL");
                //await NKafkaConsumeStringTest.Test(scenario, "SASL_SSL");

                stopwatchInner.Stop();
                Console.WriteLine($"{DateTime.UtcNow}: {id}: {stopwatchInner.ElapsedMilliseconds}ms");
            }
        }));
}

await Task.WhenAll(tasks);

//await ConfluentFetchTest.Test(scenario);
//await ConfluentConsumeStringTest.Test(scenario);
//await NKafkaConsumeStringTest.Test(scenario);
//await NKafkaFetchTest.Test(scenario);
stopwatch.Stop();

Console.WriteLine($"Total elapsed time: {stopwatch.ElapsedMilliseconds}ms.");
Console.WriteLine($"Average time: {stopwatch.ElapsedMilliseconds / threads / iterationsPerThread}ms.");
