using System.Diagnostics;
using nKafka.Client.Benchmarks;
using nKafka.Client.TestApp;

// var threads = 1;
// ThreadPool.SetMinThreads(threads, threads);
// ThreadPool.SetMaxThreads(threads, threads);

var benchmarks = new FetchBenchmarks();
var scenario = benchmarks.Scenarios
    .First(x => x.MessageSize == 10 * 1024);

var stopwatch = Stopwatch.StartNew();

var threads = 20;
var iterationsPerThread = 100;
var tasks = new List<Task>(threads);
for (var t = 0; t < tasks.Capacity; t++)
{
    var t1 = t;
    tasks.Add(Task.Run(
        async () =>
        {
            for (int i = 0; i < iterationsPerThread; i++)
            {
                var id = $"[{t1}:{i}]";
                var stopwatchInner = Stopwatch.StartNew();
                //await ConfluentConsumeBytesTest.Test(scenario);
                //await NKafkaFetchBytesSeqSinglePartTest.Test(scenario);
                //await NKafkaFetchBytesSeqMultiPartTest.Test(scenario);
                //await NKafkaFetchBytesParallelMultiPartTest.Test(scenario);
                //await NKafkaConsumeBytesTest.Test(scenario);
                
                //await ConfluentIdleTest.Test(scenario);
                //await NKafkaIdleTest.Test(scenario);
                
                //await ConfluentConsumeStringTest.Test(scenario);
                await NKafkaFetchStringParallelMultiPartTest.Test(scenario);
                //await NKafkaConsumeStringTest.Test(scenario);

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