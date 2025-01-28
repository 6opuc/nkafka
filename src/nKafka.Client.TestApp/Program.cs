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

var threads = 200;
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
                //await ConfluentFetchTest.Test(scenario);
                //await NKafkaFetchTest.Test(scenario);
                
                //await NKafkaIdleTest.Test(scenario);
                #warning why confluent eats x10 memory if TaskFactory.Run(.., longrunning) is used??
                await ConfluentIdleTest.Test(scenario);

                stopwatchInner.Stop();
                Console.WriteLine($"{id}: {stopwatchInner.ElapsedMilliseconds}ms");
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