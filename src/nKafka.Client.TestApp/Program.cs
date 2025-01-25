using System.Diagnostics;
using nKafka.Client.Benchmarks;

var threads = 1;
ThreadPool.SetMinThreads(threads, threads);
ThreadPool.SetMaxThreads(threads, threads);

var benchmarks = new FetchBenchmarks();
var scenario = benchmarks.Scenarios
    .First(x => x.MessageSize == 10 * 1024);

var stopwatch = Stopwatch.StartNew();

var tasks = new List<Task>(1);
for (var t = 0; t < tasks.Capacity; t++)
{
    tasks.Add(Task.Run(async () =>
    {
        for (int i = 0; i < 100; i++)
        {
            var stopwatchInner = Stopwatch.StartNew();
            //await ConfluentFetchTest.Test(scenario);
            await NKafkaFetchTest.Test(scenario);
            
            stopwatchInner.Stop();
            Console.WriteLine($"Elapsed time: {stopwatchInner.ElapsedMilliseconds}ms");
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