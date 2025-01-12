using System.Diagnostics;
using nKafka.Client.Benchmarks;

var threads = 1;
ThreadPool.SetMinThreads(threads, threads);
ThreadPool.SetMaxThreads(threads, threads);

var benchmarks = new FetchBenchmarks();
var scenario = benchmarks.Scenarios
    .First(x => x.MessageSize == 10 * 1024);

var stopwatch = Stopwatch.StartNew();

for (int i = 0; i < 10; i++)
{
    await NKafkaFetchTest.Test(scenario);
}

//await ConfluentFetchTest.Test(scenario);
//await ConfluentConsumeStringTest.Test(scenario);
//await NKafkaConsumeStringTest.Test(scenario);
//await NKafkaFetchTest.Test(scenario);
stopwatch.Stop();

Console.WriteLine($"Elapsed time: {stopwatch.ElapsedMilliseconds}ms.");