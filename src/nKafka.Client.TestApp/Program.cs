// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using nKafka.Client.Benchmarks;


var benchmarks = new FetchBenchmarks();
var scenario = benchmarks.Scenarios
    .First(x => x.MessageSize == 40 * 1024);

var stopwatch = Stopwatch.StartNew();

//await NKafkaFetchTest.Test(scenario);
//await ConfluentFetchTest.Test(scenario);
await ConfluentConsumeStringTest.Test(scenario);
//await NKafkaConsumeStringTest.Test(scenario);
stopwatch.Stop();

Console.WriteLine($"Elapsed time: {stopwatch.ElapsedMilliseconds}ms.");
