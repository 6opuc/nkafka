using System.Diagnostics;
using Microsoft.Diagnostics.Tracing.Parsers.Clr;
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
                //await ConfluentFetchTest.Test(scenario);
                //await NKafkaFetchTest.Test(scenario);
                
                //await NKafkaIdleTest.Test(scenario);
                //await ConfluentIdleTest.Test(scenario);
                //await NKafkaBytesFetchParallelMultiPartTest.Test(scenario);
                #warning iteration#99 Failed to join consumer group. Error code 82(fixed)
                //-- at nKafka.Client.Consumer`1.JoinGroupRequestAsync(IConnection connection, CancellationToken cancellationToken) in /home/boris/projects/nkafka/src/nKafka.Client/Consumer.cs:line 312
                //-- The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id.
                // Unhandled exception. System.InvalidOperationException: An attempt was made to transition a task to a final state when it had already completed.
                // at nKafka.Client.Connection.CancelAllPending() in /home/boris/projects/nkafka/src/nKafka.Client/Connection.cs:line 400
                // at nKafka.Client.Consumer`1.CancelAllPending() in /home/boris/projects/nkafka/src/nKafka.Client/Consumer.cs:line 723
                // at nKafka.Client.Consumer`1.DisposeAsync() in /home/boris/projects/nkafka/src/nKafka.Client/Consumer.cs:line 704

                await NKafkaConsumeStringTest.Test(scenario);

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