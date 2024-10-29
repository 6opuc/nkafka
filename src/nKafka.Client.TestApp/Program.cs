// See https://aka.ms/new-console-template for more information

using nKafka.Client.Benchmarks;

var benchmarks = new FetchBenchmarks();
var scenario = benchmarks.Scenarios
    .First(x => x.MessageSize == 4 * 1024);
await NKafkaFetchTest.Test(scenario);
//await ConfluentFetchTest.Test(scenario);