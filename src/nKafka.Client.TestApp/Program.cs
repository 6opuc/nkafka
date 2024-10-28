// See https://aka.ms/new-console-template for more information

using nKafka.Client.Benchmarks;

var benchmarks = new FetchBenchmarks();
await NKafkaFetchTest.Test(benchmarks.Scenarios.First(x => x.MessageSize == 400 * 1024));