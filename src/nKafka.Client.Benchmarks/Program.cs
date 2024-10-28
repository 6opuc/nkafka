// See https://aka.ms/new-console-template for more information


using BenchmarkDotNet.Running;
using nKafka.Client.Benchmarks;

var scenarios = new FetchBenchmarks().Scenarios
    .GroupBy(x => x.TopicName, (k, g) => g.First())
    .ToList();
foreach (var scenario in scenarios)
{
    await TopicInitializer.InitializeTestTopic(scenario);
}

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);