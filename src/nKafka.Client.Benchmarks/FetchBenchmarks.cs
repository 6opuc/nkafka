using BenchmarkDotNet.Attributes;

namespace nKafka.Client.Benchmarks;

[MemoryDiagnoser]
[ThreadingDiagnoser]
public class FetchBenchmarks
{
    [ParamsSource(nameof(Scenarios))] public FetchScenario? Scenario { get; set; }

    public IEnumerable<FetchScenario> Scenarios => new[]
    {
        new FetchScenario { PartitionCount = 12, MessageCount = 1_000_000, MessageSize = 4 },
        new FetchScenario { PartitionCount = 12, MessageCount = 10_000, MessageSize = 4 * 1024 },
        new FetchScenario { PartitionCount = 12, MessageCount = 1_000, MessageSize = 40 * 1024 },
        new FetchScenario { PartitionCount = 12, MessageCount = 1_000, MessageSize = 400 * 1024 },
    };


    [Benchmark]
    public Task Confluent() => ConfluentFetchTest.Test(Scenario!);

    [Benchmark]
    public Task nKafka() => NKafkaFetchTest.Test(Scenario!);
}