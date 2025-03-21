using BenchmarkDotNet.Attributes;

namespace nKafka.Client.Benchmarks;

[MemoryDiagnoser]
[ThreadingDiagnoser]
public class FetchBenchmarks
{
    [ParamsSource(nameof(Scenarios))] public FetchScenario? Scenario { get; set; }

    public IEnumerable<FetchScenario> Scenarios => new[]
    {
        //new FetchScenario { PartitionCount = 12, MessageCount = 1_000_000, MessageSize = 4 },
        //new FetchScenario { PartitionCount = 12, MessageCount = 100_000, MessageSize = 4 * 1024 },
        //new FetchScenario { PartitionCount = 12, MessageCount = 10_000, MessageSize = 40 * 1024 },
        //new FetchScenario { PartitionCount = 12, MessageCount = 4_000, MessageSize = 100 * 1024 },
        new FetchScenario { PartitionCount = 12, MessageCount = 40_000, MessageSize = 10 * 1024 },
        //new FetchScenario { PartitionCount = 12, MessageCount = 1_000, MessageSize = 400 * 1024 },
    };


    [Benchmark]
    public Task ConfluentConsumeBytes() => ConfluentConsumeBytesTest.Test(Scenario!);
    
    [Benchmark]
    public Task ConfluentConsumeString() => ConfluentConsumeStringTest.Test(Scenario!);

    [Benchmark]
    public Task NKafkaFetchBytesSeq1Part() => NKafkaFetchBytesSeqSinglePartTest.Test(Scenario!);
    
    [Benchmark]
    public Task NKafkaFetchBytesSeqNPart() => NKafkaFetchBytesSeqMultiPartTest.Test(Scenario!);
    
    [Benchmark]
    public Task NKafkaFetchBytesParNPart() => NKafkaFetchBytesParallelMultiPartTest.Test(Scenario!);
    
    [Benchmark]
    public Task NKafkaFetchStringParNPart() => NKafkaFetchStringParallelMultiPartTest.Test(Scenario!);
    
    [Benchmark]
    public Task NKafkaConsumeString() => NKafkaConsumeStringTest.Test(Scenario!);
    
    [Benchmark]
    public Task NKafkaConsumeBytes() => NKafkaConsumeBytesTest.Test(Scenario!);
    
    [Benchmark]
    public Task NKafkaBatchConsumeBytes() => NKafkaBatchConsumeBytesTest.Test(Scenario!);
}