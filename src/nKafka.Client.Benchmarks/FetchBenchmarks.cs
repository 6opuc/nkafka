using BenchmarkDotNet.Attributes;

namespace nKafka.Client.Benchmarks;

[MemoryDiagnoser]
[ThreadingDiagnoser]
public class FetchBenchmarks
{
    [Benchmark]
    public Task Confluent() => ConfluentFetchTest.Test();
    
    [Benchmark]
    public Task NKafka() => NKafkaFetchTest.Test();
}