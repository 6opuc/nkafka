# nKafka

Yet another [more] efficient Kafka client implementation for .net ;)

The work is still ongoing, but there are already promising results comparing to the official client:
- **60x** less memory allocations
- **16x** ~~faster~~ more efficient in CPU utilization
- it scales much better with the number of consumers (**10x** less memory footprint for 20 consumers)

## Benchmarks
- one topic with 12 partitions, 40K messages of 10KB in size
- one consumer which reads the message payload as plain bytes
- the test ends when all messages from the topic are read

This benchmark just shows the overhead of the client itself.


```
BenchmarkDotNet v0.14.0, Fedora Linux 41 (Workstation Edition)
AMD Ryzen 5 7530U with Radeon Graphics, 1 CPU, 12 logical and 6 physical cores
.NET SDK 9.0.100
  [Host]     : .NET 8.0.10 (8.0.1024.46610), X64 RyuJIT AVX2
  DefaultJob : .NET 8.0.10 (8.0.1024.46610), X64 RyuJIT AVX2
```
| Method                    | Scenario     | Mean       | Gen0        | Gen1       | Allocated |
|-------------------------- |------------- |-----------:|------------:|-----------:|----------:|
| ConfluentConsumeBytes     | 12p 40Kx10KB | 2,227.6 ms |  50000.0000 |  1000.0000 | 405.61 MB |
| **NKafkaConsumeBytes**    | 12p 40Kx10KB |   137.8 ms |    750.0000 |          - |    6.8 MB |

## Current status
> [!CAUTION]
> Not ready for production.

- [x] You can use `FetchRequest` for efficient/fast queries to kafka topics(see `NKafkaFetchBytesSeqSinglePartTest`).
- [ ] Consumer implementation is still in progress
- [ ] Producer implementation is not started yet
- [ ] Compression is not implemented yet
- [ ] Metrics/Telemetry is not implemented yet

## Links
- The idea of code generation from kafka message definitions is taken from this interesting project: https://github.com/Fresa/Kafka.Protocol
