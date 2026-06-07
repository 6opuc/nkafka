BenchmarkDotNet v0.15.8, Linux Ubuntu 26.04 LTS (Resolute Raccoon)
AMD RYZEN AI MAX+ 395 w/ Radeon 8060S 1.67GHz, 1 CPU, 32 logical and 16 physical cores
.NET SDK 10.0.300
  [Host]     : .NET 10.0.8 (10.0.8, 10.0.826.23019), X64 RyuJIT x86-64-v4
  DefaultJob : .NET 10.0.8 (10.0.8, 10.0.826.23019), X64 RyuJIT x86-64-v4

| Method                    | Protocol  | Scenario     | Mean      | Error     | StdDev    | Gen0       | Gen1      | Allocated |
|-------------------------- |---------- |------------- |----------:|----------:|----------:|-----------:|----------:|----------:|
| ConfluentConsumeBytes     | PLAINTEXT | 12p 40Kx10KB | 244.87 ms |  2.392 ms |   2.120 ms | 25000.0000 |         - | 403.78 MB |
| ConfluentConsumeString    | PLAINTEXT | 12p 40Kx10KB | 249.60 ms |  2.627 ms |   2.457 ms | 49500.0000 | 3500.0000 | 794.41 MB |
| NKafkaFetchBytesSeq1Part  | PLAINTEXT | 12p 40Kx10KB |  53.63 ms |  1.065 ms |   1.457 ms |   666.6667 |         - |  10.89 MB |
| NKafkaFetchBytesSeqNPart  | PLAINTEXT | 12p 40Kx10KB |  41.08 ms |  0.728 ms |   1.386 ms |   636.3636 |  181.8182 |  10.34 MB |
| NKafkaFetchBytesParNPart  | PLAINTEXT | 12p 40Kx10KB |  19.44 ms |  0.297 ms |   0.305 ms |   718.7500 |  343.7500 |  11.87 MB |
| NKafkaFetchStringParNPart | PLAINTEXT | 12p 40Kx10KB |  65.22 ms |  1.288 ms |   2.984 ms | 60142.8571 |  142.8571 | 794.04 MB |
| NKafkaConsumeString       | PLAINTEXT | 12p 40Kx10KB |  76.51 ms |  1.497 ms |   1.893 ms | 50200.0000 |  200.0000 | 794.23 MB |
| NKafkaConsumeBytes        | PLAINTEXT | 12p 40Kx10KB |  23.22 ms |  0.451 ms |   0.570 ms |   750.0000 |  343.7500 |  12.05 MB |
| NKafkaBatchConsumeBytes   | PLAINTEXT | 12p 40Kx10KB |  22.86 ms |  0.268 ms |   0.224 ms |   750.0000 |  343.7500 |  12.34 MB |
| ConfluentConsumeBytes     | SASL_SSL  | 12p 40Kx10KB | 282.82 ms |  5.514 ms |   5.900 ms | 25000.0000 |         - | 403.78 MB |
| ConfluentConsumeString    | SASL_SSL  | 12p 40Kx10KB | 286.05 ms |  5.603 ms |   7.480 ms | 49500.0000 | 3500.0000 | 794.41 MB |
| NKafkaFetchBytesSeq1Part  | SASL_SSL  | 12p 40Kx10KB | 248.38 ms |  4.940 ms |  13.441 ms |   500.0000 |         - |  10.94 MB |
| NKafkaFetchBytesSeqNPart  | SASL_SSL  | 12p 40Kx10KB | 242.29 ms |  4.832 ms |   4.746 ms |   500.0000 |         - |   10.4 MB |
| NKafkaFetchBytesParNPart  | SASL_SSL  | 12p 40Kx10KB | 106.68 ms |  2.123 ms |   2.085 ms |   500.0000 |         - |  11.93 MB |
| NKafkaFetchStringParNPart | SASL_SSL  | 12p 40Kx10KB | 145.15 ms |  2.896 ms |   2.974 ms | 54500.0000 |  750.0000 |  794.1 MB |
| NKafkaConsumeString       | SASL_SSL  | 12p 40Kx10KB | 372.17 ms | 47.536 ms | 140.160 ms | 52000.0000 | 1000.0000 | 833.54 MB |
| NKafkaConsumeBytes        | SASL_SSL  | 12p 40Kx10KB | 132.53 ms |  2.572 ms |   2.526 ms |   750.0000 |  250.0000 |  12.13 MB |
| NKafkaBatchConsumeBytes   | SASL_SSL  | 12p 40Kx10KB | 130.94 ms |  2.554 ms |   2.508 ms |   750.0000 |  250.0000 |  13.65 MB |

