BenchmarkDotNet v0.15.8, Linux Ubuntu 26.04 LTS (Resolute Raccoon)
AMD RYZEN AI MAX+ 395 w/ Radeon 8060S 1.68GHz, 1 CPU, 32 logical and 16 physical cores
.NET SDK 10.0.300
  [Host]     : .NET 10.0.8 (10.0.8, 10.0.826.23019), X64 RyuJIT x86-64-v4
  DefaultJob : .NET 10.0.8 (10.0.8, 10.0.826.23019), X64 RyuJIT x86-64-v4

| Method                    | Protocol  | Scenario     | Mean      | Error     | StdDev    | Gen0       | Gen1      | Allocated |
|-------------------------- |---------- |------------- |----------:|----------:|----------:|-----------:|----------:|----------:|
| ConfluentConsumeBytes     | PLAINTEXT | 12p 40Kx10KB | 246.03 ms |  4.057 ms |   3.795 ms | 25000.0000 |         - | 403.78 MB |
| ConfluentConsumeString    | PLAINTEXT | 12p 40Kx10KB | 254.11 ms |  4.119 ms |   3.853 ms | 49500.0000 | 3500.0000 | 794.41 MB |
| NKafkaFetchBytesSeq1Part  | PLAINTEXT | 12p 40Kx10KB |  53.87 ms |  1.076 ms |   1.543 ms |   600.0000 |         - |  10.89 MB |
| NKafkaFetchBytesSeqNPart  | PLAINTEXT | 12p 40Kx10KB |  42.12 ms |  0.837 ms |   1.990 ms |   615.3846 |  153.8462 |  10.34 MB |
| NKafkaFetchBytesParNPart  | PLAINTEXT | 12p 40Kx10KB |  20.30 ms |  0.401 ms |   0.818 ms |   718.7500 |  343.7500 |  11.87 MB |
| NKafkaFetchStringParNPart | PLAINTEXT | 12p 40Kx10KB |  64.95 ms |  1.268 ms |   2.350 ms | 62000.0000 |  125.0000 | 794.04 MB |
| NKafkaConsumeString       | PLAINTEXT | 12p 40Kx10KB |  77.84 ms |  1.369 ms |   1.144 ms | 50166.6667 |  166.6667 | 794.32 MB |
| NKafkaConsumeBytes        | PLAINTEXT | 12p 40Kx10KB |  23.74 ms |  0.473 ms |   0.876 ms |   750.0000 |  343.7500 |  12.05 MB |
| NKafkaBatchConsumeBytes   | PLAINTEXT | 12p 40Kx10KB |  23.61 ms |  0.469 ms |   1.000 ms |   843.7500 |  312.5000 |  13.87 MB |
| ConfluentConsumeBytes     | SASL_SSL  | 12p 40Kx10KB | 286.84 ms |  5.674 ms |   6.755 ms | 25000.0000 |         - | 403.78 MB |
| ConfluentConsumeString    | SASL_SSL  | 12p 40Kx10KB | 281.26 ms |  5.375 ms |   6.601 ms | 49500.0000 | 3500.0000 | 794.41 MB |
| NKafkaFetchBytesSeq1Part  | SASL_SSL  | 12p 40Kx10KB | 258.47 ms |  5.154 ms |  13.577 ms |   500.0000 |         - |  10.96 MB |
| NKafkaFetchBytesSeqNPart  | SASL_SSL  | 12p 40Kx10KB | 238.54 ms |  2.921 ms |   2.590 ms |   500.0000 |         - |   10.4 MB |
| NKafkaFetchBytesParNPart  | SASL_SSL  | 12p 40Kx10KB | 106.79 ms |  2.047 ms |   2.010 ms |   600.0000 |  200.0000 |  11.93 MB |
| NKafkaFetchStringParNPart | SASL_SSL  | 12p 40Kx10KB | 142.24 ms |  2.722 ms |   2.546 ms | 53333.3333 | 1000.0000 | 794.11 MB |
| NKafkaConsumeString       | SASL_SSL  | 12p 40Kx10KB | 360.03 ms | 77.641 ms | 228.925 ms | 51000.0000 | 1000.0000 | 820.35 MB |
| NKafkaConsumeBytes        | SASL_SSL  | 12p 40Kx10KB | 135.06 ms |  2.440 ms |   3.339 ms |   750.0000 |  250.0000 |  12.14 MB |
| NKafkaBatchConsumeBytes   | SASL_SSL  | 12p 40Kx10KB | 132.39 ms |  1.848 ms |   1.543 ms |   750.0000 |  250.0000 |   13.6 MB |
