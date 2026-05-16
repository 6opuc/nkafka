BenchmarkDotNet v0.15.8, Linux Ubuntu 24.04.4 LTS (Noble Numbat)
AMD Ryzen 7 5700X 1.74GHz, 1 CPU, 16 logical and 8 physical cores
.NET SDK 10.0.203
  [Host]     : .NET 10.0.7 (10.0.7, 10.0.726.21808), X64 RyuJIT x86-64-v3
  DefaultJob : .NET 10.0.7 (10.0.7, 10.0.726.21808), X64 RyuJIT x86-64-v3


| Method                    | Scenario     | Mean        | Error      | StdDev     | Median      | Gen0       | Completed Work Items | Lock Contentions | Gen1       | Allocated |
|-------------------------- |------------- |------------:|-----------:|-----------:|------------:|-----------:|---------------------:|-----------------:|-----------:|----------:|
| ConfluentConsumeBytes     | 12p 40Kx10KB | 2,011.35 ms | 141.738 ms | 417.917 ms | 2,231.46 ms | 25000.0000 |                    - |                - |          - | 403.77 MB |
| ConfluentConsumeString    | 12p 40Kx10KB | 2,912.61 ms | 165.612 ms | 488.310 ms | 3,237.64 ms | 49000.0000 |                    - |                - |  3000.0000 |  794.4 MB |
| NKafkaFetchBytesSeq1Part  | 12p 40Kx10KB |   199.84 ms |   2.008 ms |   1.780 ms |   200.50 ms |          - |           11060.5000 |                - |          - |    5.7 MB |
| NKafkaFetchBytesSeqNPart  | 12p 40Kx10KB |   205.54 ms |   3.517 ms |   3.290 ms |   204.30 ms |   333.3333 |           11386.0000 |                - |          - |   6.21 MB |
| NKafkaFetchBytesParNPart  | 12p 40Kx10KB |    83.22 ms |   0.417 ms |   0.349 ms |    83.18 ms |   333.3333 |           11943.6667 |                - |          - |   6.14 MB |
| NKafkaFetchStringParNPart | 12p 40Kx10KB |   115.44 ms |   1.429 ms |   1.337 ms |   115.70 ms | 49500.0000 |           10930.0000 |                - |  4250.0000 | 788.32 MB |
| NKafkaConsumeString       | 12p 40Kx10KB |   108.00 ms |   1.531 ms |   1.279 ms |   108.04 ms | 49500.0000 |           10908.7500 |                - | 12000.0000 | 788.67 MB |
| NKafkaConsumeBytes        | 12p 40Kx10KB |    88.85 ms |   0.914 ms |   0.855 ms |    88.87 ms |   333.3333 |           12652.8333 |                - |          - |    6.6 MB |
| NKafkaBatchConsumeBytes   | 12p 40Kx10KB |    88.83 ms |   1.211 ms |   1.133 ms |    88.84 ms |   400.0000 |           12494.8000 |                - |          - |   6.59 MB |


BenchmarkDotNet v0.15.8, Linux Fedora Linux 44 (Workstation Edition)
AMD Ryzen 5 7530U with Radeon Graphics 3.51GHz, 1 CPU, 12 logical and 6 physical cores
.NET SDK 10.0.203
  [Host]     : .NET 10.0.7 (10.0.7, 10.0.726.21808), X64 RyuJIT x86-64-v3
  DefaultJob : .NET 10.0.7 (10.0.7, 10.0.726.21808), X64 RyuJIT x86-64-v3


| Method                  | Scenario     | Mean     | Error    | StdDev   | Median   | Completed Work Items | Lock Contentions | Gen0     | Allocated |
|------------------------ |------------- |---------:|---------:|---------:|---------:|---------------------:|-----------------:|---------:|----------:|
| NKafkaConsumeBytes      | 12p 40Kx10KB | 326.4 ms | 23.95 ms | 70.60 ms | 346.6 ms |           36089.0000 |                - | 500.0000 |   6.82 MB |
| NKafkaBatchConsumeBytes | 12p 40Kx10KB | 314.9 ms | 30.66 ms | 86.97 ms | 342.2 ms |           23434.0000 |                - |        - |    6.8 MB |


