BenchmarkDotNet v0.15.8, Linux Ubuntu 26.04 LTS (Resolute Raccoon)                                                                                                                                         
AMD RYZEN AI MAX+ 395 w/ Radeon 8060S 1.68GHz, 1 CPU, 32 logical and 16 physical cores                                                                                                                     
.NET SDK 10.0.300                                                                                                                                                                                          
  [Host]     : .NET 10.0.8 (10.0.8, 10.0.826.23019), X64 RyuJIT x86-64-v4                                                                                                                                  
  DefaultJob : .NET 10.0.8 (10.0.8, 10.0.826.23019), X64 RyuJIT x86-64-v4                                                                                                                                  
                                                                                                                                                                                                           
                                                                                                                                                                                                           
| Method                    | Scenario     | Mean        | Error      | StdDev     | Median      | Completed Work Items | Lock Contentions | Gen0       | Gen1       | Allocated |                         
|-------------------------- |------------- |------------:|-----------:|-----------:|------------:|---------------------:|-----------------:|-----------:|-----------:|----------:|                         
| ConfluentConsumeBytes     | 12p 40Kx10KB | 2,553.84 ms | 158.910 ms | 468.551 ms | 2,228.72 ms |                    - |                - | 25000.0000 |          - | 403.77 MB |                         
| ConfluentConsumeString    | 12p 40Kx10KB | 3,220.28 ms |   4.453 ms |   4.166 ms | 3,220.03 ms |                    - |                - | 49000.0000 |  3000.0000 |  794.4 MB |                         
| NKafkaFetchBytesSeq1Part  | 12p 40Kx10KB |    80.06 ms |   1.601 ms |   2.493 ms |    80.29 ms |            8836.3333 |                - |   333.3333 |          - |   5.51 MB |                         
| NKafkaFetchBytesSeqNPart  | 12p 40Kx10KB |    82.03 ms |   1.598 ms |   2.440 ms |    82.16 ms |            9012.8333 |                - |   333.3333 |          - |   6.01 MB |                         
| NKafkaFetchBytesParNPart  | 12p 40Kx10KB |    35.36 ms |   0.397 ms |   0.441 ms |    35.23 ms |            7644.2143 |                - |   357.1429 |          - |   5.94 MB |
| NKafkaFetchStringParNPart | 12p 40Kx10KB |    58.68 ms |   1.168 ms |   2.223 ms |    58.56 ms |            7057.0000 |                - | 49444.4444 |  4666.6667 | 788.12 MB |
| NKafkaConsumeString       | 12p 40Kx10KB |    63.01 ms |   1.257 ms |   2.481 ms |    62.75 ms |            7273.8571 |                - | 49428.5714 | 10285.7143 | 788.43 MB |
| NKafkaConsumeBytes        | 12p 40Kx10KB |    40.44 ms |   0.774 ms |   1.564 ms |    39.97 ms |            8276.7692 |           0.1538 |   384.6154 |          - |   6.36 MB |
| NKafkaBatchConsumeBytes   | 12p 40Kx10KB |    39.13 ms |   0.629 ms |   0.525 ms |    39.14 ms |            9160.3333 |           0.0833 |   333.3333 |          - |   6.35 MB |

