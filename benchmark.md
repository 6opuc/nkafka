BenchmarkDotNet v0.15.8, Linux Ubuntu 26.04 LTS (Resolute Raccoon)                                                                                                                                         
AMD RYZEN AI MAX+ 395 w/ Radeon 8060S 1.67GHz, 1 CPU, 32 logical and 16 physical cores                                                                                                                     
.NET SDK 10.0.300                                                                                                                                                                                          
  [Host]     : .NET 10.0.8 (10.0.8, 10.0.826.23019), X64 RyuJIT x86-64-v4                                                                                                                                  
  DefaultJob : .NET 10.0.8 (10.0.8, 10.0.826.23019), X64 RyuJIT x86-64-v4                                                                                                                                
                                                                                                                                                                                                              
| Method                    | Protocol  | Scenario     | Mean      | Error    | StdDev   | Gen0       | Completed Work Items | Lock Contentions | Gen1     | Allocated |
|-------------------------- |---------- |------------- |----------:|---------:|---------:|-----------:|---------------------:|-----------------:|---------:|----------:|
| ConfluentConsumeBytes     | PLAINTEXT | 12p 40Kx10KB | 245.60 ms |  3.305 ms |   3.092 ms | 25333.3333 |                    - |                - |         - | 403.78 MB |
| ConfluentConsumeString    | PLAINTEXT | 12p 40Kx10KB | 252.91 ms |  3.841 ms |   3.593 ms | 49500.0000 |                    - |                - | 3500.0000 | 794.41 MB |
| NKafkaFetchBytesSeq1Part  | PLAINTEXT | 12p 40Kx10KB |  53.74 ms |  1.066 ms |   2.361 ms |   666.6667 |            5209.6667 |                - |         - |  10.87 MB |
| NKafkaFetchBytesSeqNPart  | PLAINTEXT | 12p 40Kx10KB |  40.06 ms |  0.790 ms |   1.815 ms |   583.3333 |            4186.3333 |                - |   83.3333 |  10.31 MB |
| NKafkaFetchBytesParNPart  | PLAINTEXT | 12p 40Kx10KB |  19.08 ms |  0.206 ms |   0.193 ms |   718.7500 |            3253.0938 |                - |  343.7500 |  11.84 MB |
| NKafkaFetchStringParNPart | PLAINTEXT | 12p 40Kx10KB |  64.28 ms |  1.281 ms |   2.969 ms | 61750.0000 |            2711.1250 |                - |  125.0000 | 794.02 MB |
| NKafkaConsumeString       | PLAINTEXT | 12p 40Kx10KB |  74.72 ms |  1.480 ms |   1.705 ms | 50200.0000 |            2695.0000 |                - |  200.0000 | 794.18 MB |
| NKafkaConsumeBytes        | PLAINTEXT | 12p 40Kx10KB |  23.83 ms |  0.334 ms |   0.279 ms |   750.0000 |            3114.6250 |                - |  343.7500 |  12.04 MB |
| NKafkaBatchConsumeBytes   | PLAINTEXT | 12p 40Kx10KB |  23.91 ms |  0.475 ms |   0.807 ms |   750.0000 |            3584.6250 |                - |  375.0000 |  12.01 MB |
| ConfluentConsumeBytes     | SASL_SSL  | 12p 40Kx10KB | 285.18 ms |  5.522 ms |   5.670 ms | 25000.0000 |                    - |                - |         - | 403.78 MB |
| ConfluentConsumeString    | SASL_SSL  | 12p 40Kx10KB | 292.49 ms |  5.621 ms |   7.881 ms | 49500.0000 |                    - |                - | 3500.0000 | 794.41 MB |
| NKafkaFetchBytesSeq1Part  | SASL_SSL  | 12p 40Kx10KB | 256.20 ms |  5.114 ms |  12.924 ms |   500.0000 |           36647.0000 |                - |         - |  10.92 MB |
| NKafkaFetchBytesSeqNPart  | SASL_SSL  | 12p 40Kx10KB | 245.24 ms |  4.840 ms |   7.245 ms |   500.0000 |           37938.5000 |                - |         - |  10.38 MB |
| NKafkaFetchBytesParNPart  | SASL_SSL  | 12p 40Kx10KB | 105.89 ms |  1.456 ms |   1.362 ms |   500.0000 |           32980.0000 |                - |         - |  11.91 MB |
| NKafkaFetchStringParNPart | SASL_SSL  | 12p 40Kx10KB | 146.22 ms |  2.699 ms |   2.651 ms | 57000.0000 |           32997.3333 |                - |  666.6667 | 794.09 MB |
| NKafkaConsumeString       | SASL_SSL  | 12p 40Kx10KB | 384.58 ms | 54.887 ms | 160.974 ms | 50000.0000 |           22029.0000 |           0.5000 |  500.0000 | 794.27 MB |
| NKafkaConsumeBytes        | SASL_SSL  | 12p 40Kx10KB | 132.54 ms |  1.946 ms |   1.625 ms |   750.0000 |           33706.7500 |                - |  250.0000 |  12.14 MB |
| NKafkaBatchConsumeBytes   | SASL_SSL  | 12p 40Kx10KB | 133.25 ms |  2.025 ms |   1.894 ms |   750.0000 |           32356.5000 |                - |  250.0000 |   12.1 MB |

