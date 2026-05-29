BenchmarkDotNet v0.15.8, Linux Ubuntu 26.04 LTS (Resolute Raccoon)                                                                                                                                         
AMD RYZEN AI MAX+ 395 w/ Radeon 8060S 1.67GHz, 1 CPU, 32 logical and 16 physical cores                                                                                                                     
.NET SDK 10.0.300                                                                                                                                                                                          
  [Host]     : .NET 10.0.8 (10.0.8, 10.0.826.23019), X64 RyuJIT x86-64-v4                                                                                                                                  
  DefaultJob : .NET 10.0.8 (10.0.8, 10.0.826.23019), X64 RyuJIT x86-64-v4                                                                                                                                  
                                                                                                                                                                                                           
                                                                                                                                                                                                           
| Method                    | Protocol  | Scenario     | Mean        | Error      | StdDev     | Median      | Completed Work Items | Lock Contentions | Gen0       | Gen1       | Gen2     | Allocated |  
|-------------------------- |---------- |------------- |------------:|-----------:|-----------:|------------:|---------------------:|-----------------:|-----------:|-----------:|---------:|----------:|  
| ConfluentConsumeBytes     | PLAINTEXT | 12p 40Kx10KB | 2,228.38 ms |   6.013 ms |   5.330 ms | 2,229.45 ms |                    - |                - | 25000.0000 |          - |        - | 403.77 MB |  
| ConfluentConsumeString    | PLAINTEXT | 12p 40Kx10KB | 2,688.39 ms | 181.696 ms | 535.735 ms | 2,241.33 ms |                    - |                - | 49000.0000 |  3000.0000 |        - |  794.4 MB |  
| NKafkaFetchBytesSeq1Part  | PLAINTEXT | 12p 40Kx10KB |    76.08 ms |   1.488 ms |   2.273 ms |    76.04 ms |            5219.4286 |                - |   714.2857 |          - |        - |  11.71 MB |  
| NKafkaFetchBytesSeqNPart  | PLAINTEXT | 12p 40Kx10KB |    60.11 ms |   1.199 ms |   3.342 ms |    59.83 ms |            3587.0000 |                - |   666.6667 |   111.1111 |        - |  10.64 MB |  
| NKafkaFetchBytesParNPart  | PLAINTEXT | 12p 40Kx10KB |    24.17 ms |   0.406 ms |   0.360 ms |    24.06 ms |            3759.4688 |                - |   750.0000 |   250.0000 |        - |  12.17 MB |  
| NKafkaFetchStringParNPart | PLAINTEXT | 12p 40Kx10KB |    54.52 ms |   1.083 ms |   0.846 ms |    54.42 ms |            3115.1111 |                - | 58666.6667 | 20777.7778 | 111.1111 | 794.35 MB |  
| NKafkaConsumeString       | PLAINTEXT | 12p 40Kx10KB |    64.85 ms |   1.273 ms |   2.263 ms |    65.38 ms |            3260.6667 |                - | 50500.0000 | 16666.6667 | 166.6667 | 794.47 MB |  
| NKafkaConsumeBytes        | PLAINTEXT | 12p 40Kx10KB |    27.45 ms |   0.538 ms |   0.504 ms |    27.41 ms |            4014.1875 |                - |   750.0000 |   343.7500 |        - |  12.32 MB |  
| NKafkaBatchConsumeBytes   | PLAINTEXT | 12p 40Kx10KB |    27.64 ms |   0.495 ms |   0.607 ms |    27.56 ms |            3728.5938 |                - |   750.0000 |   343.7500 |        - |  12.32 MB |  
| ConfluentConsumeBytes     | SASL_SSL  | 12p 40Kx10KB |   290.80 ms |   5.626 ms |   5.525 ms |   291.40 ms |                    - |                - | 25000.0000 |          - |        - | 403.78 MB |  
| ConfluentConsumeString    | SASL_SSL  | 12p 40Kx10KB |   291.97 ms |   5.765 ms |   6.863 ms |   293.32 ms |                    - |                - | 49500.0000 |  3500.0000 |        - | 794.41 MB |  
| NKafkaFetchBytesSeq1Part  | SASL_SSL  | 12p 40Kx10KB |   291.23 ms |   5.762 ms |  15.180 ms |   287.90 ms |           37498.0000 |                - |          - |          - |        - |  11.81 MB |  
| NKafkaFetchBytesSeqNPart  | SASL_SSL  | 12p 40Kx10KB |   275.39 ms |   5.506 ms |  13.608 ms |   272.80 ms |           38713.5000 |                - |   500.0000 |          - |        - |  10.71 MB |  
| NKafkaFetchBytesParNPart  | SASL_SSL  | 12p 40Kx10KB |   116.92 ms |   2.286 ms |   2.348 ms |   116.93 ms |           35104.0000 |                - |   750.0000 |          - |        - |  12.24 MB |  
| NKafkaFetchStringParNPart | SASL_SSL  | 12p 40Kx10KB |   149.14 ms |   2.838 ms |   2.516 ms |   149.44 ms |           34927.5000 |                - | 54500.0000 | 17250.0000 |        - | 794.42 MB |  
| NKafkaConsumeString       | SASL_SSL  | 12p 40Kx10KB |   157.14 ms |   3.127 ms |   5.795 ms |   156.26 ms |           29138.0000 |           1.0000 | 50000.0000 | 18000.0000 |        - | 794.57 MB |  
| NKafkaConsumeBytes        | SASL_SSL  | 12p 40Kx10KB |   143.89 ms |   2.827 ms |   3.365 ms |   142.49 ms |           37198.2500 |                - |   750.0000 |   250.0000 |        - |  12.41 MB |  
| NKafkaBatchConsumeBytes   | SASL_SSL  | 12p 40Kx10KB |   143.90 ms |   2.584 ms |   2.538 ms |   143.40 ms |           37860.5000 |                - |   750.0000 |   250.0000 |        - |  13.41 MB |  
                                            
