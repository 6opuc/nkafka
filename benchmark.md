BenchmarkDotNet v0.15.8, Linux Ubuntu 26.04 LTS (Resolute Raccoon)                                                                                                                                         
AMD RYZEN AI MAX+ 395 w/ Radeon 8060S 1.64GHz, 1 CPU, 32 logical and 16 physical cores                                                                                                                     
.NET SDK 10.0.300                                                                                                                                                                                          
  [Host]     : .NET 10.0.8 (10.0.8, 10.0.826.23019), X64 RyuJIT x86-64-v4                                                                                                                                  
  DefaultJob : .NET 10.0.8 (10.0.8, 10.0.826.23019), X64 RyuJIT x86-64-v4                                                                                                                                  
                                                                                                                                                                                                            
                                                                                                                                                                                                            
| Method                    | Protocol  | Scenario     | Mean      | Error     | StdDev     | Completed Work Items | Lock Contentions | Gen0       | Gen1      | Allocated |  
|-------------------------- |---------- |------------- |----------:|----------:|-----------:|---------------------:|-----------------:|-----------:|----------:|----------:|  
| ConfluentConsumeBytes     | PLAINTEXT | 12p 40Kx10KB | 245.65 ms |  1.576 ms |   1.230 ms |                    - |                - | 25000.0000 |         - | 403.77 MB |  
| ConfluentConsumeString    | PLAINTEXT | 12p 40Kx10KB | 256.42 ms |  3.345 ms |   3.129 ms |                    - |                - | 49500.0000 |  3500.0000 |  794.4 MB |  
| NKafkaFetchBytesSeq1Part  | PLAINTEXT | 12p 40Kx10KB |  51.82 ms |  0.984 ms |   1.987 ms |            5172.2500 |                - |   625.0000 |         - |  10.87 MB |  
| NKafkaFetchBytesSeqNPart  | PLAINTEXT | 12p 40Kx10KB |  39.25 ms |  0.772 ms |   1.331 ms |            4013.0000 |                - |   615.3846 |  153.8462 |  10.31 MB |  
| NKafkaFetchBytesParNPart  | PLAINTEXT | 12p 40Kx10KB |  18.97 ms |  0.157 ms |   0.131 ms |            3285.4375 |                - |   718.7500 |  343.7500 |  11.84 MB |  
| NKafkaFetchStringParNPart | PLAINTEXT | 12p 40Kx10KB |  64.48 ms |  1.281 ms |   3.757 ms |            3257.7143 |                - | 61142.8571 |  571.4286 | 794.02 MB |  
| NKafkaConsumeString       | PLAINTEXT | 12p 40Kx10KB |  76.55 ms |  1.530 ms |   2.985 ms |            2839.8571 |                - | 50142.8571 |  142.8571 | 794.18 MB |  
| NKafkaConsumeBytes        | PLAINTEXT | 12p 40Kx10KB |  23.69 ms |  0.280 ms |   0.248 ms |            3219.2500 |                - |   750.0000 |  343.7500 |  12.05 MB |  
| NKafkaBatchConsumeBytes   | PLAINTEXT | 12p 40Kx10KB |  23.50 ms |  0.464 ms |   0.788 ms |            3124.3750 |                - |   750.0000 |  343.7500 |  12.01 MB |  
| ConfluentConsumeBytes     | SASL_SSL  | 12p 40Kx10KB | 285.73 ms |  5.550 ms |   5.192 ms |                    - |                - | 25000.0000 |         - | 403.78 MB |  
| ConfluentConsumeString    | SASL_SSL  | 12p 40Kx10KB | 287.59 ms |  5.455 ms |   5.602 ms |                    - |                - | 49500.0000 |  3500.0000 | 794.41 MB |  
| NKafkaFetchBytesSeq1Part  | SASL_SSL  | 12p 40Kx10KB | 248.62 ms |  4.961 ms |  13.910 ms |           37060.0000 |                - |   500.0000 |         - |  10.92 MB |  
| NKafkaFetchBytesSeqNPart  | SASL_SSL  | 12p 40Kx10KB | 246.39 ms |  4.878 ms |   5.421 ms |           37370.0000 |                - |   500.0000 |         - |  10.38 MB |  
| NKafkaFetchBytesParNPart  | SASL_SSL  | 12p 40Kx10KB | 107.02 ms |  2.119 ms |   2.679 ms |           34895.4000 |                - |   600.0000 |  200.0000 |  11.91 MB |  
| NKafkaFetchStringParNPart | SASL_SSL  | 12p 40Kx10KB | 144.06 ms |  2.857 ms |   3.910 ms |           32479.3333 |                - | 55000.0000 |  7333.3333 | 794.09 MB |  
| NKafkaConsumeString       | SASL_SSL  | 12p 40Kx10KB | 419.92 ms | 58.034 ms | 171.115 ms |           28321.5000 |                - | 50000.0000 |   500.0000 | 794.28 MB |  
| NKafkaConsumeBytes        | SASL_SSL  | 12p 40Kx10KB | 134.17 ms |  2.676 ms |   2.748 ms |           32767.6667 |                - |   666.6667 |         - |  12.14 MB |  
| NKafkaBatchConsumeBytes   | SASL_SSL  | 12p 40Kx10KB | 135.82 ms |  2.674 ms |   4.002 ms |           33556.5000 |                - |   750.0000 |  250.0000 |   12.1 MB |  
                                                                                                           
