BenchmarkDotNet v0.15.8, Linux Ubuntu 26.04 LTS (Resolute Raccoon)                                                                                                                                         
AMD RYZEN AI MAX+ 395 w/ Radeon 8060S 1.68GHz, 1 CPU, 32 logical and 16 physical cores                                                                                                                     
.NET SDK 10.0.300                                                                                                                                                                                          
  [Host]     : .NET 10.0.8 (10.0.8, 10.0.826.23019), X64 RyuJIT x86-64-v4                                                                                                                                  
  ShortRun   : .NET 10.0.8 (10.0.8, 10.0.826.23019), X64 RyuJIT x86-64-v4                                                                                                                                
                                                                                                                                                                                                             
Job=ShortRun  IterationCount=3  LaunchCount=1   WarmupCount=3                                                                                                                                              
                                                                                                                                                                                                             
| Method                    | Protocol  | Scenario     | Mean      | Error        | StdDev     | Median    | Completed Work Items | Lock Contentions | Gen0       | Gen1      | Gen2     | Allocated |
|-------------------------- |---------- |------------- |----------:|-------------:|-----------:|----------:|---------------------:|-----------------:|-----------:|----------:|---------:|----------:|
| ConfluentConsumeBytes     | PLAINTEXT | 12p 40Kx10KB | 249.04 ms |    17.911 ms |   0.982 ms | 249.19 ms |                    - |                - | 25000.0000 |         - |        - | 403.78 MB |
| ConfluentConsumeString    | PLAINTEXT | 12p 40Kx10KB | 254.27 ms |    56.192 ms |   3.080 ms | 255.08 ms |                    - |                - | 49500.0000 | 3500.0000 |        - | 794.41 MB |
| NKafkaFetchBytesSeq1Part  | PLAINTEXT | 12p 40Kx10KB |  54.73 ms |    16.484 ms |   0.904 ms |  54.94 ms |            5485.4444 |                - |   666.6667 |         - |        - |  10.87 MB |
| NKafkaFetchBytesSeqNPart  | PLAINTEXT | 12p 40Kx10KB |  43.21 ms |    23.566 ms |   1.292 ms |  42.99 ms |            4420.0000 |           0.0909 |   636.3636 |   90.9091 |        - |  10.31 MB |
| NKafkaFetchBytesParNPart  | PLAINTEXT | 12p 40Kx10KB |  20.64 ms |    17.661 ms |   0.968 ms |  20.12 ms |            3360.1563 |                - |   718.7500 |  343.7500 |        - |  11.84 MB |
| NKafkaFetchStringParNPart | PLAINTEXT | 12p 40Kx10KB |  67.05 ms |    56.024 ms |   3.071 ms |  66.28 ms |            2959.3750 |                - | 60750.0000 |  750.0000 | 125.0000 | 794.02 MB |
| NKafkaConsumeString       | PLAINTEXT | 12p 40Kx10KB |  73.07 ms |    72.707 ms |   3.985 ms |  72.60 ms |            2772.4286 |                - | 50285.7143 |  142.8571 |        - | 794.18 MB |
| NKafkaConsumeBytes        | PLAINTEXT | 12p 40Kx10KB |  23.34 ms |     1.810 ms |   0.099 ms |  23.37 ms |            3197.5938 |                - |   750.0000 |  343.7500 |        - |  12.05 MB |
| NKafkaBatchConsumeBytes   | PLAINTEXT | 12p 40Kx10KB |  24.84 ms |     2.892 ms |   0.159 ms |  24.92 ms |            3359.2308 |                - |   692.3077 |  307.6923 |        - |  12.16 MB |
| ConfluentConsumeBytes     | SASL_SSL  | 12p 40Kx10KB | 283.01 ms |    57.900 ms |   3.174 ms | 282.35 ms |                    - |                - | 25000.0000 |         - |        - | 403.78 MB |
| ConfluentConsumeString    | SASL_SSL  | 12p 40Kx10KB | 296.28 ms |    46.003 ms |   2.522 ms | 294.84 ms |                    - |                - | 49500.0000 | 3500.0000 |        - | 794.41 MB |
| NKafkaFetchBytesSeq1Part  | SASL_SSL  | 12p 40Kx10KB | 270.22 ms |   309.213 ms |  16.949 ms | 269.46 ms |           36653.5000 |                - |   500.0000 |         - |        - |  10.92 MB |
| NKafkaFetchBytesSeqNPart  | SASL_SSL  | 12p 40Kx10KB | 264.39 ms |   223.605 ms |  12.257 ms | 268.09 ms |           37552.0000 |                - |   500.0000 |         - |        - |   10.4 MB |
| NKafkaFetchBytesParNPart  | SASL_SSL  | 12p 40Kx10KB | 110.25 ms |    89.522 ms |   4.907 ms | 107.74 ms |           34053.2000 |           0.2000 |   600.0000 |  200.0000 |        - |  14.03 MB |
| NKafkaFetchStringParNPart | SASL_SSL  | 12p 40Kx10KB | 146.07 ms |    22.787 ms |   1.249 ms | 145.94 ms |           28152.0000 |                - | 54500.0000 | 7000.0000 |        - | 794.09 MB |
| NKafkaConsumeString       | SASL_SSL  | 12p 40Kx10KB | 490.65 ms | 4,450.891 ms | 243.968 ms | 353.55 ms |           24014.0000 |                - | 50000.0000 | 1000.0000 |        - | 794.28 MB |
| NKafkaConsumeBytes        | SASL_SSL  | 12p 40Kx10KB | 152.61 ms |   609.342 ms |  33.400 ms | 135.95 ms |           32653.5000 |                - |   750.0000 |  250.0000 |        - |  14.14 MB |
| NKafkaBatchConsumeBytes   | SASL_SSL  | 12p 40Kx10KB | 136.66 ms |    19.026 ms |   1.043 ms | 137.01 ms |           34903.0000 |                - |   750.0000 |  250.0000 |        - |   12.1 MB |

