BenchmarkDotNet v0.15.8, Linux Ubuntu 26.04 LTS (Resolute Raccoon)                                                                                                                                         
AMD RYZEN AI MAX+ 395 w/ Radeon 8060S 1.67GHz, 1 CPU, 32 logical and 16 physical cores                                                                                                                     
.NET SDK 10.0.300                                                                                                                                                                                          
  [Host]     : .NET 10.0.8 (10.0.8, 10.0.826.23019), X64 RyuJIT x86-64-v4                                                                                                                                  
  DefaultJob : .NET 10.0.8 (10.0.8, 10.0.826.23019), X64 RyuJIT x86-64-v4                                                                                                                                  
                                                                                                                                                                                                           
                                                                                                                                                                                                           
| Method                    | Protocol  | Scenario     | Mean        | Error      | StdDev     | Median      | Completed Work Items | Lock Contentions | Gen0       | Gen1       | Gen2     | Allocated |  
|-------------------------- |---------- |------------- |------------:|-----------:|-----------:|------------:|---------------------:|-----------------:|-----------:|-----------:|---------:|----------:|  
| ConfluentConsumeBytes     | PLAINTEXT | 12p 40Kx10KB |   242.39 ms |   2.172 ms |   1.925 ms |   242.36 ms |                    - |                - | 25000.0000 |          - |        - | 403.77 MB |  
| ConfluentConsumeString    | PLAINTEXT | 12p 40Kx10KB |   252.20 ms |   2.609 ms |   2.441 ms |   253.04 ms |                    - |                - | 49666.6667 |  3666.6667 |        - |  794.4 MB |  
| NKafkaFetchBytesSeq1Part  | PLAINTEXT | 12p 40Kx10KB |    78.14 ms |   1.523 ms |   1.813 ms |    78.09 ms |            5421.5000 |                - |   666.6667 |          - |        - |  11.74 MB |  
| NKafkaFetchBytesSeqNPart  | PLAINTEXT | 12p 40Kx10KB |    55.14 ms |   0.851 ms |   0.755 ms |    55.29 ms |            3655.4444 |                - |   666.6667 |   111.1111 |        - |  10.64 MB |  
| NKafkaFetchBytesParNPart  | PLAINTEXT | 12p 40Kx10KB |    24.51 ms |   0.213 ms |   0.189 ms |    24.47 ms |            3464.0000 |                - |   750.0000 |   218.7500 |        - |  12.17 MB |  
| NKafkaFetchStringParNPart | PLAINTEXT | 12p 40Kx10KB |    56.51 ms |   1.115 ms |   1.981 ms |    55.96 ms |            3092.1250 |                - | 58000.0000 | 20875.0000 | 125.0000 | 794.35 MB |  
| NKafkaConsumeString       | PLAINTEXT | 12p 40Kx10KB |    65.96 ms |   1.302 ms |   2.886 ms |    65.76 ms |            3106.4286 |                - | 50285.7143 |  3714.2857 |        - | 794.47 MB |  
| NKafkaConsumeBytes        | PLAINTEXT | 12p 40Kx10KB |    27.92 ms |   0.425 ms |   0.355 ms |    27.81 ms |            3713.6875 |                - |   750.0000 |   343.7500 |        - |  12.32 MB |  
| NKafkaBatchConsumeBytes   | PLAINTEXT | 12p 40Kx10KB |    28.41 ms |   0.562 ms |   0.710 ms |    28.14 ms |            3651.5938 |                - |   750.0000 |   281.2500 |        - |  12.32 MB |  
| ConfluentConsumeBytes     | SASL_SSL  | 12p 40Kx10KB |   287.66 ms |   5.686 ms |   7.972 ms |   286.22 ms |                    - |                - | 25000.0000 |          - |        - | 403.78 MB |  
| ConfluentConsumeString    | SASL_SSL  | 12p 40Kx10KB |   284.72 ms |   5.662 ms |   6.059 ms |   285.15 ms |                    - |                - | 49500.0000 |  3500.0000 |        - | 794.41 MB |  
| NKafkaFetchBytesSeq1Part  | SASL_SSL  | 12p 40Kx10KB |   278.49 ms |   5.318 ms |  10.118 ms |   277.43 ms |           37435.0000 |                - |          - |          - |        - |  11.81 MB |  
| NKafkaFetchBytesSeqNPart  | SASL_SSL  | 12p 40Kx10KB |   271.67 ms |   5.400 ms |   9.169 ms |   273.25 ms |           38533.5000 |                - |   500.0000 |          - |        - |  10.71 MB |  
| NKafkaFetchBytesParNPart  | SASL_SSL  | 12p 40Kx10KB |   115.82 ms |   2.205 ms |   2.062 ms |   116.12 ms |           33801.2500 |                - |   750.0000 |          - |        - |  12.24 MB |  
| NKafkaFetchStringParNPart | SASL_SSL  | 12p 40Kx10KB |   146.29 ms |   2.651 ms |   3.968 ms |   145.37 ms |           33484.5000 |                - | 53500.0000 | 15500.0000 |        - | 794.42 MB |  
| NKafkaConsumeString       | SASL_SSL  | 12p 40Kx10KB |   231.57 ms |  38.776 ms | 114.330 ms |   161.04 ms |           26751.0000 |                - | 50000.0000 | 16000.0000 |        - | 794.57 MB |  
| NKafkaConsumeBytes        | SASL_SSL  | 12p 40Kx10KB |   141.10 ms |   2.758 ms |   2.708 ms |   141.92 ms |           34643.2500 |                - |   750.0000 |   250.0000 |        - |  12.41 MB |  
| NKafkaBatchConsumeBytes   | SASL_SSL  | 12p 40Kx10KB |   141.25 ms |   2.672 ms |   2.744 ms |   141.48 ms |           34446.0000 |                - |   750.0000 |   250.0000 |        - |  12.41 MB |  
                                            
