BenchmarkDotNet v0.15.8, Linux Ubuntu 26.04 LTS (Resolute Raccoon)
AMD RYZEN AI MAX+ 395 w/ Radeon 8060S 1.63GHz, 1 CPU, 32 logical and 16 physical cores
.NET SDK 10.0.300
  [Host]     : .NET 10.0.8 (10.0.8, 10.0.826.23019), X64 RyuJIT x86-64-v4
  DefaultJob : .NET 10.0.8 (10.0.8, 10.0.826.23019), X64 RyuJIT x86-64-v4


| Method                    | Protocol  | Scenario     | Mean      | Error    | StdDev    | Completed Work Items | Lock Contentions | Gen0       | Gen1       | Gen2     | Allocated |
|-------------------------- |---------- |------------- |----------:|---------:|----------:|---------------------:|-----------------:|-----------:|-----------:|---------:|----------:|
| ConfluentConsumeBytes     | PLAINTEXT | 12p 40Kx10KB | 302.93 ms | 5.971 ms | 10.300 ms |                    - |                - | 25000.0000 |          - |        - | 403.78 MB |
| ConfluentConsumeString    | PLAINTEXT | 12p 40Kx10KB | 297.73 ms | 5.431 ms |  6.670 ms |                    - |                - | 49500.0000 |  3500.0000 |        - | 794.41 MB |
| NKafkaFetchBytesSeq1Part  | PLAINTEXT | 12p 40Kx10KB |  79.97 ms | 1.589 ms |  2.279 ms |            5517.0000 |                - |   600.0000 |          - |        - |  11.74 MB |
| NKafkaFetchBytesSeqNPart  | PLAINTEXT | 12p 40Kx10KB |  56.54 ms | 1.082 ms |  1.012 ms |            3539.1111 |                - |   666.6667 |   111.1111 |        - |  10.64 MB |
| NKafkaFetchBytesParNPart  | PLAINTEXT | 12p 40Kx10KB |  24.64 ms | 0.420 ms |  0.351 ms |            3638.0625 |                - |   750.0000 |   250.0000 |        - |  12.17 MB |
| NKafkaFetchStringParNPart | PLAINTEXT | 12p 40Kx10KB |  57.80 ms | 1.152 ms |  1.793 ms |            2786.9000 |                - | 57000.0000 | 20200.0000 | 100.0000 | 794.35 MB |
| NKafkaConsumeString       | PLAINTEXT | 12p 40Kx10KB |  69.31 ms | 1.380 ms |  1.842 ms |            3294.7143 |                - | 50285.7143 |   142.8571 |        - | 794.47 MB |
| NKafkaConsumeBytes        | PLAINTEXT | 12p 40Kx10KB |  28.18 ms | 0.399 ms |  0.353 ms |            3814.1333 |                - |   733.3333 |   333.3333 |        - |  12.32 MB |
| NKafkaBatchConsumeBytes   | PLAINTEXT | 12p 40Kx10KB |  28.66 ms | 0.560 ms |  0.786 ms |            3691.3438 |                - |   750.0000 |   343.7500 |        - |  12.32 MB |
| ConfluentConsumeBytes     | SASL_SSL  | 12p 40Kx10KB | 295.05 ms | 5.702 ms |  8.178 ms |                    - |                - | 25000.0000 |          - |        - | 403.78 MB |
| ConfluentConsumeString    | SASL_SSL  | 12p 40Kx10KB | 295.43 ms | 5.863 ms |  5.758 ms |                    - |                - | 49000.0000 |  3000.0000 |        - | 794.41 MB |
| NKafkaFetchBytesSeq1Part  | SASL_SSL  | 12p 40Kx10KB | 285.84 ms | 5.720 ms | 16.684 ms |           37153.0000 |                - |          - |          - |        - |  11.81 MB |
| NKafkaFetchBytesSeqNPart  | SASL_SSL  | 12p 40Kx10KB | 281.06 ms | 5.289 ms |  5.879 ms |           38649.0000 |                - |   500.0000 |          - |        - |  10.71 MB |
| NKafkaFetchBytesParNPart  | SASL_SSL  | 12p 40Kx10KB | 114.48 ms | 2.139 ms |  1.896 ms |           33407.7500 |                - |   750.0000 |          - |        - |  13.24 MB |
| NKafkaFetchStringParNPart | SASL_SSL  | 12p 40Kx10KB | 148.18 ms | 2.842 ms |  3.696 ms |           31908.3333 |                - | 52666.6667 | 13000.0000 |        - | 794.75 MB |
| NKafkaConsumeString       | SASL_SSL  | 12p 40Kx10KB | 153.61 ms | 3.043 ms |  5.641 ms |           27813.0000 |                - | 50000.0000 | 16000.0000 |        - | 794.57 MB |
| NKafkaConsumeBytes        | SASL_SSL  | 12p 40Kx10KB | 144.00 ms | 2.739 ms |  2.813 ms |           35844.2500 |                - |   750.0000 |   250.0000 |        - |  12.41 MB |
| NKafkaBatchConsumeBytes   | SASL_SSL  | 12p 40Kx10KB | 141.94 ms | 2.802 ms |  2.483 ms |           33445.7500 |                - |   750.0000 |   250.0000 |        - |  12.41 MB |


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
AMD Ryzen 5 7530U with Radeon Graphics 2.90GHz, 1 CPU, 12 logical and 6 physical cores
.NET SDK 10.0.203
  [Host]     : .NET 10.0.7 (10.0.7, 10.0.726.21808), X64 RyuJIT x86-64-v3
  DefaultJob : .NET 10.0.7 (10.0.7, 10.0.726.21808), X64 RyuJIT x86-64-v3


| Method                    | Protocol  | Scenario     | Mean       | Error    | StdDev   | Gen0       | Completed Work Items | Lock Contentions | Gen1       | Allocated |
|-------------------------- |---------- |------------- |-----------:|---------:|---------:|-----------:|---------------------:|-----------------:|-----------:|----------:|
| ConfluentConsumeBytes     | PLAINTEXT | 12p 40Kx10KB |   751.7 ms | 26.69 ms | 77.44 ms | 50000.0000 |                    - |                - |          - | 403.79 MB |
| ConfluentConsumeString    | PLAINTEXT | 12p 40Kx10KB |   756.8 ms | 18.91 ms | 55.45 ms | 99000.0000 |                    - |                - |  7000.0000 | 794.42 MB |
| NKafkaFetchBytesSeq1Part  | PLAINTEXT | 12p 40Kx10KB |   657.7 ms | 11.32 ms | 10.03 ms |          - |            8128.0000 |                - |          - |   5.58 MB |
| NKafkaFetchBytesSeqNPart  | PLAINTEXT | 12p 40Kx10KB |   663.9 ms | 13.22 ms | 15.22 ms |          - |            7731.0000 |                - |          - |   6.06 MB |
| NKafkaFetchBytesParNPart  | PLAINTEXT | 12p 40Kx10KB |   165.3 ms |  3.24 ms |  5.24 ms |   500.0000 |            6176.0000 |                - |          - |   5.99 MB |
| NKafkaFetchStringParNPart | PLAINTEXT | 12p 40Kx10KB |   250.5 ms |  4.05 ms |  3.38 ms | 99000.0000 |            5541.5000 |                - |  8500.0000 | 788.18 MB |
| NKafkaConsumeString       | PLAINTEXT | 12p 40Kx10KB |   460.4 ms | 21.84 ms | 63.00 ms | 99000.0000 |           28353.5000 |                - | 18500.0000 | 788.77 MB |
| NKafkaConsumeBytes        | PLAINTEXT | 12p 40Kx10KB |   457.5 ms | 31.32 ms | 91.85 ms |          - |            7353.0000 |                - |          - |   6.66 MB |
| NKafkaBatchConsumeBytes   | PLAINTEXT | 12p 40Kx10KB |   430.5 ms | 33.88 ms | 98.83 ms |          - |           46059.0000 |                - |          - |   6.65 MB |
| ConfluentConsumeBytes     | SASL_SSL  | 12p 40Kx10KB |   749.1 ms | 14.81 ms | 36.61 ms | 50000.0000 |                    - |                - |          - | 403.79 MB |
| ConfluentConsumeString    | SASL_SSL  | 12p 40Kx10KB |   753.9 ms | 15.03 ms | 17.31 ms | 99000.0000 |                    - |                - |  7000.0000 | 794.42 MB |
| NKafkaFetchBytesSeq1Part  | SASL_SSL  | 12p 40Kx10KB | 1,175.6 ms | 22.32 ms | 21.92 ms |          - |           45494.0000 |                - |          - |   5.65 MB |
| NKafkaFetchBytesSeqNPart  | SASL_SSL  | 12p 40Kx10KB | 1,208.6 ms | 20.30 ms | 15.85 ms |          - |           47744.0000 |                - |          - |  10.13 MB |
| NKafkaFetchBytesParNPart  | SASL_SSL  | 12p 40Kx10KB |   675.5 ms | 13.43 ms | 19.69 ms |          - |           28863.0000 |                - |          - |   6.07 MB |
| NKafkaFetchStringParNPart | SASL_SSL  | 12p 40Kx10KB |   736.6 ms |  5.72 ms |  4.78 ms | 99000.0000 |           26387.0000 |           1.0000 |  8000.0000 | 788.25 MB |
| NKafkaConsumeString       | SASL_SSL  | 12p 40Kx10KB |   767.0 ms | 14.96 ms | 26.20 ms | 99000.0000 |           28480.0000 |                - | 19000.0000 | 788.89 MB |
| NKafkaConsumeBytes        | SASL_SSL  | 12p 40Kx10KB |   742.8 ms | 14.85 ms | 36.43 ms |          - |           33202.0000 |                - |          - |   6.76 MB |
| NKafkaBatchConsumeBytes   | SASL_SSL  | 12p 40Kx10KB |   713.9 ms | 14.14 ms | 28.24 ms |          - |           23464.0000 |                - |          - |   6.74 MB |
