# nKafka

Yet another [more] efficient Kafka client implementation for .net ;)

The work is still ongoing, but there are already promising results comparing to the official client:
- **32x** less memory allocations
- **10x** faster in PLAINTEXT mode
- **2x** faster in SASL_SSL mode
- it scales much better with the number of consumers

## Benchmarks

### Scenarios

All benchmarks run against topics with 12 partitions and replication factor 2:

| Scenario | Messages | Size/msg | Total |
|----------|----------|----------|-------|
| 12p 1Mx4B | 1,000,000 | 4B | 4MB |
| 12p 100Kx4KB | 100,000 | 4KB | 400MB |
| 12p 10Kx40KB | 10,000 | 40KB | 400MB |
| 12p 4Kx100KB | 4,000 | 100KB | 400MB |
| **12p 40Kx10KB** | 40,000 | 10KB | 400MB |
| 12p 1Kx400KB | 1,000 | 400KB | 400MB |

### Results (12p 40Kx10KB)

Ubuntu 26.04 LTS, AMD RYZEN AI MAX+ 395 w/ Radeon 8060S 2.00GHz, .NET 10.0.8, .NET SDK 10.0.300

#### PLAINTEXT

| Method                    | Mean      | Gen0       | Gen1       | Allocated |
|-------------------------- |----------:|-----------:|-----------:|----------:|
| ConfluentConsumeBytes     | 243.68 ms | 25000.0000 |          - | 403.77 MB |
| ConfluentConsumeString    | 253.58 ms | 49500.0000 |  3500.0000 | 794.41 MB |
| NKafkaFetchBytesSeq1Part  |  53.92 ms |   666.6667 |          - |  10.87 MB |
| NKafkaFetchBytesSeqNPart  |  41.21 ms |   583.3333 |    83.3333 |  10.31 MB |
| NKafkaFetchBytesParNPart  |  19.06 ms |   718.7500 |   343.7500 |  11.84 MB |
| NKafkaFetchStringParNPart |  65.18 ms | 61285.7143 |   142.8571 | 794.02 MB |
| NKafkaConsumeString       |  76.15 ms | 50285.7143 |   142.8571 | 794.18 MB |
| NKafkaConsumeBytes        |  23.37 ms |   750.0000 |   343.7500 |  12.04 MB |
| NKafkaBatchConsumeBytes   |  23.96 ms |   733.3333 |   333.3333 |  12.01 MB |

#### SASL_SSL (SCRAM-SHA-512)

| Method                    | Mean      | Gen0       | Gen1       | Allocated |
|-------------------------- |----------:|-----------:|-----------:|----------:|
| ConfluentConsumeBytes     | 287.01 ms | 25000.0000 |          - | 403.78 MB |
| ConfluentConsumeString    | 292.57 ms | 49500.0000 |  3500.0000 | 794.41 MB |
| NKafkaFetchBytesSeq1Part  | 237.02 ms |   500.0000 |          - |  10.92 MB |
| NKafkaFetchBytesSeqNPart  | 240.04 ms |   500.0000 |          - |  10.38 MB |
| NKafkaFetchBytesParNPart  | 106.74 ms |   500.0000 |          - |  11.91 MB |
| NKafkaFetchStringParNPart | 144.13 ms | 56000.0000 |   500.0000 | 794.09 MB |
| NKafkaConsumeString       | 391.96 ms | 50000.0000 |  1000.0000 | 794.27 MB |
| NKafkaConsumeBytes        | 133.76 ms |   750.0000 |   250.0000 |  13.14 MB |
| NKafkaBatchConsumeBytes   | 134.59 ms |   750.0000 |   250.0000 |  12.10 MB |

See [benchmark.md](benchmark.md) for full results across multiple machines.

## Current status
> [!CAUTION]
> Not ready for production.

- [x] You can use `FetchRequest` for efficient/fast queries to kafka topics(see `NKafkaFetchBytesSeqSinglePartTest`).
- [ ] Consumer implementation is still in progress
- [ ] Producer implementation is not started yet
- [ ] Compression is not implemented yet
- [ ] Metrics/Telemetry is not implemented yet

## Setting up the environment

Requires either [Docker](https://docker.com) or [Podman](https://podman.io). The scripts auto-detect which tool is installed.

Override detection via `CONTAINER_TOOL` env var:

```bash
export CONTAINER_TOOL=docker   # or podman
```

```bash
# 1. Generate self-signed TLS certificates (JKS keystores for each broker)
./infra/gen-certs.sh

# 2. Start the Kafka cluster (3 nodes + Kafka UI on port 8081)
#    (auto-detects docker compose, podman compose, or podman-compose)
${CONTAINER_TOOL:-docker} compose -f infra/docker-compose.yaml up -d

# 3. Create SCRAM users (admin / admin-secret)
./infra/init-cluster.sh

# 4. Create benchmark topics and fill them with test data
./infra/init-benchmark-topics.sh
```

Broker ports (SASL_SSL / PLAINTEXT):
| Broker | SASL_SSL | PLAINTEXT |
|--------|----------|-----------|
| kafka-1 | `localhost:9192` | `localhost:9193` |
| kafka-2 | `localhost:9292` | `localhost:9293` |
| kafka-3 | `localhost:9392` | `localhost:9393` |

## Benchmark topics

The `init-benchmark-topics.sh` script creates 6 topics (12 partitions, RF=2) used by the benchmarks:

| Topic | Messages | Size/msg | Total |
|-------|----------|----------|-------|
| `test_p12_m1M_s4B` | 1,000,000 | 4B | 4MB |
| `test_p12_m100K_s4KB` | 100,000 | 4KB | 400MB |
| `test_p12_m10K_s40KB` | 10,000 | 40KB | 400MB |
| `test_p12_m4K_s100KB` | 4,000 | 100KB | 400MB |
| `test_p12_m40K_s10KB` | 40,000 | 10KB | 400MB |
| `test_p12_m1K_s400KB` | 1,000 | 400KB | 400MB |

## Links
- The idea of code generation from kafka message definitions is taken from this interesting project: https://github.com/Fresa/Kafka.Protocol


