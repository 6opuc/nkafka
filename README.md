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

Ubuntu 26.04 LTS, AMD RYZEN AI MAX+ 395 w/ Radeon 8060S 1.68GHz, .NET 10.0.8, .NET SDK 10.0.300

#### PLAINTEXT

| Method                    | Mean      | Gen0       | Gen1       | Allocated |
|-------------------------- |----------:|-----------:|-----------:|----------:|
| ConfluentConsumeBytes     | 249.04 ms | 25000.0000 |          - | 403.78 MB |
| ConfluentConsumeString    | 254.27 ms | 49500.0000 | 3500.0000  | 794.41 MB |
| NKafkaFetchBytesSeq1Part  |  54.73 ms |   666.6667 |          - |  10.87 MB |
| NKafkaFetchBytesSeqNPart  |  43.21 ms |   636.3636 |   90.9091  |  10.31 MB |
| NKafkaFetchBytesParNPart  |  20.64 ms |   718.7500 |  343.7500  |  11.84 MB |
| NKafkaFetchStringParNPart |  67.05 ms | 60750.0000 |  750.0000  | 794.02 MB |
| NKafkaConsumeString       |  73.07 ms | 50285.7143 |  142.8571  | 794.18 MB |
| NKafkaConsumeBytes        |  23.34 ms |   750.0000 |  343.7500  |  12.05 MB |
| NKafkaBatchConsumeBytes   |  24.84 ms |   692.3077 |  307.6923  |  12.16 MB |

#### SASL_SSL (SCRAM-SHA-512)

| Method                    | Mean      | Gen0       | Gen1       | Allocated |
|-------------------------- |----------:|-----------:|-----------:|----------:|
| ConfluentConsumeBytes     | 283.01 ms | 25000.0000 |          - | 403.78 MB |
| ConfluentConsumeString    | 296.28 ms | 49500.0000 | 3500.0000  | 794.41 MB |
| NKafkaFetchBytesSeq1Part  | 270.22 ms |   500.0000 |          - |  10.92 MB |
| NKafkaFetchBytesSeqNPart  | 264.39 ms |   500.0000 |          - |   10.4 MB |
| NKafkaFetchBytesParNPart  | 110.25 ms |   600.0000 |  200.0000  |  14.03 MB |
| NKafkaFetchStringParNPart | 146.07 ms | 54500.0000 | 7000.0000  | 794.09 MB |
| NKafkaConsumeString       | 490.65 ms | 50000.0000 | 1000.0000  | 794.28 MB |
| NKafkaConsumeBytes        | 152.61 ms |   750.0000 |  250.0000  |  14.14 MB |
| NKafkaBatchConsumeBytes   | 136.66 ms |   750.0000 |  250.0000  |   12.1 MB |

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


