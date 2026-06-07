namespace nKafka.Client;

public sealed record ConsumerConfig(
    string BootstrapServers,
    string Topics,
    string ClientId,
    string GroupId,
    string InstanceId,
    string Protocol,
    int ResponseBufferSize = 512 * 1024,
    int RequestBufferSize = 512 * 1024,
    int SessionTimeoutMs = 45_000,
    int HeartbeatIntervalMs = 15_000,
    int MaxPollIntervalMs = 30_000,
    bool CheckCrcs = false,
    TimeSpan MaxWaitTime = default,
    int MaxFetchRetries = 3,
    TimeSpan FetchRetryBaseDelay = default,
    int FetchPartitionMaxBytes = 1 * 1024 * 1024,
    TlsConfig? Tls = null,
    SaslConfig? Sasl = null);
