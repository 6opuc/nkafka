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
    TimeSpan SessionTimeout = default,
    TimeSpan HeartbeatInterval = default,
    TimeSpan MaxPollInterval = default,
    bool CheckCrcs = false,
    TimeSpan MaxWaitTime = default,
    TimeSpan FetchTimeout = default,
    int MaxFetchRetries = 3,
    TimeSpan FetchRetryBaseDelay = default,
    int FetchPartitionMaxBytes = 1 * 1024 * 1024,
    TlsConfig? Tls = null,
    SaslConfig? Sasl = null)
{
}
