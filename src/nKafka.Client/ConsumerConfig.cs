namespace nKafka.Client;

public sealed record ConsumerConfig(
    string BootstrapServers,
    string Topics,
    string ClientId,
    string GroupId,
    string InstanceId,
    string Protocol,
    int ResponseBufferSize,
    int RequestBufferSize,
    TimeSpan SessionTimeout,
    TimeSpan HeartbeatInterval,
    TimeSpan MaxPollInterval,
    bool CheckCrcs,
    TimeSpan MaxWaitTime,
    TimeSpan FetchTimeout,
    int MaxFetchRetries,
    TimeSpan FetchRetryBaseDelay,
    int FetchPartitionMaxBytes,
    TlsConfig? Tls,
    SaslConfig? Sasl)
{
    public ConsumerConfig(string bootstrapServers, string topics, string clientId, string groupId, string instanceId, string protocol)
        : this(bootstrapServers, topics, clientId, groupId, instanceId, protocol,
              512 * 1024, 512 * 1024,
              TimeSpan.FromSeconds(45), TimeSpan.FromSeconds(15), TimeSpan.FromSeconds(30),
              false, default, default, 3, default, 1024 * 1024, null, null)
    {
    }
}
