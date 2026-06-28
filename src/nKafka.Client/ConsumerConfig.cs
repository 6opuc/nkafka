namespace nKafka.Client;

public sealed record ConsumerConfig(
    string BootstrapServers,
    string Topics,
    string ClientId,
    string GroupId,
    string InstanceId,
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
    public ConsumerConfig(
        string bootstrapServers,
        string topics,
        string clientId,
        string groupId,
        string instanceId)
        : this(
              BootstrapServers: bootstrapServers,
              Topics: topics,
              ClientId: clientId,
              GroupId: groupId,
              InstanceId: instanceId,
              ResponseBufferSize: 512 * 1024,
              RequestBufferSize: 512 * 1024,
              SessionTimeout: TimeSpan.FromSeconds(45),
              HeartbeatInterval: TimeSpan.FromSeconds(15),
              MaxPollInterval: TimeSpan.FromSeconds(30),
              CheckCrcs: false,
              MaxWaitTime: TimeSpan.FromMilliseconds(500),
              FetchTimeout: TimeSpan.FromSeconds(1),
              MaxFetchRetries: 3,
              FetchRetryBaseDelay: TimeSpan.FromMilliseconds(100),
              FetchPartitionMaxBytes: 1024 * 1024,
              Tls: null,
              Sasl: null)
    {
    }
}
