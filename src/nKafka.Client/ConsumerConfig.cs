namespace nKafka.Client;

public class ConsumerConfig
{
    public string BootstrapServers { get; }
    public string Topics { get; }
    public string ClientId { get; }
    public string GroupId { get; }
    public string InstanceId { get; }
    public string Protocol { get; }
    public int ResponseBufferSize { get; }
    public int RequestBufferSize { get; }
    public int SessionTimeoutMs { get; }
    public int HeartbeatIntervalMs { get; }
    
    public int MaxPollIntervalMs { get; }
    
    public bool CheckCrcs { get; set; } = false;
    public TimeSpan MaxWaitTime { get; set; } = TimeSpan.FromSeconds(5);
    

    public ConsumerConfig(
        string bootstrapServers,
        string topics,
        string clientId,
        string groupId,
        string instanceId,
        string protocol,
        int responseBufferSize = 512 * 1024,
        int requestBufferSize = 512 * 1024,
        int sessionTimeoutMs = 45_000,
        int heartbeatIntervalMs = 45_000 / 3,
        int maxPollIntervalMs = 30_000)
    {
        BootstrapServers = bootstrapServers;
        Topics = topics;
        ClientId = clientId;
        GroupId = groupId;
        InstanceId = instanceId;
        Protocol = protocol;
        RequestBufferSize = requestBufferSize;
        ResponseBufferSize = responseBufferSize;
        SessionTimeoutMs = sessionTimeoutMs;
        HeartbeatIntervalMs = heartbeatIntervalMs;
        MaxPollIntervalMs = maxPollIntervalMs;
    }
}