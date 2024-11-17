namespace nKafka.Client;

public class ConsumerConfig
{
    public string BootstrapServers { get; }
    public string Topics { get; }
    public string GroupId { get; }
    public string Protocol { get; }
    public int ResponseBufferSize { get; }
    public int RequestBufferSize { get; }
    public int SessionTimeoutMs { get; }
    public int HeartbeatIntervalMs { get; }

    public ConsumerConfig(
        string bootstrapServers,
        string topics,
        string groupId,
        string protocol,
        int responseBufferSize = 512 * 1024,
        int requestBufferSize = 512 * 1024,
        int sessionTimeoutMs = 45_000,
        int? heartbeatIntervalMs = null)
    {
        BootstrapServers = bootstrapServers;
        Topics = topics;
        GroupId = groupId;
        Protocol = protocol;
        RequestBufferSize = requestBufferSize;
        ResponseBufferSize = responseBufferSize;
        SessionTimeoutMs = sessionTimeoutMs;
        HeartbeatIntervalMs = heartbeatIntervalMs ?? (sessionTimeoutMs / 3);
    }
}