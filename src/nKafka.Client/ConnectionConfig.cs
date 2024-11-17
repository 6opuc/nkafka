namespace nKafka.Client;

public class ConnectionConfig
{
    public string Host { get; }
    public int Port { get; }
    public int ResponseBufferSize { get; }
    public int RequestBufferSize { get; }

    public ConnectionConfig(
        string host,
        int port,
        int bufferSize = 512 * 1024,
        int requestBufferSize = 512 * 1024)
    {
        Host = host;
        Port = port;
        ResponseBufferSize = bufferSize;
        RequestBufferSize = requestBufferSize;
    }
}