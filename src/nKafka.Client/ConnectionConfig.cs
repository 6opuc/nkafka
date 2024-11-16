namespace nKafka.Client;

public class ConnectionConfig
{
    public string Host { get; }
    public int Port { get; }
    public int BufferSize { get; }

    public ConnectionConfig(string host, int port, int bufferSize = 512 * 1024)
    {
        Host = host;
        Port = port;
        BufferSize = bufferSize;
    }
}