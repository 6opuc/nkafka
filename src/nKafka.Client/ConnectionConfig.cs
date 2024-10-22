namespace nKafka.Client;

public class ConnectionConfig
{
    public string Host { get; }
    public int Port { get; }

    public ConnectionConfig(string host, int port)
    {
        Host = host;
        Port = port;
    }
}