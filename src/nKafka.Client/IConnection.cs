namespace nKafka.Client;

public interface IConnection : IAsyncDisposable
{
    ValueTask OpenAsync(ConnectionConfig connectionConfig, CancellationToken cancellationToken);
}