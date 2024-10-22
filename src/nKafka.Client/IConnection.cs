using nKafka.Contracts;

namespace nKafka.Client;

public interface IConnection : IAsyncDisposable
{
    ValueTask OpenAsync(ConnectionConfig connectionConfig, CancellationToken cancellationToken);
    
    ValueTask<TResponse> SendAsync<TResponse>(Request<TResponse> request, CancellationToken cancellationToken);
}