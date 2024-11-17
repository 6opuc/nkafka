using nKafka.Contracts;

namespace nKafka.Client;

public interface IConnection : IAsyncDisposable
{
    ValueTask OpenAsync(
        CancellationToken cancellationToken);
    
    ValueTask<TResponse> SendAsync<TResponse>(
        RequestClient<TResponse> requestClient,
        CancellationToken cancellationToken);
}