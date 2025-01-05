using nKafka.Contracts;

namespace nKafka.Client;

public interface IConnection : IAsyncDisposable
{
    ValueTask OpenAsync(
        CancellationToken cancellationToken);
    
    ValueTask<IDisposableMessage<TResponse>> SendAsync<TResponse>(
        RequestClient<TResponse> requestClient,
        CancellationToken cancellationToken);
}