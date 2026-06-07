using nKafka.Contracts;

namespace nKafka.Client;

public interface IConnection : IAsyncDisposable
{
    ValueTask OpenAsync(CancellationToken cancellationToken);

    ValueTask<IDisposableMessage<TResponse>> SendAsync<TResponse>(
        IRequest<TResponse> request,
        CancellationToken cancellationToken);

    bool SupportsApiKeyVersion(ApiKey apiKey, short minVersion);
}
