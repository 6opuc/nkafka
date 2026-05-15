using nKafka.Contracts.MessageDefinitions;

namespace nKafka.Client;

public sealed class FetchResult : IDisposable
{
    public IDisposableMessage<FetchResponse>? Response { get; }
    public Exception? Exception { get; }
    public bool IsSuccess => Exception == null;

    public FetchResult(IDisposableMessage<FetchResponse> response)
    {
        Response = response;
    }

    public FetchResult(Exception exception)
    {
        Exception = exception;
    }

    public void Dispose()
    {
        Response?.Dispose();
    }
}
