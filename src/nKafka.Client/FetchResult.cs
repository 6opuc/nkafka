using nKafka.Contracts.MessageDefinitions;

namespace nKafka.Client;

public sealed class FetchResult : IDisposable
{
    public IDisposableMessage<FetchResponse>? Response { get; }
    public Exception? Exception { get; }
    public int GenerationId { get; }
    public bool IsSuccess => Exception == null;

    public FetchResult(IDisposableMessage<FetchResponse> response, int generationId)
    {
        Response = response;
        GenerationId = generationId;
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
