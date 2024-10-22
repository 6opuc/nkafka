using nKafka.Contracts;

namespace nKafka.Client;

public class PendingRequest
{
    public IRequestClient RequestClient { get; init; }
    public TaskCompletionSource<object> Response { get; init; }
    public CancellationToken CancellationToken { get; init; }
        

    public PendingRequest(
        IRequestClient requestClient, 
        TaskCompletionSource<object> response,
        CancellationToken cancellationToken)
    {
        RequestClient = requestClient;
        Response = response;
        CancellationToken = cancellationToken;
    }
}