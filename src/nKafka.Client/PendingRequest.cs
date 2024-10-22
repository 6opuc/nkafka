using nKafka.Contracts;

namespace nKafka.Client;

public class PendingRequest
{
    public IRequest Request { get; init; }
    public TaskCompletionSource<object> Response { get; init; }
    public CancellationToken CancellationToken { get; init; }
        

    public PendingRequest(
        IRequest request, 
        TaskCompletionSource<object> response,
        CancellationToken cancellationToken)
    {
        Request = request;
        Response = response;
        CancellationToken = cancellationToken;
    }
}