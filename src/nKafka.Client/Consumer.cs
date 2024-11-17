namespace nKafka.Client;

public class Consumer<TMessage> : IConsumer<TMessage>
{

    public ValueTask JoinGroupAsync(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public ValueTask<ConsumeResult<TMessage>> ConsumeAsync(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
    
    public ValueTask DisposeAsync()
    {
        throw new NotImplementedException();
    }
}