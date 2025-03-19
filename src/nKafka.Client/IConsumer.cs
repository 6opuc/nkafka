namespace nKafka.Client;

public interface IConsumer<TMessage> : IAsyncDisposable
{
    ValueTask JoinGroupAsync(CancellationToken cancellationToken);
    
    ValueTask<ConsumeResult<TMessage>?> ConsumeAsync(CancellationToken cancellationToken);
    
    ValueTask<IEnumerable<ConsumeResult<TMessage>>> ConsumeBatchAsync(CancellationToken cancellationToken);
}