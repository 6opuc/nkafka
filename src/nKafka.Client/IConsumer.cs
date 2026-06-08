namespace nKafka.Client;

public interface IConsumer<TMessage> : IAsyncDisposable
{
    ValueTask JoinGroupAsync(CancellationToken cancellationToken);

    ValueTask<ConsumeResult<TMessage>?> ConsumeAsync(CancellationToken cancellationToken);

    ValueTask<IConsumerBatch<TMessage>> ConsumeBatchAsync(CancellationToken cancellationToken);

    ValueTask CommitAsync(ConsumeResult<TMessage> consumeResult, CancellationToken cancellationToken);
}
