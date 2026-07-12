namespace nKafka.Client;

public interface IConsumePipeline<TMessage> : IAsyncDisposable
{
    ValueTask<ConsumeResult<TMessage>?> ConsumeNextAsync(CancellationToken cancellationToken);
    IConsumerBatch<TMessage> CreateBatch(CancellationToken cancellationToken);
}
