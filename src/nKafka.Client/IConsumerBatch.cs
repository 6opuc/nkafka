namespace nKafka.Client;

public interface IConsumerBatch<TMessage> : IEnumerable<ConsumeResult<TMessage>>, IDisposable
{
}
