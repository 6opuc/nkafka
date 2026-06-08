namespace nKafka.Client;

internal sealed class ConsumerBatch<TMessage>(Consumer<TMessage> consumer) : IConsumerBatch<TMessage>
{
    private readonly Consumer<TMessage> _consumer = consumer;
    private IEnumerator<ConsumeResult<TMessage>>? _enumerator;
    private bool _disposed;

    public IEnumerator<ConsumeResult<TMessage>> GetEnumerator()
    {
        _enumerator = GetEnumeratorCore();
        return _enumerator;
    }

    private IEnumerator<ConsumeResult<TMessage>> GetEnumeratorCore()
    {
        try
        {
            while (_consumer._messageDeserializeEnumerator?.MoveNext() == true)
            {
                var deserializationContext = _consumer._messageDeserializeEnumerator.Current;
                var message = _consumer._deserializer.Deserialize(deserializationContext);
                yield return new ConsumeResult<TMessage>
                {
                    Topic = deserializationContext.Topic,
                    Partition = deserializationContext.Partition,
                    Offset = deserializationContext.Offset,
                    Timestamp = deserializationContext.Timestamp,
                    Message = message,
                };
            }
        }
        finally
        {
            Dispose();
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _enumerator?.Dispose();
        _enumerator = null;
        _consumer._messageDeserializeEnumerator?.Dispose();
        _consumer._messageDeserializeEnumerator = null;
        _consumer._fetchResult?.Dispose();
        _consumer._fetchResult = null;
    }

    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
}
