namespace nKafka.Client;

public sealed class ConsumerBatch<TMessage> : IConsumerBatch<TMessage>
{
    private readonly ConsumePipeline<TMessage> _pipeline;
    private IEnumerator<ConsumeResult<TMessage>>? _enumerator;
    private bool _disposed;

    public ConsumerBatch(ConsumePipeline<TMessage> pipeline)
    {
        _pipeline = pipeline;
    }

    public IEnumerator<ConsumeResult<TMessage>> GetEnumerator()
    {
        _enumerator = GetEnumeratorCore();
        return _enumerator;
    }

    private IEnumerator<ConsumeResult<TMessage>> GetEnumeratorCore()
    {
        try
        {
            while (true)
            {
                var result = _pipeline.ConsumeFromCurrentEnumerator();
                if (result == null)
                {
                    break;
                }
                yield return result.Value;
            }
        }
        finally
        {
            Dispose();
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _enumerator?.Dispose();
        _enumerator = null;
        _pipeline.DisposeFetchResult();
    }

    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
}
