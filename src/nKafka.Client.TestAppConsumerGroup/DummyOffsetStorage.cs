namespace nKafka.Client.TestAppConsumerGroup;

public class DummyOffsetStorage : IOffsetStorage
{
    private readonly Dictionary<(string, string, int), long> _offsets = new Dictionary<(string, string, int), long>();
    
    public ValueTask<long> GetAsync(
        IConnection connection,
        string consumerGroup,
        string topic,
        int partition,
        CancellationToken cancellationToken)
    {
        return !_offsets.TryGetValue((consumerGroup, topic, partition), out var offset)
            ? ValueTask.FromResult<long>(0)
            : ValueTask.FromResult(offset);
    }

    public ValueTask SetAsync(
        IConnection connection,
        string consumerGroup,
        string topic,
        int partition,
        long offset,
        CancellationToken cancellationToken)
    {
        _offsets[(consumerGroup, topic, partition)] = offset;
        return ValueTask.CompletedTask;
    }
}