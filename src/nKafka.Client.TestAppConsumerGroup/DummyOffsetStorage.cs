namespace nKafka.Client.TestAppConsumerGroup;

public class DummyOffsetStorage : IOffsetStorage
{
    public ValueTask<long> GetOffset(
        IConnection connection,
        string consumerGroup,
        string topic,
        int partition,
        CancellationToken cancellationToken)
    {
        return ValueTask.FromResult(0L);
    }
}