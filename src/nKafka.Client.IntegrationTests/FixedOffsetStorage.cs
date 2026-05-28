namespace nKafka.Client.IntegrationTests;

/// <summary>
/// Offset storage that returns a fixed offset for all partitions.
/// Useful for testing consumer behavior at specific offsets.
/// </summary>
public class FixedOffsetStorage(long fixedOffset) : IOffsetStorage
{
    public ValueTask<long> GetAsync(
        IConnection connection,
        string consumerGroup,
        string topic,
        int partition,
        CancellationToken cancellationToken)
    {
        return ValueTask.FromResult(fixedOffset);
    }

    public ValueTask SetAsync(
        IConnection connection,
        string consumerGroup,
        string topic,
        int partition,
        long offset,
        CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }
}
