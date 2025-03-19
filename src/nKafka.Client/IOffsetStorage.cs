namespace nKafka.Client;

public interface IOffsetStorage
{
    ValueTask<long> GetAsync(
        IConnection connection,
        string consumerGroup,
        string topic,
        int partition,
        CancellationToken cancellationToken);
    
    ValueTask SetAsync(
        IConnection connection,
        string consumerGroup,
        string topic,
        int partition,
        long offset,
        CancellationToken cancellationToken);
}