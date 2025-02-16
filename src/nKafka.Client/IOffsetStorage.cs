namespace nKafka.Client;

public interface IOffsetStorage
{
    ValueTask<long> GetOffset(
        IConnection connection,
        string consumerGroup,
        string topic,
        int partition,
        CancellationToken cancellationToken);
}