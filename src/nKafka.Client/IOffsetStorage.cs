namespace nKafka.Client;

public interface IOffsetStorage
{
    ValueTask<long> GetOffset(string consumerGroup, string topic, int partition);
}