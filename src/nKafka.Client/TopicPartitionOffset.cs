namespace nKafka.Client;

public struct TopicPartitionOffset
{
    public string Topic { get; }
    public int Partition { get; }
    public long Offset { get; }
    

    public TopicPartitionOffset(string topic, int partition, long offset)
    {
        Topic = topic;
        Partition = partition;
        Offset = offset;
    }
}