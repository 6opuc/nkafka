namespace nKafka.Client;

public class ConsumeResult<TMessage>
{
    public IReadOnlyList<TMessage> Messages { get; }
    public TopicPartitionOffset LastOffset { get; }

    public ConsumeResult(IReadOnlyList<TMessage> messages, TopicPartitionOffset lastOffset)
    {
        Messages = messages;
        LastOffset = lastOffset;
    }
}