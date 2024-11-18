namespace nKafka.Client;

public struct ConsumeResult<TMessage>
{
    public string Topic { get; set; }
    public int Partition { get; set; }
    public long Offset { get; set; }
    public DateTime Timestamp { get; set; }
    public TMessage? Message { get; set; }
}