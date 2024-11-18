namespace nKafka.Client;

public struct MessageDeserializationContext
{
    public string Topic { get; set; }
    public int Partition { get; set; }
    public long Offset { get; set; }
    public DateTime Timestamp { get; set; }
    public Memory<byte>? Key { get; set; }
    public Memory<byte>? Value { get; set; }
    public IReadOnlyDictionary<string, Memory<byte>?>? Headers { get; set; }
}