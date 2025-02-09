namespace nKafka.Client;

public struct MessageDeserializationContext
{
    public string Topic;
    public int Partition;
    public long Offset;
    public DateTime Timestamp;
    public Memory<byte>? Key;
    public Memory<byte>? Value;
    public IReadOnlyDictionary<string, Memory<byte>?>? Headers;
}