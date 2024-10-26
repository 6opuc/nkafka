namespace nKafka.Contracts.Records;

public class Message
{
    public long Offset { get; set; }
    public int MessageSize { get; set; }
    public uint Crc { get; set; }
    public byte Magic { get; set; }
    public byte Attributes { get; set; }
    public long? Timestamp { get; set; }
    public byte[]? Key { get; set; }
    public byte[]? Value { get; set; }
}