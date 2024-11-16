

namespace nKafka.Contracts.Records;

public class Record
{
    public byte Attributes { get; set; }
    public long TimestampDelta { get; set; }
    public int OffsetDelta { get; set; }
    public Memory<byte>? Key { get; set; }
    public Memory<byte>? Value { get; set; }
    public IReadOnlyDictionary<string, Memory<byte>?>? Headers { get; set; }
}