

namespace nKafka.Contracts.Records;

public class Record
{
    public byte Attributes;
    public long TimestampDelta;
    public int OffsetDelta;
    public Memory<byte>? Key;
    public Memory<byte>? Value;
    public IReadOnlyDictionary<string, Memory<byte>?>? Headers;
}