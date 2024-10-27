using System.Collections.Immutable;

namespace nKafka.Contracts.Records;

public class Record
{
    public byte Attributes { get; set; }
    public long TimestampDelta { get; set; }
    public int OffsetDelta { get; set; }
    public byte[]? Key { get; set; }
    public byte[]? Value { get; set; }
    public IReadOnlyDictionary<string, byte[]>? Headers { get; set; }
}