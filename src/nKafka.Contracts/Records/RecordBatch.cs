namespace nKafka.Contracts.Records;

public class RecordBatch
{
    public long BaseOffset { get; set; }
    public int BatchLength { get; set; }
    public int PartitionLeaderEpoch { get; set; }
    public byte Magic { get; set; }
    public uint Crc { get; set; }
    public short Attributes { get; set; }
    public int LastOffsetDelta { get; set; }
    public long FirstTimestamp { get; set; }
    public long MaxTimestamp { get; set; }
    public long ProducerId { get; set; }
    public short ProducerEpoch { get; set; }
    public int BaseSequence { get; set; }
    public IList<Record>? Records { get; set; }
}