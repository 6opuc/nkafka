namespace nKafka.Contracts.Records;

public class RecordBatch
{
    public long BaseOffset;
    public int BatchLength;
    public int PartitionLeaderEpoch;
    public byte Magic;
    public uint Crc;
    public short Attributes;
    public int LastOffsetDelta;
    public long FirstTimestamp;
    public long MaxTimestamp;
    public long ProducerId;
    public short ProducerEpoch;
    public int BaseSequence;
    public IList<Record>? Records;
}