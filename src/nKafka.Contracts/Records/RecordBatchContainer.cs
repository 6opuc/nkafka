namespace nKafka.Contracts.Records;

public class RecordBatchContainer
{
    public int SizeInBytes { get; set; }
    public int RemainderInBytes { get; set; }
    public IList<RecordBatch>? RecordBatches { get; set; }
}