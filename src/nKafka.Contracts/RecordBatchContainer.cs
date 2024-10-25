namespace nKafka.Contracts;

public class RecordBatchContainer
{
    public int SizeInBytes { get; set; }
    public byte[]? Payload { get; set; }
}