namespace nKafka.Contracts.Records;

public static class RecordBatchContainerSerializerNotImplemented
{
    public static void Serialize(MemoryStream output, RecordBatchContainer? message)
    {
        throw new NotImplementedException();
    }

    public static RecordBatchContainer? Deserialize(MemoryStream input)
    {
        throw new NotImplementedException();
    }
}