namespace nKafka.Contracts.Records;

public static class RecordsContainerSerializerNotImplemented
{
    public static void Serialize(MemoryStream output, RecordsContainer? message)
    {
        throw new NotImplementedException();
    }

    public static RecordsContainer? Deserialize(MemoryStream input)
    {
        throw new NotImplementedException();
    }
}