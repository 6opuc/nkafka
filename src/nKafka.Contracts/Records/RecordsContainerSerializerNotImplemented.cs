namespace nKafka.Contracts.Records;

public static class RecordsContainerSerializerNotImplemented
{
    public static void Serialize(MemoryStream output, RecordsContainer? message, ISerializationContext context)
    {
        throw new NotImplementedException();
    }

    public static RecordsContainer? Deserialize(MemoryStream input, ISerializationContext context)
    {
        throw new NotImplementedException();
    }
}