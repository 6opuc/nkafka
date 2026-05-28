namespace nKafka.Contracts.Records;

public static class RecordsContainerSerializerNotImplemented
{
    public static void Serialize(ref BufferWriter writer, RecordsContainer? message, ISerializationContext context)
    {
        throw new NotImplementedException();
    }

    public static RecordsContainer? Deserialize(ref BufferReader reader, ISerializationContext context)
    {
        throw new NotImplementedException();
    }
}
