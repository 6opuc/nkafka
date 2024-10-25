namespace nKafka.Contracts;

public static class RecordBatchContainerSerializer
{
    public static void Serialize(MemoryStream output, RecordBatchContainer? message)
    {
        throw new NotImplementedException();
    }

    public static RecordBatchContainer? Deserialize(MemoryStream input)
    {
        var size = PrimitiveSerializer.DeserializeInt(input);
        if (size < 0)
        {
            return null;
        }
        
        var message = new RecordBatchContainer
        {
            SizeInBytes = size,
            Payload = new byte[size],
        };
        input.Read(message.Payload, 0, size);
        return message;
    }
    
    public static void SerializeFlexible(MemoryStream output, RecordBatchContainer? message)
    {
        throw new NotImplementedException();
    }
    
    public static RecordBatchContainer? DeserializeFlexible(MemoryStream input)
    {
        var size = PrimitiveSerializer.DeserializeLength(input);
        if (size < 0)
        {
            return null;
        }
        
        var message = new RecordBatchContainer
        {
            SizeInBytes = size,
            Payload = new byte[size],
        };
        input.Read(message.Payload, 0, size);
        return message;
    }
}