namespace nKafka.Contracts.Records;

public static class RecordsContainerSerializerV0
{
    public static void Serialize(MemoryStream output, RecordsContainer? message)
    {
        throw new NotImplementedException();
    }

    public static RecordsContainer? Deserialize(MemoryStream input)
    {
        var size = PrimitiveSerializer.DeserializeInt(input);
        if (size < 0)
        {
            throw new Exception($"Negative record container size: {size}.");
        }

        var start = input.Position;
        if (start + size > input.Length)
        {
            throw new Exception($"Record container expected {size} bytes but got only {input.Length - input.Position}.");
        }
        
        var container = new RecordsContainer
        {
            SizeInBytes = size,
#warning decide on capacity
            Messages = new List<Message>(),
        };
        var endOfLastMessage = start;
        while (true)
        {
            var message = MessageSerializerV0.Deserialize(input);
            if (message == null)
            {
                // incomplete message
                break;
            }

            endOfLastMessage = input.Position;
            container.Messages.Add(message);
        }
        container.RemainderInBytes = size - (int)(endOfLastMessage-start);
        if (container.RemainderInBytes > 0)
        {
            input.Position = endOfLastMessage + container.RemainderInBytes;
        }
        return container;
    }
}