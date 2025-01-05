namespace nKafka.Contracts.Records;

public static class RecordsContainerSerializerV1
{
    public static void Serialize(MemoryStream output, RecordsContainer? message, ISerializationContext context)
    {
        throw new NotImplementedException();
    }

    public static RecordsContainer? Deserialize(MemoryStream input, ISerializationContext context)
    {
        var size = PrimitiveSerializer.DeserializeInt(input);
        if (size < 0)
        {
            throw new Exception($"Negative record container size: {size}.");
        }

        var start = input.Position;
        if (start + size > input.Length)
        {
            throw new Exception(
                $"Record container expected {size} bytes but got only {input.Length - input.Position}.");
        }

        var container = new RecordsContainer
        {
            SizeInBytes = size,
            Messages = new List<Message>(),
        };
        var endOfLastMessage = start;
        while (true)
        {
            var message = MessageSerializerV1.Deserialize(input, context);
            if (message == null)
            {
                // incomplete message
                break;
            }

            endOfLastMessage = input.Position;
            container.Messages.Add(message);
        }

        container.RemainderInBytes = size - (int)(endOfLastMessage - start);
        if (container.RemainderInBytes > 0)
        {
            input.Position = endOfLastMessage + container.RemainderInBytes;
        }

        return container;
    }
}