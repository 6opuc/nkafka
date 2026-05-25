namespace nKafka.Contracts.Records;

public static class RecordsContainerSerializerV0
{
    public static void Serialize(ref BufferWriter writer, RecordsContainer? message, ISerializationContext context)
    {
        throw new NotImplementedException();
    }

    public static RecordsContainer? Deserialize(ref BufferReader reader, ISerializationContext context)
    {
        var size = reader.ReadInt32BigEndian();
        if (size < 0)
        {
            throw new Exception($"Negative record container size: {size}.");
        }

        var start = reader.Position;
        var remainingBefore = reader.Remaining;
        if (size > remainingBefore)
        {
            throw new Exception(
                $"Record container expected {size} bytes but got only {remainingBefore}.");
        }

        var container = new RecordsContainer
        {
            SizeInBytes = size,
            Messages = new List<Message>(),
        };
        var endOfLastMessage = start;
        while (true)
        {
            var message = MessageSerializerV0.Deserialize(ref reader, start + size, context);
            if (message == null)
            {
                break;
            }

            endOfLastMessage = reader.Position;
            container.Messages.Add(message);
        }

        container.RemainderInBytes = size - (int)(endOfLastMessage - start);
        return container;
    }
}
