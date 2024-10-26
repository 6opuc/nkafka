namespace nKafka.Contracts.Records;

public static class MessageSerializerV0
{
    public static Message? Deserialize(MemoryStream input)
    {
        var start = input.Position;
        if (start + 8 + 4 > input.Length)
        {
            // we will not be able to read message size
            return null;
        }

        var message = new Message
        {
            Offset = PrimitiveSerializer.DeserializeLong(input),
            MessageSize = PrimitiveSerializer.DeserializeInt(input),
        };
        
        var messageStart = input.Position;

        if (input.Position + message.MessageSize > input.Length)
        {
            // we will not be able to read full message
            input.Position = start;
            return null;
        }
        
        message.Crc = PrimitiveSerializer.DeserializeUint(input);
#warning check crc from this position
        message.Magic = PrimitiveSerializer.DeserializeByte(input);
        if (message.Magic != 0)
        {
            throw new Exception($"Version 0 was expected, but received version {message.Magic}.");
        }
        message.Attributes = PrimitiveSerializer.DeserializeByte(input);

        var keyLength = PrimitiveSerializer.DeserializeInt(input);
        message.Key = keyLength == -1
            ? null
            : keyLength == 0
                ? Array.Empty<byte>()
                : new byte[keyLength];
        if (keyLength > 0)
        {
            input.Read(message.Key!, 0, keyLength);
        }
        var valueLength = PrimitiveSerializer.DeserializeInt(input);
        message.Value = valueLength == -1
            ? null
            : valueLength == 0
                ? Array.Empty<byte>()
                : new byte[valueLength];
        if (valueLength > 0)
        {
            input.Read(message.Value!, 0, valueLength);
        }

#warning validate actual crc
#warning validate actual message length
        
        if (input.Position != messageStart + message.MessageSize)
        {
            input.Position = messageStart + message.MessageSize;
        }

        return message;
    }
}