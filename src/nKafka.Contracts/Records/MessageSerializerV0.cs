namespace nKafka.Contracts.Records;

public static class MessageSerializerV0
{
    public static Message? Deserialize(ref BufferReader reader, long eof, ISerializationContext context)
    {
        if (reader.Position + 8 + 4 > eof)
        {
            return null;
        }

        var message = new Message
        {
            Offset = reader.ReadInt64BigEndian(),
            MessageSize = reader.ReadInt32BigEndian(),
        };
        
        var messageStart = reader.Position;

        if (reader.Position + message.MessageSize > eof)
        {
            return null;
        }
        
        message.Crc = reader.ReadUInt32BigEndian();

        var crcStart = reader.Position;
        message.Magic = reader.ReadByte();
        if (message.Magic != 0)
        {
            throw new Exception($"Version 0 was expected, but received version {message.Magic}.");
        }
        message.Attributes = reader.ReadByte();

        var keyLength = reader.ReadInt32BigEndian();
        message.Key = keyLength == -1
            ? null
            : keyLength == 0
                ? Array.Empty<byte>()
                : reader.ReadSpan(keyLength).ToArray();
        var valueLength = reader.ReadInt32BigEndian();
        message.Value = valueLength == -1
            ? null
            : valueLength == 0
                ? Array.Empty<byte>()
                : reader.ReadSpan(valueLength).ToArray();

        if (context.Config.CheckCrcs)
        {
            long crcDataLength = reader.Position - crcStart;
            ChecksumValidator.ValidateCrc32(message.Crc, reader.Buffer, (int)crcStart, crcDataLength);
        }

        var actualMessageSize = reader.Position - messageStart;
        if (actualMessageSize != message.MessageSize)
        {
            throw new Exception($"Expected message size was {message.MessageSize}, but got {actualMessageSize}.");
        }

        return message;
    }
}
