using nKafka.Contracts.Exceptions;

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

        int messageStart = reader.Position;

        if (reader.Position + message.MessageSize > eof)
        {
            return null;
        }

        message.Crc = reader.ReadUInt32BigEndian();

        int crcStart = reader.Position;
        message.Magic = reader.ReadByte();
        if (message.Magic != 0)
        {
            throw new ProtocolException($"Version 0 was expected, but received version {message.Magic}.");
        }
        message.Attributes = reader.ReadByte();

        int keyLength = reader.ReadInt32BigEndian();
        message.Key = keyLength == -1
            ? null
            : reader.ReadMemory(keyLength);
        int valueLength = reader.ReadInt32BigEndian();
        message.Value = valueLength == -1
            ? null
            : reader.ReadMemory(valueLength);

        if (context.Config.CheckCrcs)
        {
            long crcDataLength = reader.Position - crcStart;
            ChecksumValidator.ValidateCrc32(message.Crc, reader.Buffer, (int)crcStart, crcDataLength);
        }

        int actualMessageSize = reader.Position - messageStart;
        if (actualMessageSize != message.MessageSize)
        {
            throw new DeserializationException($"Expected message size was {message.MessageSize}, but got {actualMessageSize}.");
        }

        return message;
    }
}
