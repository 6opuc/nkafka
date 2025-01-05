using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageSerializers;

namespace nKafka.Contracts;

public static class ConsumerProtocolAssignmentSerializationHelper
{
    private static readonly short _version = 3;
        
    public static void Serialize(MemoryStream output, ConsumerProtocolAssignment? message, bool flexible, ISerializationContext context)
    {
        using var buffer = new MemoryStream();
        if (message != null)
        {
            PrimitiveSerializer.SerializeShort(buffer, _version);
            ConsumerProtocolAssignmentSerializer.Serialize(buffer, message, _version, context);
        }

        if (flexible)
        {
            PrimitiveSerializer.SerializeLengthLong(output, buffer.Position);
        }
        else
        {
            PrimitiveSerializer.SerializeInt(output, buffer.Position == 0 ? -1 : (int)buffer.Position);
        }
        buffer.Position = 0;
        buffer.CopyTo(output);
    }

    public static ConsumerProtocolAssignment? Deserialize(MemoryStream input, bool flexible, ISerializationContext context)
    {
        var length = flexible
            ? PrimitiveSerializer.DeserializeLength(input)
            : PrimitiveSerializer.DeserializeInt(input);
        if (length <= 0)
        {
            return null;
        }
        var version = PrimitiveSerializer.DeserializeShort(input);
        return ConsumerProtocolAssignmentSerializer.Deserialize(input, version, context);
    }
}