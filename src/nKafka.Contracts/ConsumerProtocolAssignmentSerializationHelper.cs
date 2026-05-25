using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageSerializers;

namespace nKafka.Contracts;

public static class ConsumerProtocolAssignmentSerializationHelper
{
    private static readonly short _version = 3;
        
    public static void Serialize(ref BufferWriter writer, ConsumerProtocolAssignment? message, bool flexible, ISerializationContext context)
    {
        using var buffer = context.CreateBuffer();
        var tw = new BufferWriter(buffer.Memory);
        if (message != null)
        {
            tw.WriteShort(_version);
            ConsumerProtocolAssignmentSerializer.Serialize(ref tw, message, _version, context);
            buffer.Writer = tw;
        }

        if (flexible)
        {
            writer.WriteLength(buffer.Position);
        }
        else
        {
            writer.WriteInt(buffer.Position == 0 ? -1 : (int)buffer.Position);
        }
        
        writer.Write(buffer.Memory.Span.Slice(0, (int)buffer.Position));
    }

    public static ConsumerProtocolAssignment? Deserialize(ref BufferReader reader, bool flexible, ISerializationContext context)
    {
        var length = flexible
            ? reader.ReadLength()
            : reader.ReadInt32BigEndian();
        if (length <= 0)
        {
            return null;
        }
        var version = reader.ReadInt16BigEndian();
        return ConsumerProtocolAssignmentSerializer.Deserialize(ref reader, version, context);
    }
}
