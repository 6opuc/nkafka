using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageSerializers;

namespace nKafka.Contracts;

public static class ConsumerProtocolSubscriptionSerializationHelper
{
    private static readonly short _version = 3;
        
    /// <summary>
    /// Serializes a ConsumerProtocolSubscription message.
    /// Uses a temporary BufferWriter (writerTemp) to build the message, then writes it to the output writer.
    /// This pattern allows us to calculate the message size before writing to the final destination.
    /// </summary>
    public static void Serialize(ref BufferWriter writer, ConsumerProtocolSubscription? message, bool flexible, ISerializationContext context)
    {
        var writerTemp = context.CreateWriter();
        try
        {
            if (message != null)
            {
                writerTemp.WriteShort(_version);
                ConsumerProtocolSubscriptionSerializer.Serialize(ref writerTemp, message, _version, context);
            }

            if (flexible)
            {
                writer.WriteLength(writerTemp.Position);
            }
            else
            {
                writer.WriteInt(writerTemp.Position == 0 ? -1 : (int)writerTemp.Position);
            }
            
            writer.Write(writerTemp.Memory.Span.Slice(0, (int)writerTemp.Position));
        }
        finally { writerTemp.Dispose(); }
    }

    public static ConsumerProtocolSubscription? Deserialize(ref BufferReader reader, bool flexible, ISerializationContext context)
    {
        var length = flexible
            ? reader.ReadLength()
            : reader.ReadInt32BigEndian();
        if (length == -1)
        {
            return null;
        }
        if (length == 0)
        {
            return new ConsumerProtocolSubscription();
        }
        var version = reader.ReadInt16BigEndian();
        return ConsumerProtocolSubscriptionSerializer.Deserialize(ref reader, version, context);
    }
}
