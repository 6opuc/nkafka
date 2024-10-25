using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageSerializers;

namespace nKafka.Contracts;

public static class ConsumerProtocolAssignmentExtensions
{
    public static byte[] AsMetadata(this ConsumerProtocolAssignment value, short version)
    {
#warning reduce allocations, use buffer pools!!
#warning create override in message definitions: read size, version and payload        
        using var output = new MemoryStream();
        PrimitiveSerializer.SerializeShort(output, version);
        ConsumerProtocolAssignmentSerializer.Serialize(output, value, version);
        return output.ToArray();
    }

    public static ConsumerProtocolAssignment ConsumerProtocolAssignmentFromMetadata(this byte[] value)
    {
        using var input = new MemoryStream(value, 0, value.Length, false, true);
        var version = PrimitiveSerializer.DeserializeShort(input);
        return ConsumerProtocolAssignmentSerializer.Deserialize(input, version);
    }
}