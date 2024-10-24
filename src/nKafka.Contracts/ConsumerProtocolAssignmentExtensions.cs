using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageSerializers;

namespace nKafka.Contracts;

public static class ConsumerProtocolAssignmentExtensions
{
    public static byte[] AsMetadata(this ConsumerProtocolAssignment value, short version)
    {
#warning reduce allocations, use buffer pools!!
        using var output = new MemoryStream();
        PrimitiveSerializer.SerializeShort(output, version);
        ConsumerProtocolAssignmentSerializer.Serialize(output, value, version);
        return output.ToArray();
    }
}