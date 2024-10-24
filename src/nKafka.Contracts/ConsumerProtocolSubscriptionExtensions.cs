using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageSerializers;

namespace nKafka.Contracts;

public static class ConsumerProtocolSubscriptionExtensions
{
    public static byte[] AsMetadata(this ConsumerProtocolSubscription value, short version)
    {
        #warning reduce allocations, use buffer pools!!
        using var output = new MemoryStream();
        PrimitiveSerializer.SerializeShort(output, version);
        ConsumerProtocolSubscriptionSerializer.Serialize(output, value, version);
        return output.ToArray();
    }
}