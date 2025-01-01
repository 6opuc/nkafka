using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageSerializers;

namespace nKafka.Contracts;

public static class ConsumerProtocolSubscriptionExtensions
{
    public static byte[] AsMetadata(this ConsumerProtocolSubscription value, short version)
    {
#warning create override in message definitions: read size, version and payload    
        #warning reduce allocations, use buffer pools!!
        using var output = new MemoryStream();
        PrimitiveSerializer.SerializeShort(output, version);
        ConsumerProtocolSubscriptionSerializer.Serialize(output, value, version);
        return output.ToArray();
    }

    public static ConsumerProtocolSubscription ConsumerProtocolSubscriptionFromMetadata(this byte[] value)
    {
        using var input = new MemoryStream(value, 0, value.Length, false, true);
        var version = PrimitiveSerializer.DeserializeShort(input);
        return ConsumerProtocolSubscriptionSerializer.Deserialize(input, version);
    }
}