using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageSerializers;

namespace nKafka.Contracts;

public static class ConsumerProtocolSubscriptionExtensions
{
    public static byte[] AsMetadata(this ConsumerProtocolSubscription value, short version)
    {
        using var output = new MemoryStream();
        ConsumerProtocolSubscriptionSerializer.Serialize(output, value, version);
        return output.GetBuffer();
    }
}