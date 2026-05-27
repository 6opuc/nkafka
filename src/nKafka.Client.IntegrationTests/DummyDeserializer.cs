namespace nKafka.Client.IntegrationTests;

public class DummyDeserializer : IMessageDeserializer<byte[]>
{
    public byte[]? Deserialize(MessageDeserializationContext context)
    {
        return context.Value?.ToArray();
    }
}
