namespace nKafka.Client.TestAppConsumerGroup;

public class DummyBytesMessageDeserializer : IMessageDeserializer<Memory<byte>?>
{
    public Memory<byte>? Deserialize(MessageDeserializationContext context)
    {
        if (context.Value != null &&
            context.Value.Value.Length > 0)
        {
            return context.Value;
        }

        return null;
    }
}