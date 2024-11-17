namespace nKafka.Client.TestApp;

public class DummyStringMessageDeserializer : IMessageDeserializer<DummyStringMessage>
{
    public DummyStringMessage? Deserialize(MessageDeserializationContext context)
    {
        throw new NotImplementedException();
    }
}