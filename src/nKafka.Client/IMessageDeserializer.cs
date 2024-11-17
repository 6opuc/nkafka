namespace nKafka.Client;

public interface IMessageDeserializer<TMessage>
{
    TMessage? Deserialize(MessageDeserializationContext context);
}