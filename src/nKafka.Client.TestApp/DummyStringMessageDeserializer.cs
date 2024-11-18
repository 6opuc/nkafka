using System.Text;

namespace nKafka.Client.TestApp;

public class DummyStringMessageDeserializer : IMessageDeserializer<DummyStringMessage>
{
    public DummyStringMessage? Deserialize(MessageDeserializationContext context)
    {
        var result = new DummyStringMessage();
        if (context.Value != null &&
            context.Value.Value.Length > 0)
        {
            result.Value = Encoding.UTF8.GetString(context.Value.Value.Span);
        }
        return result;
    }
}