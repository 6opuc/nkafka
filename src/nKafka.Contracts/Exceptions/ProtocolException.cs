namespace nKafka.Contracts.Exceptions;

public class ProtocolException : KafkaException
{
    public ProtocolException(string message) : base(message)
    {
    }

    public ProtocolException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
