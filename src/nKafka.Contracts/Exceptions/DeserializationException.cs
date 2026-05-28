namespace nKafka.Contracts.Exceptions;

public class DeserializationException : KafkaException
{
    public DeserializationException(string message) : base(message)
    {
    }

    public DeserializationException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
