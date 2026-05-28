namespace nKafka.Contracts.Exceptions;

public class SerializationException : KafkaException
{
    public SerializationException(string message) : base(message)
    {
    }

    public SerializationException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
