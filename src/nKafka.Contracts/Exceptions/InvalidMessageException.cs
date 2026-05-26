namespace nKafka.Contracts.Exceptions;

public class InvalidMessageException : KafkaException
{
    public InvalidMessageException(string message) : base(message)
    {
    }

    public InvalidMessageException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
