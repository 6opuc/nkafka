namespace nKafka.Contracts.Exceptions;

public class IncompleteMessageException : KafkaException
{
    public IncompleteMessageException(string message) : base(message)
    {
    }

    public IncompleteMessageException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
