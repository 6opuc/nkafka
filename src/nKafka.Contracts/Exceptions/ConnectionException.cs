namespace nKafka.Contracts.Exceptions;

public class ConnectionException : KafkaException
{
    public ConnectionException(string message) : base(message)
    {
    }

    public ConnectionException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
