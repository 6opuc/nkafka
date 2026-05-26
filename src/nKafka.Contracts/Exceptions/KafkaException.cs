namespace nKafka.Contracts.Exceptions;

public abstract class KafkaException : Exception
{
    protected KafkaException(string message) : base(message)
    {
    }

    protected KafkaException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
