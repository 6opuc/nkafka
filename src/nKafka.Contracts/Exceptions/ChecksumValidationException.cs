namespace nKafka.Contracts.Exceptions;

public class ChecksumValidationException : KafkaException
{
    public ChecksumValidationException(string message) : base(message)
    {
    }

    public ChecksumValidationException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
