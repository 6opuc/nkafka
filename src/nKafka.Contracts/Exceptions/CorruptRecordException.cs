namespace nKafka.Contracts.Exceptions;

public class CorruptRecordException : KafkaException
{
    public CorruptRecordException(string message) : base(message)
    {
    }

    public CorruptRecordException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
