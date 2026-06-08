namespace nKafka.Contracts.Exceptions;

public class PendingRequestTimeoutException : KafkaException
{
    public int CorrelationId { get; }
    public ApiKey ApiKey { get; }
    public int TimeoutMs { get; }

    public PendingRequestTimeoutException(int correlationId, ApiKey apiKey, int timeoutMs)
        : base($"Request (correlation ID: {correlationId}, API key: {apiKey}) timed out after {timeoutMs}ms.")
    {
        CorrelationId = correlationId;
        ApiKey = apiKey;
        TimeoutMs = timeoutMs;
    }
}
