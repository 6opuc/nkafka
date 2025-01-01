namespace nKafka.Contracts;

public interface IRequestClient
{
    int CorrelationId { get; }
    void SerializeRequest(MemoryStream output, string clientId);
    object DeserializeResponse(MemoryStream input);
}