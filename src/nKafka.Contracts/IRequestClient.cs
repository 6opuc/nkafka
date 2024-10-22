namespace nKafka.Contracts;

public interface IRequestClient
{
    int CorrelationId { get; }
    void SerializeRequest(MemoryStream output);
    object DeserializeResponse(MemoryStream input);
}