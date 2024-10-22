namespace nKafka.Contracts;

public interface IRequest
{
    int CorrelationId { get; }
    void SerializeRequest(MemoryStream output);
    object DeserializeResponse(MemoryStream input);
}