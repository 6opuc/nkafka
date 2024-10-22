namespace nKafka.Contracts;

public interface IRequest
{
    int CorrelationId { get; }
    ApiKey ApiKey { get; }
    short HeaderVersion { get; }
    short ApiVersion { get; }
    void SerializeRequest(MemoryStream output);
    object DeserializeResponse(MemoryStream input);
}