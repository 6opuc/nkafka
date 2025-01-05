namespace nKafka.Contracts;

public interface IRequestClient
{
    int CorrelationId { get; }
    void SerializeRequest(MemoryStream output, ISerializationContext context);
    object DeserializeResponse(MemoryStream input, ISerializationContext context);
}