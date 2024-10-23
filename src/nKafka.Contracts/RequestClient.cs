using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageSerializers;

namespace nKafka.Contracts;

public abstract class RequestClient<TResponsePayload> : IRequestClient
{
    public int CorrelationId { get; } = IdGenerator.Next();
    protected abstract ApiKey ApiKey { get; }

    protected abstract short RequestHeaderVersion { get; }
    protected abstract short ResponseHeaderVersion { get; }

    protected abstract short ApiVersion { get; }
    
    public void SerializeRequest(MemoryStream output)
    {
        var header = new RequestHeader
        {
            RequestApiKey = (short)ApiKey,
            RequestApiVersion = ApiVersion,
            CorrelationId = CorrelationId,
            #warning client id (application name should be provided)
            ClientId = null,
        };
        PrimitiveSerializer.SerializeInt(output, 0); // placeholder for header + payload
        var start = output.Position;
        RequestHeaderSerializer.Serialize(output, header, RequestHeaderVersion);
        SerializeRequestPayload(output);
        var end = output.Position;
        var size = (int)(end - start);
        output.Position = start - 4;
        PrimitiveSerializer.SerializeInt(output, size);
        output.Position = end;
    }

    protected abstract void SerializeRequestPayload(MemoryStream output);

    public TResponsePayload DeserializeResponse(MemoryStream input)
    {
        var header = ResponseHeaderSerializer.Deserialize(input, ResponseHeaderVersion);
        if (header.CorrelationId == null)
        {
            throw new InvalidOperationException("Received response with empty correlation id.");
        }
                        
        if (header.CorrelationId != CorrelationId)
        {
            throw new InvalidOperationException(
                $"Received response with incorrect correlation id. Expected {CorrelationId}, but got {header.CorrelationId}.");
        }

        return DeserializeResponsePayload(input);
    }

    protected abstract TResponsePayload DeserializeResponsePayload(MemoryStream input);

    object IRequestClient.DeserializeResponse(MemoryStream input)
    {
        return DeserializeResponse(input)!;
    }
}