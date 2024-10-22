using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageSerializers;

namespace nKafka.Contracts;

public abstract class Request<TResponsePayload> : IRequest
{
    public int CorrelationId { get; } = IdGenerator.Next();
    public abstract ApiKey ApiKey { get; }
    public abstract short HeaderVersion { get; }
    public abstract short ApiVersion { get; }
    
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
        RequestHeaderSerializer.Serialize(output, header, HeaderVersion);
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
        var header = ResponseHeaderSerializer.Deserialize(input, HeaderVersion);
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

    object IRequest.DeserializeResponse(MemoryStream input)
    {
        return DeserializeResponse(input)!;
    }
}