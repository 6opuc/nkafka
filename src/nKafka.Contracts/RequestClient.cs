using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageSerializers;

namespace nKafka.Contracts;

public abstract class RequestClient<TResponsePayload> : IRequestClient
{
    public int CorrelationId { get; } = IdGenerator.Next();
    protected abstract ApiKey ApiKey { get; }
    protected abstract VersionRange FlexibleVersions { get; }

    private short RequestHeaderVersion
    {
        get
        {
            if (ApiKey == ApiKey.ControlledShutdown && ApiVersion == 0)
            {
                return 0;
            }

            return (short)(FlexibleVersions.Includes(ApiVersion) ? 2 : 1);
        }
    }

    private short ResponseHeaderVersion
    {
        get
        {
            if (ApiKey == ApiKey.ApiVersions)
            {
                return 0;
            }

            return (short)(FlexibleVersions.Includes(ApiVersion) ? 1 : 0);
        }
    }

    protected abstract short ApiVersion { get; }

    public void SerializeRequest(MemoryStream output, ISerializationContext context)
    {
        var header = new RequestHeader
        {
            RequestApiKey = (short)ApiKey,
            RequestApiVersion = ApiVersion,
            CorrelationId = CorrelationId,
            ClientId = context.Config.ClientId,
        };
        PrimitiveSerializer.SerializeInt(output, 0); // placeholder for header + payload
        var start = output.Position;
        RequestHeaderSerializer.Serialize(output, header, RequestHeaderVersion, context);
        SerializeRequestPayload(output, context);
        var end = output.Position;
        var size = (int)(end - start);
        output.Position = start - 4;
        PrimitiveSerializer.SerializeInt(output, size);
        output.Position = end;
    }

    protected abstract void SerializeRequestPayload(MemoryStream output, ISerializationContext context);

    public TResponsePayload DeserializeResponse(MemoryStream input, ISerializationContext context)
    {
        var header = ResponseHeaderSerializer.Deserialize(input, ResponseHeaderVersion, context);
        if (header.CorrelationId == null)
        {
            throw new InvalidOperationException("Received response with empty correlation id.");
        }

        if (header.CorrelationId != CorrelationId)
        {
            throw new InvalidOperationException(
                $"Received response with incorrect correlation id. Expected {CorrelationId}, but got {header.CorrelationId}.");
        }

        return DeserializeResponsePayload(input, context);
    }

    protected abstract TResponsePayload DeserializeResponsePayload(MemoryStream input, ISerializationContext context);

    object IRequestClient.DeserializeResponse(MemoryStream input, ISerializationContext context)
    {
        return DeserializeResponse(input, context)!;
    }
}