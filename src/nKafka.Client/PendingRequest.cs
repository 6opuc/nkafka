using nKafka.Contracts;
using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageSerializers;

namespace nKafka.Client;

public class PendingRequest
{
    public IRequest Payload { get; init; }
    public TaskCompletionSource<MessageWithPooledPayload> Response { get; init; }
    public CancellationToken CancellationToken { get; init; }
    public int CorrelationId { get; init; }
    public short ApiVersion { get; init; }


    public PendingRequest(
        IRequest payload,
        TaskCompletionSource<MessageWithPooledPayload> response,
        CancellationToken cancellationToken,
        int correlationId,
        short apiVersion)
    {
        Payload = payload;
        Response = response;
        CancellationToken = cancellationToken;
        CorrelationId = correlationId;
        ApiVersion = payload.FixedVersion ?? apiVersion;
    }

    public void SerializeRequest(ref BufferWriter writer, ISerializationContext context)
    {
        var header = new RequestHeader
        {
            RequestApiKey = (short)Payload.ApiKey,
            RequestApiVersion = ApiVersion,
            CorrelationId = CorrelationId,
            ClientId = context.Config.ClientId,
        };
        var start = writer.Position;
        writer.WriteInt(0); // placeholder for header + payload size
        var requestHeaderVersion = Payload.ApiKey == ApiKey.ControlledShutdown && ApiVersion == 0
            ? (short)0
            : (short)(Payload.FlexibleVersions.Includes(ApiVersion) ? 2 : 1);
        RequestHeaderSerializer.Serialize(ref writer, header, requestHeaderVersion, context);
        Payload.SerializeRequest(ref writer, ApiVersion, context);
        var end = writer.Position;
        var size = (int)(end - start) - 4;
        writer.Position = start;
        writer.WriteInt(size);
        writer.Position = end;
    }

    public object DeserializeResponse(ref BufferReader reader, ISerializationContext context)
    {
        var responseHeaderVersion = Payload.ApiKey == ApiKey.ApiVersions
            ? (short)0
            : (short)(Payload.FlexibleVersions.Includes(ApiVersion) ? 1 : 0);
        var header = ResponseHeaderSerializer.Deserialize(ref reader, responseHeaderVersion, context);
        if (header.CorrelationId == null)
        {
            throw new InvalidOperationException("Received response with empty correlation id.");
        }

        if (header.CorrelationId != CorrelationId)
        {
            throw new InvalidOperationException(
                $"Received response with incorrect correlation id. Expected {CorrelationId}, but got {header.CorrelationId}.");
        }

        return Payload.DeserializeResponse(ref reader, ApiVersion, context);
    }
}
