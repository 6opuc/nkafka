namespace nKafka.Contracts;

public interface IRequest<TResponse> : IRequest
{
}

public interface IRequest
{
    ApiKey ApiKey { get; }
    short? FixedVersion { get; set; }
    VersionRange FlexibleVersions { get; }

    void SerializeRequest(ref BufferWriter writer, short version, ISerializationContext context);

    object DeserializeResponse(ReadOnlyMemory<byte> buffer, short version, ISerializationContext context);
}