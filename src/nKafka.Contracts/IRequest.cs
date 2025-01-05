namespace nKafka.Contracts;

public interface IRequest<TResponse> : IRequest
{
}

public interface IRequest
{
    ApiKey ApiKey { get; }
    short? FixedVersion { get; set; }
    VersionRange FlexibleVersions { get; }

    void SerializeRequest(MemoryStream output, short version, ISerializationContext context);

    object DeserializeResponse(MemoryStream input, short version, ISerializationContext context);
}