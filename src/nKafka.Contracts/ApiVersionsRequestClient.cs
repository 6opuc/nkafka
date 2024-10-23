using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageSerializers;

namespace nKafka.Contracts;

public class ApiVersionsRequestClient : RequestClient<ApiVersionsResponse>
{
    protected override ApiKey ApiKey => ApiKey.ApiVersions;
    protected override VersionRange FlexibleVersions { get; } = new VersionRange(3);
    protected override short ApiVersion { get; }
    private readonly ApiVersionsRequest _request;

    public ApiVersionsRequestClient(short apiVersion, ApiVersionsRequest request)
    {
        #warning min/max version for header
        #warning min/max version for request
        #warning request property validation?
        ApiVersion = apiVersion;
        _request = request;
    }
    
    protected override void SerializeRequestPayload(MemoryStream output)
    {
        ApiVersionsRequestSerializer.Serialize(output, _request, ApiVersion);
    }

    protected override ApiVersionsResponse DeserializeResponsePayload(MemoryStream input)
    {
        return ApiVersionsResponseSerializer.Deserialize(input, ApiVersion);
    }
}