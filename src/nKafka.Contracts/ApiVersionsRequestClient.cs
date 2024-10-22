using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageSerializers;

namespace nKafka.Contracts;

public class ApiVersionsRequestClient : RequestClient<ApiVersionsResponse>
{
    public override ApiKey ApiKey => ApiKey.ApiVersions;
    public override short HeaderVersion { get; }
    public override short ApiVersion { get; }
    public ApiVersionsRequest Request { get; }

    public ApiVersionsRequestClient(short headerVersion, short apiVersion, ApiVersionsRequest request)
    {
        #warning min/max version for header
        HeaderVersion = headerVersion;
        #warning min/max version for request
        ApiVersion = apiVersion;
        #warning request property validation?
        Request = request;
    }
    
    protected override void SerializeRequestPayload(MemoryStream output)
    {
        ApiVersionsRequestSerializer.Serialize(output, Request, ApiVersion);
    }

    protected override ApiVersionsResponse DeserializeResponsePayload(MemoryStream input)
    {
        return ApiVersionsResponseSerializer.Deserialize(input, ApiVersion);
    }
}