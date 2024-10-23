using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageSerializers;

namespace nKafka.Contracts;

public class ApiVersionsRequestClient : RequestClient<ApiVersionsResponse>
{
    protected override ApiKey ApiKey => ApiKey.ApiVersions;
    protected override short RequestHeaderVersion => (short)(ApiVersion >= 3 ? 2 : 1);
    protected override short ResponseHeaderVersion => 0;
    protected override short ApiVersion { get; }
    public ApiVersionsRequest Request { get; }

    public ApiVersionsRequestClient(short apiVersion, ApiVersionsRequest request)
    {
        #warning min/max version for header
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