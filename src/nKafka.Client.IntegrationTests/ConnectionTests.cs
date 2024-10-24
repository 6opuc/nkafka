using FluentAssertions;
using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageDefinitions.MetadataRequestNested;
using nKafka.Contracts.RequestClients;

namespace nKafka.Client.IntegrationTests;

public class ConnectionTests
{
    [SetUp]
    public void Setup()
    {
    }

    [Test]
    public async Task ConnectAsyncAndDisposeAsyncShouldNotThrow()
    {
        await using var connection = await OpenConnection();
    }

    private async Task<Connection> OpenConnection()
    {
        var config = new ConnectionConfig("kafka-1", 9192);
        var connection = new Connection(TestLogger.Create<Connection>());
        
        await connection.OpenAsync(config, CancellationToken.None);

        return connection;
    }

    [Test]
    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    public async Task SendAsync_ApiVersionsRequest_ShouldReturnExpectedResult(short apiVersion)
    {
        await using var connection = await OpenConnection();
        var requestClient = new ApiVersionsRequestClient(apiVersion, new ApiVersionsRequest
        {
            ClientSoftwareName = "nKafka.Client",
            ClientSoftwareVersion = "0.0.1",
        });
        
        var response = await connection.SendAsync(requestClient, CancellationToken.None);

        response.Should().NotBeNull();
        #warning check response
    }
    
    [Test]
    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    [TestCase(4)]
    public async Task SendAsync_FindCoordinatorRequest_ShouldReturnExpectedResult(short apiVersion)
    {
        await using var connection = await OpenConnection();
        var consumerGroupId = Guid.NewGuid().ToString();
        var requestClient = new FindCoordinatorRequestClient(apiVersion, new FindCoordinatorRequest
        {
            Key = consumerGroupId,
            KeyType = 0, // 0 = group, 1 = transaction
            CoordinatorKeys = [consumerGroupId], // for versions 4+
        });
        
        var response = await connection.SendAsync(requestClient, CancellationToken.None);

        response.Should().NotBeNull();
        #warning check response
    }
    
    [Test]
    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    [TestCase(4)]
    [TestCase(5)]
    [TestCase(6)]
    [TestCase(7)]
    [TestCase(8)]
    [TestCase(9)]
    [TestCase(10)]
    [TestCase(11)]
    [TestCase(12)]
    public async Task SendAsync_MetadataRequest_ShouldReturnExpectedResult(short apiVersion)
    {
        await using var connection = await OpenConnection();
        var requestClient = new MetadataRequestClient(apiVersion, new MetadataRequest
        {
            Topics = [
                new MetadataRequestTopic
                {
                    Name = "test",
                    TopicId = Guid.Empty,
                }
            ],
            AllowAutoTopicCreation = false,
            IncludeClusterAuthorizedOperations = true,
            IncludeTopicAuthorizedOperations = true,
        });
        
        var response = await connection.SendAsync(requestClient, CancellationToken.None);

        response.Should().NotBeNull();
#warning check response
    }
}