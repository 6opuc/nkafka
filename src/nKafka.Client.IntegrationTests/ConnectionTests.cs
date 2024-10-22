using FluentAssertions;
using Microsoft.Extensions.Logging;
using nKafka.Contracts;
using nKafka.Contracts.MessageDefinitions;
using NUnit.Framework.Internal;

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
        var config = new ConnectionConfig("kafka-1", 9192);
        await using var connection = new Connection(TestLogger.Create<Connection>());
        
        await connection.OpenAsync(config, CancellationToken.None);
    }

    [Test]
    [TestCase(0, 0)]
    public async Task SendAsync_ApiVersionsRequest_ShouldReturnExpectedResult(short headerVersion, short apiVersion)
    {
        var config = new ConnectionConfig("kafka-1", 9192);
        await using var connection = new Connection(TestLogger.Create<Connection>());
        await connection.OpenAsync(config, CancellationToken.None);
        var requestClient = new ApiVersionsRequestClient(headerVersion, apiVersion, new ApiVersionsRequest
        {
            ClientSoftwareName = "nKafka.Client",
            ClientSoftwareVersion = "0.0.1",
        });
        
        var response = await connection.SendAsync(requestClient, CancellationToken.None);

        response.Should().NotBeNull();
    }
}