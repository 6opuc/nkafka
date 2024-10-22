using Microsoft.Extensions.Logging;
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
}