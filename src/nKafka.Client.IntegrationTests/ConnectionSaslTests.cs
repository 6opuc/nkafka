using FluentAssertions;
using nKafka.Contracts;
using nKafka.Contracts.MessageDefinitions;

namespace nKafka.Client.IntegrationTests;

public class ConnectionSaslTests
{
    private const string BootstrapHost = "localhost";
    private const int BootstrapPort = 9192;
    private const string SaslMechanism = "SCRAM-SHA-512";
    private const string SaslUsername = "admin";
    private const string SaslPassword = "admin-secret";
    private static readonly string SslCaCertPath = Path.GetFullPath(
        Path.Combine(
            TestContext.CurrentContext.TestDirectory,
            "../../../../../infra/secrets/ca-cert.pem"));

    [Test]
    public async Task ConnectAsync_SaslSsl_ShouldSucceed()
    {
        await using var connection = await OpenSaslConnection();

        var request = new ApiVersionsRequest
        {
            ClientSoftwareName = "nKafka.Client",
            ClientSoftwareVersion = "0.0.1",
        };
        using var response = await connection.SendAsync(request, CancellationToken.None);

        response.Should().NotBeNull();
        response.Message.ErrorCode.Should().Be(0);
        response.Message.ApiKeys.Should().NotBeNull();
    }

    [Test]
    public async Task SaslHandshake_ShouldReturnSupportedMechanisms()
    {
        // Open a connection with TLS only (no SASL auth) so we can manually handshake
        var config = new ConnectionConfig(
            "SASL_SSL",
            BootstrapHost,
            BootstrapPort,
            "nKafka.Client.IntegrationTests")
        {
            RequestApiVersionsOnOpen = false,
            SslCaCertPath = SslCaCertPath,
        };

        await using var connection = new Connection(config, TestLoggerFactory.Instance);
        await connection.OpenAsync(CancellationToken.None);

        var request = new SaslHandshakeRequest
        {
            Mechanism = SaslMechanism,
        };
        using var response = await connection.SendAsync(request, CancellationToken.None);

        response.Should().NotBeNull();
        response.Message.ErrorCode.Should().Be(0);
        response.Message.Mechanisms.Should().Contain(SaslMechanism);
    }

    [Test]
    public async Task ConnectAsync_WrongPassword_ShouldFail()
    {
        var config = new ConnectionConfig(
            "SASL_SSL",
            BootstrapHost,
            BootstrapPort,
            "nKafka.Client.IntegrationTests")
        {
            RequestApiVersionsOnOpen = false,
            SaslMechanism = SaslMechanism,
            SaslUsername = SaslUsername,
            SaslPassword = "wrong-password",
            SslCaCertPath = SslCaCertPath,
        };

        var connection = new Connection(config, TestLoggerFactory.Instance);

        var act = async () => await connection.OpenAsync(CancellationToken.None);
        await act.Should().ThrowAsync<Exception>()
            .WithMessage("*authentication failed*");
    }

    [Test]
    public async Task ConnectAsync_UnsupportedMechanism_ShouldFail()
    {
        var config = new ConnectionConfig(
            "SASL_SSL",
            BootstrapHost,
            BootstrapPort,
            "nKafka.Client.IntegrationTests")
        {
            RequestApiVersionsOnOpen = false,
            SaslMechanism = "SCRAM-SHA-1",
            SaslUsername = SaslUsername,
            SaslPassword = SaslPassword,
            SslCaCertPath = SslCaCertPath,
        };

        var connection = new Connection(config, TestLoggerFactory.Instance);

        var act = async () => await connection.OpenAsync(CancellationToken.None);
        await act.Should().ThrowAsync<Exception>()
            .WithMessage("*error code 33*");
    }

    private async Task<Connection> OpenSaslConnection()
    {
        var config = new ConnectionConfig(
            "SASL_SSL",
            BootstrapHost,
            BootstrapPort,
            "nKafka.Client.IntegrationTests")
        {
            RequestApiVersionsOnOpen = false,
            SaslMechanism = SaslMechanism,
            SaslUsername = SaslUsername,
            SaslPassword = SaslPassword,
            SslCaCertPath = SslCaCertPath,
        };

        var connection = new Connection(config, TestLoggerFactory.Instance);
        await connection.OpenAsync(CancellationToken.None);
        return connection;
    }
}
