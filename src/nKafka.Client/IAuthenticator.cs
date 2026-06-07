namespace nKafka.Client;

public interface IAuthenticator
{
    ValueTask AuthenticateAsync(IConnection connection, CancellationToken ct);
}
