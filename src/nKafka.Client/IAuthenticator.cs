namespace nKafka.Client;

public interface IAuthenticator
{
    ValueTask AuthenticateAsync(Stream stream, CancellationToken ct);
}
