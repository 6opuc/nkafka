namespace nKafka.Client;

public interface IStreamProvider : IAsyncDisposable
{
    Stream ReadStream { get; }
    Stream WriteStream { get; }
    ValueTask OpenAsync(CancellationToken ct);
}
