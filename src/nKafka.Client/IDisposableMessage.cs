namespace nKafka.Client;

public interface IDisposableMessage<T> : IDisposable
{
    T Message { get; }
    short Version { get; }
}