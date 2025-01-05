using System.Buffers;

namespace nKafka.Client;

public class MessageWithPooledPayload(object message, ArrayPool<byte> pool, byte[] payload)
    : IDisposable
{
    public object Message { get; } = message;

    public void Dispose()
    {
        pool.Return(payload);
    }
}