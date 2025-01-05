using System.Buffers;

namespace nKafka.Contracts;

public class PooledMemoryStream(ArrayPool<byte> arrayPool, byte[] buffer)
    : MemoryStream(buffer, 0, 0, true, true)
{
    private readonly byte[] _buffer = buffer;

    protected override void Dispose(bool disposing)
    {
        arrayPool.Return(_buffer);
        base.Dispose(disposing);
    }
}