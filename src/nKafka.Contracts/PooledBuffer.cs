using System.Buffers;

namespace nKafka.Contracts;

public sealed class PooledBuffer : IDisposable
{
    private readonly ArrayPool<byte> _pool;
    private readonly byte[] _rentedBuffer;
    private BufferWriter _writer;

    public PooledBuffer(ArrayPool<byte> pool, int capacity)
    {
        _pool = pool;
        _rentedBuffer = pool.Rent(capacity);
        _writer = new BufferWriter(_rentedBuffer);
    }

    public BufferWriter Writer { get => _writer; internal set => _writer = value; }
    public int Position => _writer.Position;
    public Memory<byte> Memory => _rentedBuffer;

    public void Dispose()
    {
        _pool.Return(_rentedBuffer);
    }
}
