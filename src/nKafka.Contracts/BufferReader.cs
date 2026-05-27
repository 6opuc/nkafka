using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;

namespace nKafka.Contracts;

public struct BufferReader
{
    private readonly ReadOnlyMemory<byte> _buffer;
    private int _pos;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public BufferReader(ReadOnlyMemory<byte> buffer)
    {
        _buffer = buffer;
        _pos = 0;
    }

    public ReadOnlyMemory<byte> Buffer => _buffer;
    public int Position { get => _pos; internal set => _pos = value; }

    public int Remaining => _buffer.Length - _pos;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureAvailable(int count)
    {
        if (count < 0 || _pos + count > _buffer.Length)
        {
            throw new InvalidOperationException($"Insufficient buffer data: need {count} bytes but only {Remaining} bytes remaining.");
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Advance(int count)
    {
        if (count < 0 || _pos + count > _buffer.Length)
        {
            throw new InvalidOperationException($"Invalid advance: cannot advance by {count} bytes from position {_pos} in buffer of size {_buffer.Length}.");
        }
        _pos += count;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public BufferReader CreateChild(int length)
    {
        var result = new BufferReader(_buffer.Slice(_pos, length));
        _pos += length;
        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> ReadSpan(int length)
    {
        EnsureAvailable(length);
        var span = _buffer.Span.Slice(_pos, length);
        _pos += length;
        return span;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public BufferReader CreateRemaining()
    {
        var result = new BufferReader(_buffer.Slice(_pos));
        _pos = _buffer.Length;
        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public byte ReadByte()
    {
        EnsureAvailable(1);
        return _buffer.Span[_pos++];
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool ReadBool()
    {
        EnsureAvailable(1);
        return _buffer.Span[_pos++] != 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public short ReadInt16BigEndian()
    {
        EnsureAvailable(2);
        short value = BinaryPrimitives.ReadInt16BigEndian(_buffer.Span[_pos..]);
        _pos += 2;
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int ReadInt32BigEndian()
    {
        EnsureAvailable(4);
        int value = BinaryPrimitives.ReadInt32BigEndian(_buffer.Span[_pos..]);
        _pos += 4;
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public uint ReadUInt32BigEndian()
    {
        EnsureAvailable(4);
        uint value = BinaryPrimitives.ReadUInt32BigEndian(_buffer.Span[_pos..]);
        _pos += 4;
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long ReadInt64BigEndian()
    {
        EnsureAvailable(8);
        long value = BinaryPrimitives.ReadInt64BigEndian(_buffer.Span[_pos..]);
        _pos += 8;
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public double ReadDoubleBigEndian()
    {
        EnsureAvailable(8);
        var span = _buffer.Span.Slice(_pos, 8);
        _pos += 8;
        return BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64BigEndian(span));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ulong ReadUVarLong()
    {
        ulong value = 0;
        int shift = 0;
        while (_pos < _buffer.Length)
        {
            byte b = _buffer.Span[_pos++];
            value |= (b & 0x7fUL) << shift;
            shift += 7;
            if ((b & 0x80) == 0) return value;
        }
        throw new EndOfStreamException("Unexpected end of stream while reading varlong.");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long ReadVarLong()
    {
        ulong raw = ReadUVarLong();
        return (long)((raw >> 1) ^ (0UL - (raw & 1UL)));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int ReadVarInt() => checked((int)ReadVarLong());

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public uint ReadUVarInt() => checked((uint)ReadUVarLong());

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int ReadLength()
    {
        ulong value = ReadUVarLong();
        if (value == 0) return -1;
        if (value > int.MaxValue + 1UL)
        {
            throw new InvalidOperationException($"Length value {value} exceeds maximum allowed ({int.MaxValue}).");
        }
        return (int)value - 1;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public string? ReadString()
    {
        short len = ReadInt16BigEndian();
        if (len < 0) return null;
        EnsureAvailable(len);
        string s = Encoding.UTF8.GetString(_buffer.Span.Slice(_pos, len));
        _pos += len;
        return s;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public string? ReadVarString()
    {
        int len = ReadLength();
        if (len < 0) return null;
        EnsureAvailable(len);
        string s = Encoding.UTF8.GetString(_buffer.Span.Slice(_pos, len));
        _pos += len;
        return s;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Guid ReadGuid()
    {
        EnsureAvailable(16);
        var g = new Guid(_buffer.Span.Slice(_pos, 16));
        _pos += 16;
        return g;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Memory<byte> ReadMemory(int length)
    {
        if (length < 0)
        {
            throw new InvalidOperationException($"Invalid memory length: {length}.");
        }
        EnsureAvailable(length);
        var result = _buffer.Slice(_pos, length);
        _pos += length;
        return Unsafe.As<ReadOnlyMemory<byte>, Memory<byte>>(ref result);
    }
}
