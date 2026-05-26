using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;

namespace nKafka.Contracts;

public struct BufferWriter : IDisposable
{
    private Memory<byte> _buffer;
    private readonly byte[]? _rentedArray;
    private readonly ArrayPool<byte>? _pool;
    private int _pos;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public BufferWriter(Memory<byte> buffer)
    {
        _buffer = buffer;
        _rentedArray = null;
        _pool = null;
        _pos = 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public BufferWriter(ArrayPool<byte> pool, int capacity)
    {
        _pool = pool;
        _rentedArray = pool.Rent(capacity);
        _buffer = new Memory<byte>(_rentedArray, 0, capacity);
        _pos = 0;
    }

    public Memory<byte> Buffer => _buffer;
    public Span<byte> Span => _buffer.Span;
    public int Position { get => _pos; set => _pos = value; }
    public int Remaining => _buffer.Length - _pos;
    public int Capacity => _buffer.Length;
    public Memory<byte> Memory => _buffer;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Reset() => _pos = 0;

    public void Dispose()
    {
        if (_pool != null && _rentedArray != null)
        {
            _pool.Return(_rentedArray);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureSpace(int count)
    {
        if (_pos + count > _buffer.Length)
        {
            throw new InvalidOperationException($"Insufficient buffer space: need {count} bytes but only {Remaining} bytes remaining.");
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
    public void WriteByte(byte value)
    {
        EnsureSpace(1);
        _buffer.Span[_pos++] = value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteBool(bool value)
    {
        EnsureSpace(1);
        _buffer.Span[_pos++] = value ? (byte)1 : (byte)0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteInt16BigEndian(short value)
    {
        EnsureSpace(2);
        BinaryPrimitives.WriteInt16BigEndian(_buffer.Span.Slice(_pos), value);
        _pos += 2;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteInt32BigEndian(int value)
    {
        EnsureSpace(4);
        BinaryPrimitives.WriteInt32BigEndian(_buffer.Span.Slice(_pos), value);
        _pos += 4;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteUInt32BigEndian(uint value)
    {
        EnsureSpace(4);
        BinaryPrimitives.WriteUInt32BigEndian(_buffer.Span.Slice(_pos), value);
        _pos += 4;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteInt64BigEndian(long value)
    {
        EnsureSpace(8);
        BinaryPrimitives.WriteInt64BigEndian(_buffer.Span.Slice(_pos), value);
        _pos += 8;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteDoubleBigEndian(double value)
    {
        WriteInt64BigEndian(BitConverter.DoubleToInt64Bits(value));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteDoubleBigEndian(double? value) => WriteDoubleBigEndian(value!.Value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteUVarLong(ulong value)
    {
        do
        {
            EnsureSpace(1);
            var byteValue = value & 0x7fUL;
            value >>= 7;
            if (value > 0)
            {
                byteValue |= 128;
            }
            _buffer.Span[_pos++] = (byte)byteValue;
        } while (value > 0);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteVarLong(long value)
    {
        WriteUVarLong(ZigZag.Encode(value));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteVarInt(int value) => WriteVarLong(value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteUVarInt(uint value) => WriteUVarLong(value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteLength(int value) => WriteUVarLong((ulong)(value + 1));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteString(string? value)
    {
        if (value == null)
        {
            WriteByte(0xff);
            WriteByte(0xff);
            return;
        }

        var length = Encoding.UTF8.GetByteCount(value);
        if (length > short.MaxValue)
        {
            throw new InvalidOperationException($"String is too long: {length} bytes exceeds maximum of {short.MaxValue}.");
        }
        WriteInt16BigEndian((short)length);
        var span = value.AsSpan();
        Encoding.UTF8.GetBytes(span, _buffer.Span.Slice(_pos));
        _pos += length;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteVarString(string? value)
    {
        if (value == null)
        {
            WriteLength(-1);
            return;
        }

        var length = Encoding.UTF8.GetByteCount(value);
        WriteLength(length);
        var span = value.AsSpan();
        Encoding.UTF8.GetBytes(span, _buffer.Span.Slice(_pos));
        _pos += length;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteShort(short value) => WriteInt16BigEndian(value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteShort(short? value)
    {
        WriteShort(value!.Value);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteUshort(ushort value) => WriteShort((short)value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteUshort(ushort? value) => WriteShort((short)value!.Value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteByte(byte? value) => WriteByte(value!.Value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteInt(int value)
    {
        EnsureSpace(4);
        _buffer.Span[_pos++] = (byte)(value >> 24);
        _buffer.Span[_pos++] = (byte)(value >> 16);
        _buffer.Span[_pos++] = (byte)(value >> 8);
        _buffer.Span[_pos++] = (byte)value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteInt(int? value) => WriteInt(value!.Value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteUint(uint value) => WriteUInt32BigEndian(value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteUint(uint? value) => WriteUInt32BigEndian(value!.Value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteLong(long value)
    {
        EnsureSpace(8);
        ulong ui = (ulong)value;
        for (int j = 7; j >= 0; j--)
            _buffer.Span[_pos++] = (byte)(ui >> j * 8 & 0xff);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteLong(long? value) => WriteLong(value!.Value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteBool(bool? value) => WriteBool(value ?? false);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteVarLong(long? value) => WriteVarLong(value!.Value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteUVarLong(ulong? value) => WriteUVarLong(value!.Value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteVarInt(int? value) => WriteVarInt(value!.Value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteUVarInt(uint? value) => WriteUVarInt(value!.Value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteLength(int? value) => WriteLength(value ?? 0);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteGuid(Guid? value)
    {
        if (value == null)
        {
            _buffer.Span.Slice(_pos, 16).Clear();
            _pos += 16;
            return;
        }

        value!.Value.TryWriteBytes(_buffer.Span.Slice(_pos));
        _pos += 16;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(ReadOnlySpan<byte> buffer)
    {
        if (buffer.Length > 0)
        {
            EnsureSpace(buffer.Length);
            buffer.CopyTo(_buffer.Span.Slice(_pos));
            _pos += buffer.Length;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(ReadOnlyMemory<byte> buffer) => Write(buffer.Span);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteMemory(ReadOnlyMemory<byte> value)
    {
        if (value.IsEmpty) return;
        Write(value.Span);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int VarIntSize(uint value)
    {
        int size = 0;
        do
        {
            size++;
            value >>= 7;
        } while (value > 0);
        return size;
    }
}
