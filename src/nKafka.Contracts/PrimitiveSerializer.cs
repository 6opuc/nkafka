using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
        var value = BinaryPrimitives.ReadInt16BigEndian(_buffer.Span[_pos..]);
        _pos += 2;
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int ReadInt32BigEndian()
    {
        EnsureAvailable(4);
        var value = BinaryPrimitives.ReadInt32BigEndian(_buffer.Span[_pos..]);
        _pos += 4;
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public uint ReadUInt32BigEndian()
    {
        EnsureAvailable(4);
        var value = BinaryPrimitives.ReadUInt32BigEndian(_buffer.Span[_pos..]);
        _pos += 4;
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long ReadInt64BigEndian()
    {
        EnsureAvailable(8);
        var value = BinaryPrimitives.ReadInt64BigEndian(_buffer.Span[_pos..]);
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
            var b = _buffer.Span[_pos++];
            value |= (b & 0x7fUL) << shift;
            shift += 7;
            if ((b & 0x80) == 0) return value;
        }
        throw new EndOfStreamException("Unexpected end of stream while reading varlong.");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long ReadVarLong()
    {
        var raw = ReadUVarLong();
        return (long)((raw >> 1) ^ (0UL - (raw & 1UL)));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int ReadVarInt() => checked((int)ReadVarLong());

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public uint ReadUVarInt() => checked((uint)ReadUVarLong());

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int ReadLength()
    {
        var value = ReadUVarLong();
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
        var len = ReadInt16BigEndian();
        if (len < 0) return null;
        EnsureAvailable(len);
        var s = Encoding.UTF8.GetString(_buffer.Span.Slice(_pos, len));
        _pos += len;
        return s;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public string? ReadVarString()
    {
        var len = ReadLength();
        if (len < 0) return null;
        EnsureAvailable(len);
        var s = Encoding.UTF8.GetString(_buffer.Span.Slice(_pos, len));
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

public struct BufferWriter
{
    private readonly Memory<byte> _buffer;
    private int _pos;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public BufferWriter(Memory<byte> buffer)
    {
        _buffer = buffer;
        _pos = 0;
    }

    public Memory<byte> Buffer => _buffer;
    public Span<byte> Span => _buffer.Span;
    public int Position { get => _pos; set => _pos = value; }
    public int Remaining => _buffer.Length - _pos;
    public int Capacity => _buffer.Length;

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
}

public static class ZigZag
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong Encode(long value)
    {
        return unchecked((ulong)((value << 1) ^ (value >> 63)));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long Decode(ulong value)
    {
        return (long)((value >> 1) ^ (0UL - (value & 1UL)));
    }
}
