using System.Text;

namespace nKafka.Contracts;

public static class PrimitiveSerializer
{
    private static readonly byte[] MinusOneShort = [0xff, 0xff];
    private static readonly byte[] MinusOneVarInt = { 0x01 };
    private const byte ZeroByte = 0x00;
    private const byte OneByte = 0x01;

    public static void SerializeString(MemoryStream output, string? value)
    {
        if (value == null)
        {
            output.Write(MinusOneShort, 0, MinusOneShort.Length);
            return;
        }

        var length = Encoding.UTF8.GetByteCount(value);
        if (length > short.MaxValue)
        {
            throw new InvalidOperationException(
                $"value is too long. Max length: {short.MaxValue}. Current value length: {length}");
        }

        SerializeShort(output, (short)length);

        var diff = output.Length - output.Position - length;
        if (diff < 0)
        {
            output.SetLength(output.Length - diff);
        }

        Encoding.UTF8.GetBytes(value, 0, value.Length, output.GetBuffer(), (int)output.Position);
        output.Position += length;
    }

    public static string? DeserializeString(MemoryStream input)
    {
        var length = DeserializeShort(input);
        if (length == -1)
        {
            return null;
        }

        if (length == 0)
        {
            return string.Empty;
        }

        if (input.Position + length > input.Length)
        {
            throw new InvalidOperationException(
                $"DeserializeString needs {length} bytes but got only {input.Length - input.Position}");
        }

        var value = Encoding.UTF8.GetString(input.GetBuffer(), (int)input.Position, length);
        input.Position += length;
        return value;
    }

    public static void SerializeVarString(MemoryStream output, string? value)
    {
        var length = value == null
            ? -1
            : Encoding.UTF8.GetByteCount(value);
        SerializeLengthLong(output, length);
        if (value == null)
        {
            return;
        }

        var diff = output.Length - output.Position - length;
        if (diff < 0)
        {
            output.SetLength(output.Length - diff);
        }
        Encoding.UTF8.GetBytes(value, 0, value.Length, output.GetBuffer(), (int)output.Position);
        output.Position += length;
    }

    public static string? DeserializeVarString(MemoryStream input)
    {
        var length = DeserializeLengthLong(input);
        if (length == -1)
        {
            return null;
        }

        if (length == 0)
        {
            return string.Empty;
        }

        if (length > int.MaxValue)
        {
            throw new InvalidOperationException(
                $"value is too long. Max length: {int.MaxValue}. Current value length: {length}");
        }

        if (input.Position + length > input.Length)
        {
            throw new InvalidOperationException(
                $"DeserializeVarString needs {length} bytes but got only {input.Length - input.Position}");
        }

        var value = Encoding.UTF8.GetString(input.GetBuffer(), (int)input.Position, (int)length);
        input.Position += length;
        return value;
    }

    public static void SerializeShort(MemoryStream output, short? value)
    {
        SerializeIntAsByte(output, value!.Value >> 8);
        SerializeIntAsByte(output, value!.Value);
    }

    private static void SerializeIntAsByte(MemoryStream output, int value)
    {
        output.WriteByte((byte)(value & 0xff));
    }

    public static short DeserializeShort(MemoryStream input)
    {
        if (input.Position + 2 > input.Length)
        {
            throw new InvalidOperationException(
                $"DeserializeShort needs 2 bytes but got only {input.Length - input.Position}");
        }

        return (short)((input.ReadByte() << 8) | input.ReadByte());
    }

    public static void SerializeUshort(MemoryStream output, ushort? value)
    {
        SerializeShort(output, (short)value!.Value);
    }

    public static ushort DeserializeUshort(MemoryStream input)
    {
        return (ushort)DeserializeShort(input);
    }

    public static void SerializeByte(MemoryStream output, byte? value)
    {
        output.WriteByte(value!.Value);
    }

    public static byte DeserializeByte(MemoryStream input)
    {
        return (byte)input.ReadByte();
    }

    public static void SerializeInt(MemoryStream output, int? value)
    {
        SerializeIntAsByte(output, value!.Value >> 8 * 3);
        SerializeIntAsByte(output, value!.Value >> 8 * 2);
        SerializeIntAsByte(output, value!.Value >> 8);
        SerializeIntAsByte(output, value!.Value);
    }

    public static void SerializeUint(MemoryStream output, uint? value)
    {
        SerializeInt(output, (int)value!.Value);
    }

    public static int DeserializeInt(MemoryStream input)
    {
        if (input.Position + 4 > input.Length)
        {
            throw new InvalidOperationException(
                $"DeserializeInt needs 4 bytes but got only {input.Length - input.Position}");
        }

        return input.ReadByte() << 3 * 8 | input.ReadByte() << 2 * 8 | input.ReadByte() << 8 | input.ReadByte();
    }

    public static uint DeserializeUint(MemoryStream input)
    {
        return (uint)DeserializeInt(input);
    }
    
    public static void SerializeLong(MemoryStream output, long? value)
    {
        ulong ui = (ulong)value!.Value;
        for (int j = 7; j >= 0; j--)
            output.WriteByte((byte)(ui >> j * 8 & 0xff));
    }

    public static long DeserializeLong(MemoryStream input)
    {
        if (input.Position + 8 > input.Length)
        {
            throw new Exception($"DeserializeLong needs 8 bytes but got only {input.Length - input.Position}");
        }

        var value = 0L;
        for (var i = 0; i < 8; i++)
        {
            value = value << 8 | (uint)input.ReadByte();
        }

        return value;
    }

    public static void SerializeBool(MemoryStream output, bool? value)
    {
        output.WriteByte(value == true ? OneByte : ZeroByte);
    }

    public static bool DeserializeBool(MemoryStream input)
    {
        return input.ReadByte() != ZeroByte;
    }

    public static void SerializeVarLong(MemoryStream output, long? value)
    {
        var asZigZag = ToZigZag(value!.Value);
        SerializeUVarLong(output, asZigZag);
    }

    public static void SerializeUVarLong(MemoryStream output, ulong? value)
    {
        do
        {
            // Take 7 bits
            var byteValue = value & 0x7f;
            // Remove 7 bits
            value >>= 7;

            // Value should be encoded to more than one byte?
            if (value > 0)
            {
                // Add 1 to most significant bit to indicate more bytes will follow
                byteValue |= 128;
            }

            output.WriteByte((byte)byteValue!);
        } while (value > 0);
    }
    
    public static void SerializeLengthLong(MemoryStream output, long value) =>
        SerializeUVarLong(output, (ulong)(value + 1));

    public static long DeserializeVarLong(MemoryStream input)
    {
        return FromZigZag(DeserializeUVarLong(input));
    }
    
    private static long FromZigZag(this ulong value)
    {
        return unchecked((long)((value >> 1) - (value & 1) * value));
    }

    public static ulong DeserializeUVarLong(MemoryStream input)
    {
        var more = true;
        ulong value = 0;
        var shift = 0;
        while (more)
        {
            var lowerBits = DeserializeByte(input);

            more = (lowerBits & 128) != 0;
            value |= (uint)((lowerBits & 0x7f) << shift);
            shift += 7;
        }

        return value;
    }
    
    public static long DeserializeLengthLong(MemoryStream input) =>
        (long)DeserializeUVarLong(input) - 1;

    private static ulong ToZigZag(long i)
    {
        return unchecked((ulong)((i << 1) ^ (i >> 63)));
    }

    public static void SerializeVarInt(MemoryStream output, int? value)
    {
        SerializeVarLong(output, value);
    }
    
    public static void SerializeUVarInt(MemoryStream output, uint? value)
    {
        SerializeUVarLong(output, value);
    }
    
    public static void SerializeLength(MemoryStream output, int value)
    {
        SerializeLengthLong(output, value);
    }

    public static int DeserializeVarInt(MemoryStream input)
    {
        return checked((int)DeserializeVarLong(input));
    }

    public static uint DeserializeUVarInt(MemoryStream input)
    {
        return checked((uint)DeserializeUVarLong(input));
    }
    
    public static int DeserializeLength(MemoryStream input)
    {
        return checked((int)DeserializeLengthLong(input));
    }

    public static void SerializeGuid(MemoryStream output, Guid? value)
    {
        var availableSize = output.Length - output.Position;
        var diff = 16 - availableSize;
        if (diff > 0)
        {
            output.SetLength(output.Length + diff);
        }

        value!.Value.TryWriteBytes(output.GetBuffer().AsSpan()[(int)output.Position..]);
        output.Position += 16;
    }

    public static Guid DeserializeGuid(MemoryStream input)
    {
        if (input.Position + 16 > input.Length)
        {
            throw new Exception($"DeserializeGuid needs 16 bytes but got only {input.Length - input.Position}");
        }

        var bytes = input.GetBuffer().AsSpan()[(int)input.Position..((int)input.Position + 16)];
        input.Position += 16;
        return new Guid(bytes);
    }
}