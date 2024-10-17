using System.Text;

namespace nKafka.Contracts.Primitives;

public static class PrimitiveSerializer
{
    private static readonly byte[] MinusOneShort = [ 0xff, 0xff ];
    public static readonly byte[] MinusOneVarInt = { 0x01 };
    
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
            throw new InvalidOperationException($"value is too long. Max length: {short.MaxValue}. Current value length: {length}");
        }
        SerializeShort(output, (short)length);
        
        output.SetLength(output.Length + length);
        Encoding.UTF8.GetBytes(value, 0, value.Length, output.GetBuffer(), (int) output.Position);
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
        if (value == null)
        {
            output.Write(MinusOneVarInt, 0, MinusOneVarInt.Length);
            return;
        }
        
        var length = Encoding.UTF8.GetByteCount(value);
        SerializeVarLong(output, length);
        
        output.SetLength(output.Length + length);
        Encoding.UTF8.GetBytes(value, 0, value.Length, output.GetBuffer(), (int) output.Position);
        output.Position += length;
    }

    public static string? DeserializeVarString(MemoryStream input)
    {
        var length = DeserializeVarLong(input);
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
            throw new InvalidOperationException($"value is too long. Max length: {int.MaxValue}. Current value length: {length}");
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
        output.WriteByte((byte) (value & 0xff));
    }
    
    public static short DeserializeShort(MemoryStream input)
    {
        if (input.Position + 2 > input.Length)
        {
            throw new InvalidOperationException(
                $"DeserializeShort needs 2 bytes but got only {input.Length - input.Position}");
        }

        return (short) ((input.ReadByte() << 8) | input.ReadByte());
    }

    public static void SerializeByte(MemoryStream output, byte? value)
    {
        output.WriteByte(value.Value);
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
    
    public static int DeserializeInt(MemoryStream input)
    {
        if (input.Position + 4 > input.Length)
        {
            throw new InvalidOperationException(
                $"DeserializeInt needs 4 bytes but got only {input.Length - input.Position}");
        }

        return input.ReadByte() << 3*8 | input.ReadByte() << 2*8 | input.ReadByte() << 8 | input.ReadByte();
    }

    public static void SerializeLong(MemoryStream output, long? value)
    {
        ulong ui = (ulong) value!.Value;
        for (int j = 7; j >= 0; j--)
            output.WriteByte((byte) (ui >> j*8 & 0xff));
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
    
    public static void SerializeVarLong(MemoryStream output, long? value)
    {
        var asZigZag = ToZigZag(value!.Value);

        // value & 1111 1111 ... 1000 0000 will zero the last 7 bytes,
        // if the result is zero, it means we only have those last 7 bytes
        // to write.
        while((asZigZag & 0xffffffffffffff80L) != 0L)
        {
            // keep only the 7 most significant bytes:
            // value = (value & 0111 1111)
            // and add a 1 in the most significant bit of the byte, meaning
            // it's not the last byte of the VarInt:
            // value = (value | 1000 0000)
            output.WriteByte((byte)((asZigZag & 0x7f) | 0x80));
            // Shift the 7 bits we just wrote to the stream and continue:
            asZigZag >>= 7;
        }
        output.WriteByte((byte)asZigZag);
    }

    public static long DeserializeVarLong(MemoryStream input)
    {
        ulong asZigZag = 0L; // Result value
        int i = 0; // Number of bits written
        long b; // Byte read

        // Check if the 8th bit of the byte is 1, meaning there will be more to read:
        // b & 1000 0000
        while (((b = input.ReadByte()) & 0x80) != 0) {
            // Take the 7 bits of the byte we want to add and insert them at the
            // right location (offset i)
            asZigZag |= (ulong)(b & 0x7f) << i;
            i += 7;
            if (i > 63)
                throw new OverflowException();
        }

        if (i == 63 && b != 0x01)
        {
            // We read 63 bits, we can only read one more (the most significant bit, MSB),
            // or it means that the VarInt can't fit in a long.
            // If the bit to read was 0, we would not have read it (as it's the MSB), thus, it must be 1.
            throw new OverflowException();
        }

        asZigZag |= (ulong)b << i;

        // The value is signed
        if ((asZigZag & 0x1) == 0x1)
        {
            return (-1 * ((long)(asZigZag >> 1) + 1));
        }


        return (long)(asZigZag >> 1);
    }
    
    private static ulong ToZigZag(long i)
    {
        return unchecked((ulong)((i << 1) ^ (i >> 63)));
    }

    public static void SerializeVarInt(MemoryStream output, int? value)
    {
        SerializeVarLong(output, value);
    }

    public static int DeserializeVarInt(MemoryStream input)
    {
        return checked((int)DeserializeVarLong(input));
    }
}