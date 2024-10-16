using System.Text;

namespace nKafka.Contracts.Primitives;

public static class PrimitiveSerializer
{
    private static readonly byte[] MinusOneShort = [ 0xff, 0xff ];
    
    public static void SerializeString(MemoryStream output, string? value)
    {
        if (value == null)
        {
            output.Write(MinusOneShort, 0, MinusOneShort.Length);
            return;
        }

        var bytes = Encoding.UTF8.GetBytes(value);

        if (bytes.Length > short.MaxValue)
        {
            throw new InvalidOperationException($"value is too long. Max length: {short.MaxValue}. Current value length: {bytes.Length}");
        }

        SerializeShort(output, (short)bytes.Length);
        output.Write(bytes, 0, bytes.Length);
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

    public static void SerializeShort(MemoryStream output, short value)
    {
        output.WriteByte((byte)(value >> 8));
        output.WriteByte((byte)value);
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
}