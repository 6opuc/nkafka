namespace nKafka.Contracts;

public static class Crc32
{
    public static uint Calculate(ReadOnlySpan<byte> buffer, long start, long size)
    {
        return System.IO.Hashing.Crc32.HashToUInt32(buffer.Slice((int)start, (int)size));
    }
}
