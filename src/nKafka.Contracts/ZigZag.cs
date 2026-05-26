using System.Runtime.CompilerServices;

namespace nKafka.Contracts;

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
