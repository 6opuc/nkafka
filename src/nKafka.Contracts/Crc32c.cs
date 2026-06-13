using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.X86;

namespace nKafka.Contracts;

public static class Crc32c
{
    private static readonly Func<ReadOnlySpan<byte>, long, long, uint> _calculateSpan;

    static Crc32c()
    {
        _calculateSpan = Crc32cHardwareImplementation.IsSupported
            ? Crc32cHardwareImplementation.CalculateSpan
            : Crc32cSoftwareImplementation.CalculateSpan;
    }

    public static uint Calculate(ReadOnlySpan<byte> buffer, long start, long size)
    {
        return _calculateSpan(buffer, start, size);
    }

    private static class Crc32cHardwareImplementation
    {
        private static readonly uint _seed = 0xffffffffu;
        private static readonly bool _sse42x64Available = Sse42.X64.IsSupported;
        private static readonly bool _sse42Available = Sse42.IsSupported;

        public static bool IsSupported => _sse42Available;

        public static uint CalculateSpan(ReadOnlySpan<byte> buffer, long start, long size)
        {
            if (!_sse42Available)
            {
                throw new PlatformNotSupportedException();
            }

            if (start < 0 || start > int.MaxValue)
            {
                throw new ArgumentOutOfRangeException(nameof(start), "Start position must be between 0 and int.MaxValue.");
            }

            var crc = _seed;
            var span = buffer.Slice((int)start, (int)size);
            var pos = 0;

            if (_sse42x64Available)
            {
                while (pos + 8 <= span.Length)
                {
                    crc = (uint)Sse42.X64.Crc32(crc, MemoryMarshal.Read<ulong>(span.Slice(pos)));
                    pos += 8;
                }
            }

            while (pos + 4 <= span.Length)
            {
                crc = Sse42.Crc32(crc, MemoryMarshal.Read<uint>(span.Slice(pos)));
                pos += 4;
            }

            while (pos < span.Length)
            {
                crc = Sse42.Crc32(crc, span[pos]);
                pos++;
            }

            return ~crc;
        }
    }

    private static class Crc32cSoftwareImplementation
    {
        private static readonly uint _polynomial = 0x82F63B78u;
        private static readonly uint _seed = 0xffffffffu;
        private static readonly uint[] _table = InitializeTable();

        private static uint[] InitializeTable()
        {
            var table = new uint[256];
            for (var i = 0; i < 256; i++)
            {
                var entry = (uint)i;
                for (var j = 0; j < 8; j++)
                {
                    if ((entry & 1) == 1)
                    {
                        entry = (entry >> 1) ^ _polynomial;
                    }
                    else
                    {
                        entry = entry >> 1;
                    }
                }

                table[i] = entry;
            }

            return table;
        }

        public static uint CalculateSpan(ReadOnlySpan<byte> buffer, long start, long size)
        {
            if (start < 0 || start > int.MaxValue)
            {
                throw new ArgumentOutOfRangeException(nameof(start), "Start position must be between 0 and int.MaxValue.");
            }

            var crc = _seed;
            var span = buffer.Slice((int)start, (int)size);
            var end = (int)size;
            for (var i = 0; i < end; i++)
            {
                crc = (crc >> 8) ^ _table[span[i] ^ crc & 0xff];
            }

            return ~crc;
        }
    }
}
