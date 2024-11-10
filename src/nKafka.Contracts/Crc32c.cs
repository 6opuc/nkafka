using System.Runtime.Intrinsics.X86;

namespace nKafka.Contracts;

public static class Crc32c
{
    private static readonly Func<MemoryStream, long, long, uint> _calculate;

    static Crc32c()
    {
        _calculate = Crc32cHardwareImplementation.IsSupported
            ? Crc32cHardwareImplementation.Calculate
            : Crc32cSoftwareImplementation.Calculate;
    }
    
    public static uint Calculate(MemoryStream stream, long start, long size)
    {
        return _calculate(stream, start, size);
    }

    private static class Crc32cHardwareImplementation
    {
        private static readonly uint _seed = 0xffffffffu;
        private static readonly bool _sse42x64Available = Sse42.X64.IsSupported;
        private static readonly bool _sse42Available = Sse42.IsSupported;

        public static bool IsSupported => _sse42Available;
        
        public static uint Calculate(MemoryStream stream, long start, long size)
        {
            if (!_sse42Available)
            {
                throw new PlatformNotSupportedException();
            }
            
            var crc = _seed;
            var buffer = stream.GetBuffer();

            if (_sse42x64Available)
            {
                while (size >= 8)
                {
                    crc = (uint)Sse42.X64.Crc32(crc, BitConverter.ToUInt64(buffer, (int)start));
                    start += 8;
                    size -= 8;
                }
            }
            
            while (size >= 4)
            {
                crc = Sse42.Crc32(crc, BitConverter.ToUInt32(buffer, (int)start));
                start += 4;
                size -= 4;
            }
            
            while (size > 0)
            {
                crc = Sse42.Crc32(crc, buffer[start]);
                start++;
                size--;
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
                    if ((entry & 1) == 1)
                        entry = (entry >> 1) ^ _polynomial;
                    else
                        entry = entry >> 1;
                table[i] = entry;
            }

            return table;
        }
        
        public static uint Calculate(MemoryStream stream, long start, long size)
        {
            var crc = _seed;
            var buffer = stream.GetBuffer();
            for (var i = start; i < start + size; ++i)
                crc = (crc >> 8) ^ _table[buffer[i] ^ crc & 0xff];
            return ~crc;
        }
    }
}