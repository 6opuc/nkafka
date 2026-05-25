using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;
using System.Runtime.InteropServices;

namespace nKafka.Contracts;

public static class Crc32c
{
    private static readonly Func<MemoryStream, long, long, uint> _calculateStream;
    private static readonly Func<ReadOnlySpan<byte>, long, long, uint> _calculateSpan;

    static Crc32c()
    {
        _calculateStream = Crc32cHardwareImplementation.IsSupported
            ? Crc32cHardwareImplementation.CalculateStream
            : Crc32cSoftwareImplementation.CalculateStream;
        _calculateSpan = Crc32cHardwareImplementation.IsSupported
            ? Crc32cHardwareImplementation.CalculateSpan
            : Crc32cSoftwareImplementation.CalculateSpan;
    }
    
    public static uint Calculate(MemoryStream stream, long start, long size)
    {
        return _calculateStream(stream, start, size);
    }
    
    public static uint Calculate(ReadOnlyMemory<byte> buffer, long start, long size)
    {
        return _calculateSpan(buffer.Span, start, size);
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
        
        public static uint CalculateStream(MemoryStream stream, long start, long size)
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
            var buffer = stream.GetBuffer();
            var end = start + size;
            if (end > int.MaxValue)
            {
                throw new ArgumentOutOfRangeException(nameof(size), "End position exceeds buffer bounds.");
            }

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
                    if ((entry & 1) == 1)
                        entry = (entry >> 1) ^ _polynomial;
                    else
                        entry = entry >> 1;
                table[i] = entry;
            }

            return table;
        }
        
        public static uint CalculateStream(MemoryStream stream, long start, long size)
        {
            if (start < 0 || start > int.MaxValue)
            {
                throw new ArgumentOutOfRangeException(nameof(start), "Start position must be between 0 and int.MaxValue.");
            }
            
            var crc = _seed;
            var buffer = stream.GetBuffer();
            var end = start + size;
            if (end > int.MaxValue)
            {
                throw new ArgumentOutOfRangeException(nameof(size), "End position exceeds buffer bounds.");
            }
            
            for (var i = start; i < end; ++i)
                crc = (crc >> 8) ^ _table[buffer[i] ^ crc & 0xff];
            return ~crc;
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
                crc = (crc >> 8) ^ _table[span[i] ^ crc & 0xff];
            return ~crc;
        }
    }
}
