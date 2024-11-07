// Copyright (c) Damien Guard.  All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Originally published at http://damieng.com/blog/2006/08/08/calculating_crc32_in_c_and_net

// The original code has been stripped of all non used parts and adapted to our use.

using System.Runtime.Intrinsics.X86;

#warning rewrite: 3 versions of Crc32
#warning try avx + vector 512

namespace nKafka.Contracts
{
    /// <summary>
    /// Implements a 32-bit CRC hash algorithm.
    /// </summary>
    internal class Crc32
    {
        private const uint DefaultPolynomial = 0xedb88320u;
        private const uint DefaultSeed = 0xffffffffu;

        private const uint CastagnoliPolynomial = 0x82F63B78u;
        private const uint CastagnoliSeed = DefaultSeed;

        private readonly uint _polynomial;
        private readonly uint _seed;
        private readonly uint[] _table;

        private static readonly Crc32 DefaultCrc32;
        private static readonly Crc32 CastagnoliCrc32;

        /// <summary>
        /// Compute the CRC-32 of the byte sequence using IEEE standard polynomial values.
        /// This is the regular "internet" CRC.
        /// </summary>
        /// <param name="stream">byte stream</param>
        /// <param name="start">start offset of the byte sequence</param>
        /// <param name="size">size of the byte sequence</param>
        /// <returns></returns>
        public static uint Compute(MemoryStream stream, long start, long size) =>
            DefaultCrc32.ComputeForStream(stream, start, size);

        /// <summary>
        /// Compute the CRC-32 of the byte sequence using Castagnoli polynomial values.
        /// This alternate CRC-32 is often used to compute the CRC as it is often yield
        /// better chances to detect errors in larger payloads.
        ///
        /// In particular, it is used to compute the CRC of a RecordBatch in newer versions
        /// of the Kafka protocol.
        /// </summary>
        /// <param name="stream">byte stream</param>
        /// <param name="start">start offset of the byte sequence</param>
        /// <param name="size">size of the byte sequence</param>
        /// <returns></returns>
        public static uint ComputeCastagnoli(MemoryStream stream, long start, long size) =>
            CastagnoliCrc32.ComputeForStream(stream, start, size);

        private readonly bool _x64Available;
        
        private readonly bool _sse42Available;
        
        private Crc32(uint polynomial, uint seed, bool tryUseHardwareCrc = false)
        {
            _polynomial = polynomial;
            _seed = seed;
            _table = InitializeTable();
            if (tryUseHardwareCrc)
            {
                _sse42Available = Sse42.IsSupported;
                _x64Available = Sse42.X64.IsSupported;
            }
        }

        static Crc32()
        {
            DefaultCrc32 = new Crc32(DefaultPolynomial, DefaultSeed);
            CastagnoliCrc32 = new Crc32(CastagnoliPolynomial, CastagnoliSeed, true);
        }

        private uint ComputeForStream(MemoryStream stream, long start, long size)
        {
            var crc = _seed;
            var buffer = stream.GetBuffer();
            
            if (_sse42Available)
            {
                if (_x64Available)
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
            }
            else
            {
                for (var i = start; i < start + size; ++i)
                    crc = (crc >> 8) ^ _table[buffer[i] ^ crc & 0xff];
            }

            return ~crc;
        }

        private uint[] InitializeTable()
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

        public static void CheckCrcCastagnoli(uint crc, MemoryStream stream, long crcStartPos, long crcLength = -1)
        {
            CheckCrc(CastagnoliCrc32, crc, stream, crcStartPos, crcLength);
        }

        public static void CheckCrc(uint crc, MemoryStream stream, long crcStartPos, long crcLength = -1)
        {
            CheckCrc(DefaultCrc32, crc, stream, crcStartPos, crcLength);
        }

        private static void CheckCrc(Crc32 crcAlgo, uint crc, MemoryStream stream, long crcStartPos, long crcLength = -1)
        {
            var length = crcLength == -1 ? stream.Position - crcStartPos : crcLength;
            var computedCrc = crcAlgo.ComputeForStream(stream, crcStartPos, length);
            if (computedCrc != crc)
            {
                throw new Exception($"Corrupt message: CRC32 does not match. Calculated {computedCrc} but got {crc}.");
            }
        }
    }
}

