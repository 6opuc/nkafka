namespace nKafka.Contracts
{
    public static class Crc32
    {
        public static uint Calculate(MemoryStream stream, long start, long size)
        {
            var buffer = stream.GetBuffer().AsSpan((int)start, (int)size);
            return System.IO.Hashing.Crc32.HashToUInt32(buffer);
        }
    }
}

