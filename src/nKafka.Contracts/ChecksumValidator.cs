namespace nKafka.Contracts;

public static class ChecksumValidator
{
    public static void ValidateCrc32(
        uint expectedCrc,
        MemoryStream stream,
        long start)
    {
        Validate(Crc32.Calculate, expectedCrc, stream, start);
    }
    
    public static void ValidateCrc32(
        uint expectedCrc,
        ReadOnlyMemory<byte> buffer,
        long start,
        long size)
    {
        Validate(Crc32.Calculate, expectedCrc, buffer.Span, start, size);
    }
    
    public static void ValidateCrc32c(
        uint expectedCrc,
        MemoryStream stream,
        long start)
    {
        Validate(Crc32c.Calculate, expectedCrc, stream, start);
    }
    
    public static void ValidateCrc32c(
        uint expectedCrc,
        ReadOnlyMemory<byte> buffer,
        long start,
        long size)
    {
        Validate(Crc32c.Calculate, expectedCrc, buffer.Span, start, size);
    }
    
    private static void Validate(
        Func<MemoryStream, long, long, uint> calculateChecksumStream,
        uint expectedChecksum,
        MemoryStream stream,
        long start,
        long size = -1)
    {
        if (size < 0)
        {
            size = stream.Position - start;
        }
        var calculatedChecksum = calculateChecksumStream(stream, start, size);
        if (calculatedChecksum != expectedChecksum)
        {
            throw new Exception($"Corrupt message: checksum does not match. Calculated {calculatedChecksum} but got {expectedChecksum}.");
        }
    }
    
    private static void Validate(
        Func<ReadOnlySpan<byte>, long, long, uint> calculateChecksumSpan,
        uint expectedChecksum,
        ReadOnlySpan<byte> buffer,
        long start,
        long size)
    {
        var calculatedChecksum = calculateChecksumSpan(buffer, start, size);
        if (calculatedChecksum != expectedChecksum)
        {
            throw new Exception($"Corrupt message: checksum does not match. Calculated {calculatedChecksum} but got {expectedChecksum}.");
        }
    }
}
