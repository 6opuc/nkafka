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
    
    public static void ValidateCrc32c(
        uint expectedCrc,
        MemoryStream stream,
        long start)
    {
        Validate(Crc32c.Calculate, expectedCrc, stream, start);
    }
    
    private static void Validate(
        Func<MemoryStream, long, long, uint> calculateChecksum,
        uint expectedChecksum,
        MemoryStream stream,
        long start,
        long size = -1)
    {
        if (size < 0)
        {
            size = stream.Position - start;
        }
        var calculatedChecksum = calculateChecksum(stream, start, size);
        if (calculatedChecksum != expectedChecksum)
        {
            throw new Exception($"Corrupt message: checksum does not match. Calculated {calculatedChecksum} but got {expectedChecksum}.");
        }
    }
}