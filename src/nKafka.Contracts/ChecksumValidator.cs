namespace nKafka.Contracts;

public static class ChecksumValidator
{
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
        ReadOnlyMemory<byte> buffer,
        long start,
        long size)
    {
        Validate(Crc32c.Calculate, expectedCrc, buffer.Span, start, size);
    }

    private static void Validate(
        Func<ReadOnlySpan<byte>, long, long, uint> calculateChecksumSpan,
        uint expectedChecksum,
        ReadOnlySpan<byte> buffer,
        long start,
        long size)
    {
        uint calculatedChecksum = calculateChecksumSpan(buffer, start, size);
        if (calculatedChecksum != expectedChecksum)
        {
            throw new Exception($"Corrupt message: checksum does not match. Calculated {calculatedChecksum} but got {expectedChecksum}.");
        }
    }
}
