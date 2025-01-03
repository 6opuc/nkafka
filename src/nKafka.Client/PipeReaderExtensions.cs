using System.Buffers;
using System.IO.Pipelines;

namespace nKafka.Client;

public static class PipeReaderExtensions
{
    public static async ValueTask<int> ReadIntAsync(
        this PipeReader reader,
        CancellationToken cancellationToken = default)
    {
        do
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            var readResult = await reader.ReadAtLeastAsync(4, cancellationToken);
            if (readResult.Buffer.Length == 0)
            {
                continue;
            }

            return ConvertToInt();

            // Span<T> in async method hack
            int ConvertToInt()
            {
                Span<byte> buffer = stackalloc byte[4];
                readResult.Buffer.Slice(0, 4).CopyTo(buffer);
                reader.AdvanceTo(readResult.Buffer.GetPosition(buffer.Length));
                if (BitConverter.IsLittleEndian)
                {
                    buffer.Reverse();
                }

                return BitConverter.ToInt32(buffer);
            }
        } while (true);
    }
    
    public static async ValueTask<int> ReadAsync(
        this PipeReader reader,
        byte[] output,
        int length,
        CancellationToken cancellationToken = default)
    {
        if (length <= 0)
        {
            return 0;
        }

        var readCount = 0;

        ReadResult result;
        do
        {
            result = await reader.ReadAsync(cancellationToken)
                .ConfigureAwait(false);
            var buffer = result.Buffer.Slice(
                0, Math.Min(length - readCount, result.Buffer.Length));
            buffer.CopyTo(output.AsSpan()[readCount..]);
            readCount += (int)buffer.Length;
            reader.AdvanceTo(buffer.GetPosition(buffer.Length));

            if (readCount == length)
            {
                return readCount;
            }
        } while (!result.IsCanceled && !result.IsCompleted);

        return readCount;
    }
}