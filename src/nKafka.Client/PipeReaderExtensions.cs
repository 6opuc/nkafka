using System.Buffers;
using System.IO.Pipelines;

namespace nKafka.Client;

public static class PipeReaderExtensions
{
    public static async ValueTask<int> ReadIntAsync(
        this PipeReader reader,
        CancellationToken cancellationToken = default)
    {
        var bytes = await reader.ReadAsync(4, cancellationToken);
        if (BitConverter.IsLittleEndian)
        {
            Array.Reverse(bytes);
        }
        return BitConverter.ToInt32(bytes);
    }
    
    public static async ValueTask<byte[]> ReadAsync(
        this PipeReader reader,
        int length,
        CancellationToken cancellationToken = default)
    {
        if (length <= 0)
        {
            return Array.Empty<byte>();
        }

        #warning consider buffer pool
        var bytes = new byte[length];
        var writtenCount = 0;

        ReadResult result;
        do
        {
            result = await reader.ReadAsync(cancellationToken)
                .ConfigureAwait(false);
            var buffer = result.Buffer.Slice(
                0, Math.Min(length - writtenCount, result.Buffer.Length));
            buffer.CopyTo(bytes.AsSpan()[writtenCount..]);
            writtenCount += (int)buffer.Length;
            reader.AdvanceTo(buffer.GetPosition(buffer.Length));

            if (writtenCount == length)
            {
                return bytes;
            }
        } while (!result.IsCanceled && !result.IsCompleted);

        if (writtenCount == 0)
        {
            reader.Complete();
            throw new OperationCanceledException(cancellationToken);
        }

        var exception = new OperationCanceledException(
            $"Expected {length} bytes, got {writtenCount}",
            cancellationToken);
        reader.Complete(exception);
        throw exception;
    }
}