using System.Buffers;
using System.IO.Pipelines;

namespace nKafka.Client;

public static class PipeReaderExtensions
{
    public static async ValueTask<int> ReadIntAsync(
        this PipeReader reader,
        CancellationToken cancellationToken = default)
    {
        #warning stackalloc if possible
        var buffer = new byte[4];
        var read = await reader.ReadAsync(buffer, 4, cancellationToken);
        if (read != buffer.Length)
        {
            throw new EndOfStreamException();
        }
        if (BitConverter.IsLittleEndian)
        {
            Array.Reverse(buffer);
        }
        return BitConverter.ToInt32(buffer);
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