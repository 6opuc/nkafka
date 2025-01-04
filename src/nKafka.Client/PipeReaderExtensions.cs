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
            
            #warning System.InvalidOperationException : Reading is already in progress.
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
                return
                    (buffer[0] << 24) |
                    (buffer[1] << 16) |
                    (buffer[2] << 8) |
                    buffer[3];
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

        do
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            var readResult = await reader.ReadAtLeastAsync(length, cancellationToken);
            if (readResult.Buffer.Length == 0)
            {
                continue;
            }
            
            var buffer = readResult.Buffer.Slice(0, length);
            buffer.CopyTo(output.AsSpan());
            reader.AdvanceTo(buffer.GetPosition(length));

            return length;
        } while (true);
    }
}