using System.Net.Sockets;

namespace nKafka.Client;

public class SocketWriterStream : Stream
{
    private readonly Socket _socket;

    public SocketWriterStream(Socket socket)
    {
        _socket = socket;
    }

    public override void Flush()
    {
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        throw new NotImplementedException();
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotImplementedException();
    }

    public override void SetLength(long value)
    {
        throw new NotImplementedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        _socket.Send(buffer, offset, count, SocketFlags.None);
    }

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await _socket
                .SendAsync(buffer, SocketFlags.None, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (SocketException) when (!_socket.Connected)
        {
            throw new OperationCanceledException(cancellationToken);
        }
    }

    public override bool CanRead => false;

    public override bool CanSeek => false;

    public override bool CanWrite => true;

    public override long Length => 0;

    public override long Position { get; set; } = 0;
}