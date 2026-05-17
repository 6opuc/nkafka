using System.Net.Sockets;

namespace nKafka.Client;

public class SocketWriterStream : Stream
{
    private readonly Stream _stream;

    public SocketWriterStream(Stream stream)
    {
        _stream = stream;
    }

    public static SocketWriterStream FromSocket(Socket socket)
    {
        return new SocketWriterStream(new NetworkStream(socket));
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
        _stream.Write(buffer, offset, count);
    }

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        await _stream.WriteAsync(buffer, cancellationToken);
    }

    public override bool CanRead => false;

    public override bool CanSeek => false;

    public override bool CanWrite => true;

    public override long Length => 0;

    public override long Position { get; set; } = 0;
}
