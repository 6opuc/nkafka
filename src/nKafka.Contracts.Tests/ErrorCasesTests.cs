using FluentAssertions;
using nKafka.Contracts;
using NUnit.Framework;
using System.Text;

namespace nKafka.Contracts.Tests;

[TestFixture]
public class ErrorCasesTests
{
    private const int BufferSize = 1024;
    private byte[] _buffer = null!;

    #region Bounds Checking Tests

    [Test]
    public void BufferWriter_WriteByte_ThrowsWhenBufferFull()
    {
        var smallBuffer = new byte[1];
        var writer = new BufferWriter(smallBuffer);
        writer.WriteByte(42);
        
        Assert.Throws<InvalidOperationException>(() => writer.WriteByte(43));
    }

    [Test]
    public void BufferWriter_WriteInt_ThrowsWhenInsufficientSpace()
    {
        var smallBuffer = new byte[2];
        var writer = new BufferWriter(smallBuffer);
        
        Assert.Throws<InvalidOperationException>(() => writer.WriteInt(42));
    }

    [Test]
    public void BufferWriter_WriteLong_ThrowsWhenInsufficientSpace()
    {
        var smallBuffer = new byte[4];
        var writer = new BufferWriter(smallBuffer);
        
        Assert.Throws<InvalidOperationException>(() => writer.WriteLong(42));
    }

    [Test]
    public void BufferWriter_Advance_ThrowsWhenExceedingCapacity()
    {
        var buffer = new byte[10];
        var writer = new BufferWriter(buffer);
        
        Assert.Throws<InvalidOperationException>(() => writer.Advance(15));
    }

    #endregion

    #region Incomplete VarInt Handling Tests

    [Test]
    public void BufferReader_ReadVarInt_ThrowsOnIncompleteData()
    {
        _buffer = new byte[10];
        _buffer[0] = 0x80;
        _buffer[1] = 0x80;
        _buffer[2] = 0x80;
        _buffer[3] = 0x80;
        _buffer[4] = 0x80;
        _buffer[5] = 0x80;
        _buffer[6] = 0x80;
        _buffer[7] = 0x80;
        _buffer[8] = 0x80;
        _buffer[9] = 0x80;
        
        var reader = new BufferReader(_buffer);
        
        Assert.Throws<EndOfStreamException>(() => { reader.ReadVarInt(); });
    }

    [Test]
    public void BufferReader_ReadVarLong_ThrowsOnIncompleteData()
    {
        _buffer = new byte[10];
        _buffer[0] = 0x80;
        _buffer[1] = 0x80;
        _buffer[2] = 0x80;
        _buffer[3] = 0x80;
        _buffer[4] = 0x80;
        _buffer[5] = 0x80;
        _buffer[6] = 0x80;
        _buffer[7] = 0x80;
        _buffer[8] = 0x80;
        _buffer[9] = 0x80;
        
        var reader = new BufferReader(_buffer);
        
        Assert.Throws<EndOfStreamException>(() => { reader.ReadVarLong(); });
    }

    [Test]
    public void BufferReader_ReadUVarLong_ThrowsOnIncompleteData()
    {
        _buffer = new byte[10];
        _buffer[0] = 0x80;
        _buffer[1] = 0x80;
        _buffer[2] = 0x80;
        _buffer[3] = 0x80;
        _buffer[4] = 0x80;
        _buffer[5] = 0x80;
        _buffer[6] = 0x80;
        _buffer[7] = 0x80;
        _buffer[8] = 0x80;
        _buffer[9] = 0x80;
        
        var reader = new BufferReader(_buffer);
        
        Assert.Throws<EndOfStreamException>(() => { reader.ReadUVarLong(); });
    }

    [Test]
    public void BufferReader_ReadVarInt_CompletesWithValidData()
    {
        _buffer = new byte[10];
        _buffer[0] = 0xFF;
        _buffer[1] = 0xFF;
        _buffer[2] = 0xFF;
        _buffer[3] = 0xFF;
        _buffer[4] = 0xFF;
        _buffer[5] = 0xFF;
        _buffer[6] = 0xFF;
        _buffer[7] = 0xFF;
        _buffer[8] = 0xFF;
        _buffer[9] = 0x01;
        
        var reader = new BufferReader(_buffer);
        var result = reader.ReadVarInt();
        result.Should().Be(-1);
    }

    #endregion

    #region Buffer Overflow Tests

    [Test]
    public void BufferWriter_WriteByte_AtMaxCapacity()
    {
        var smallBuffer = new byte[1];
        var writer = new BufferWriter(smallBuffer);
        writer.WriteByte(42);
        writer.Position.Should().Be(1);
    }

    [Test]
    public void BufferWriter_WriteInt_ExactlyAtCapacity()
    {
        var buffer = new byte[4];
        var writer = new BufferWriter(buffer);
        writer.WriteInt(0x12345678);
        writer.Position.Should().Be(4);
    }

    [Test]
    public void BufferWriter_WriteLong_ExactlyAtCapacity()
    {
        var buffer = new byte[8];
        var writer = new BufferWriter(buffer);
        writer.WriteLong(0x123456789ABCDEF0L);
        writer.Position.Should().Be(8);
    }

    #endregion

    #region Empty Buffer Tests

    [Test]
    public void BufferReader_ReadByte_FromEmptyBuffer()
    {
        var emptyBuffer = new byte[0];
        var reader = new BufferReader(emptyBuffer);
        
        Assert.Throws<InvalidOperationException>(() => { reader.ReadByte(); });
    }

    [Test]
    public void BufferReader_ReadInt_FromEmptyBuffer()
    {
        var emptyBuffer = new byte[0];
        var reader = new BufferReader(emptyBuffer);
        
        Assert.Throws<InvalidOperationException>(() => { reader.ReadInt32BigEndian(); });
    }

    [Test]
    public void BufferReader_ReadVarInt_FromEmptyBuffer()
    {
        var emptyBuffer = new byte[0];
        var reader = new BufferReader(emptyBuffer);
        
        Assert.Throws<EndOfStreamException>(() => { reader.ReadVarInt(); });
    }

    #endregion

    #region Invalid Advance Tests

    [Test]
    public void BufferWriter_Advance_WithNegativeValue()
    {
        var buffer = new byte[10];
        var writer = new BufferWriter(buffer);
        
        Assert.Throws<InvalidOperationException>(() => writer.Advance(-1));
    }

    [Test]
    public void BufferReader_Advance_WithNegativeValue()
    {
        var buffer = new byte[10];
        var reader = new BufferReader(buffer);
        
        Assert.Throws<InvalidOperationException>(() => reader.Advance(-1));
    }

    [Test]
    public void BufferReader_Advance_ExceedingBuffer()
    {
        var buffer = new byte[10];
        var reader = new BufferReader(buffer);
        
        Assert.Throws<InvalidOperationException>(() => reader.Advance(15));
    }

    #endregion

    #region String Tests

    [Test]
    public void BufferWriter_WriteString_NullValue()
    {
        var buffer = new byte[10];
        var writer = new BufferWriter(buffer);
        writer.WriteString(null);
        writer.Position.Should().Be(2);
        buffer[0].Should().Be(0xFF);
        buffer[1].Should().Be(0xFF);
    }

    [Test]
    public void BufferWriter_WriteVarString_NullValue()
    {
        var buffer = new byte[10];
        var writer = new BufferWriter(buffer);
        writer.WriteVarString(null);
        writer.Position.Should().Be(1);
        buffer[0].Should().Be(0xFF);
    }

    [Test]
    public void BufferReader_ReadString_NullValue()
    {
        var buffer = new byte[10];
        buffer[0] = 0xFF;
        buffer[1] = 0xFF;
        var reader = new BufferReader(buffer);
        reader.ReadString().Should().BeNull();
    }

    [Test]
    public void BufferReader_ReadVarString_NullValue()
    {
        var buffer = new byte[10];
        buffer[0] = 0xFF;
        var reader = new BufferReader(buffer);
        reader.ReadVarString().Should().BeNull();
    }

    #endregion

    #region Guid Tests

    [Test]
    public void BufferWriter_WriteGuid_NullValue()
    {
        var buffer = new byte[20];
        var writer = new BufferWriter(buffer);
        writer.WriteGuid((Guid?)null);
        writer.Position.Should().Be(16);
        for (int i = 0; i < 16; i++)
            buffer[i].Should().Be(0);
    }

    [Test]
    public void BufferReader_ReadGuid_FromBuffer()
    {
        var guid = Guid.NewGuid();
        guid.TryWriteBytes(_buffer);
        var reader = new BufferReader(_buffer);
        reader.ReadGuid().Should().Be(guid);
    }

    #endregion

    #region Write Span Tests

    [Test]
    public void BufferWriter_WriteSpan_Empty()
    {
        var buffer = new byte[10];
        var writer = new BufferWriter(buffer);
        writer.Write(ReadOnlySpan<byte>.Empty);
        writer.Position.Should().Be(0);
    }

    [Test]
    public void BufferWriter_WriteSpan_NonEmpty()
    {
        var buffer = new byte[10];
        var writer = new BufferWriter(buffer);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        writer.Write(data.AsSpan());
        writer.Position.Should().Be(5);
        for (int i = 0; i < 5; i++)
            buffer[i].Should().Be((byte)(i + 1));
    }

    #endregion

    #region Read Memory Tests

    [Test]
    public void BufferReader_ReadMemory_NegativeLength()
    {
        var buffer = new byte[10];
        var reader = new BufferReader(buffer);
        
        Assert.Throws<InvalidOperationException>(() => { reader.ReadMemory(-1); });
    }

    [Test]
    public void BufferReader_ReadMemory_ValidLength()
    {
        var buffer = new byte[10];
        for (int i = 0; i < 10; i++)
            buffer[i] = (byte)i;
        var reader = new BufferReader(buffer);
        var result = reader.ReadMemory(5);
        result.Length.Should().Be(5);
        for (int i = 0; i < 5; i++)
            result.Span[i].Should().Be((byte)i);
    }

    #endregion

    #region Length Tests

    [Test]
    public void BufferWriter_WriteLength_Zero()
    {
        var buffer = new byte[10];
        var writer = new BufferWriter(buffer);
        writer.WriteLength(0);
        writer.Position.Should().Be(1);
        buffer[0].Should().Be(0x01);
    }

    [Test]
    public void BufferWriter_WriteLength_PositiveValue()
    {
        var buffer = new byte[10];
        var writer = new BufferWriter(buffer);
        writer.WriteLength(100);
        writer.Position.Should().BeGreaterThan(1);
    }

    [Test]
    public void BufferReader_ReadLength_ZeroValue()
    {
        var buffer = new byte[10];
        buffer[0] = 0x01;
        var reader = new BufferReader(buffer);
        reader.ReadLength().Should().Be(0);
    }

    [Test]
    public void BufferReader_ReadLength_FFValue()
    {
        var buffer = new byte[10];
        buffer[0] = 0xFF;
        var reader = new BufferReader(buffer);
        reader.ReadLength().Should().Be(-1);
    }

    #endregion

    #region Bool Tests

    [Test]
    public void BufferWriter_WriteBool_True()
    {
        var buffer = new byte[10];
        var writer = new BufferWriter(buffer);
        writer.WriteBool(true);
        buffer[0].Should().Be(1);
    }

    [Test]
    public void BufferWriter_WriteBool_False()
    {
        var buffer = new byte[10];
        var writer = new BufferWriter(buffer);
        writer.WriteBool(false);
        buffer[0].Should().Be(0);
    }

    [Test]
    public void BufferWriter_WriteBool_Null()
    {
        var buffer = new byte[10];
        var writer = new BufferWriter(buffer);
        writer.WriteBool((bool?)null);
        buffer[0].Should().Be(0);
    }

    [Test]
    public void BufferReader_ReadBool_True()
    {
        var buffer = new byte[10];
        buffer[0] = 1;
        var reader = new BufferReader(buffer);
        reader.ReadBool().Should().BeTrue();
    }

    [Test]
    public void BufferReader_ReadBool_False()
    {
        var buffer = new byte[10];
        buffer[0] = 0;
        var reader = new BufferReader(buffer);
        reader.ReadBool().Should().BeFalse();
    }

    #endregion

    #region Reset Tests

    [Test]
    public void BufferWriter_Reset_ClearsPosition()
    {
        var buffer = new byte[10];
        var writer = new BufferWriter(buffer);
        writer.WriteInt(42);
        writer.WriteLong(99);
        writer.Reset();
        writer.Position.Should().Be(0);
        writer.WriteInt(123);
        buffer[0].Should().Be(0);
        buffer[1].Should().Be(0);
        buffer[2].Should().Be(0);
        buffer[3].Should().Be(123);
    }

    #endregion

    #region Dispose Tests

    [Test]
    public void BufferWriter_Dispose_WithPooledArray()
    {
        var pool = System.Buffers.ArrayPool<byte>.Shared;
        using (var writer = new BufferWriter(pool, 100))
        {
            writer.WriteInt(42);
            writer.Position.Should().Be(4);
        }
    }

    [Test]
    public void BufferWriter_Dispose_WithMemoryBuffer()
    {
        var buffer = new byte[100];
        using (var writer = new BufferWriter(buffer))
        {
            writer.WriteInt(42);
            writer.Position.Should().Be(4);
        }
    }

    #endregion

    #region CreateChild Tests

    [Test]
    public void BufferReader_CreateChild_WithValidLength()
    {
        var buffer = new byte[20];
        for (int i = 0; i < 20; i++)
            buffer[i] = (byte)i;
        var reader = new BufferReader(buffer);
        var child = reader.CreateChild(10);
        child.Remaining.Should().Be(10);
        reader.Position.Should().Be(10);
    }

    [Test]
    public void BufferReader_CreateChild_ReadsCorrectData()
    {
        var buffer = new byte[20];
        for (int i = 0; i < 20; i++)
            buffer[i] = (byte)i;
        var reader = new BufferReader(buffer);
        var child = reader.CreateChild(4);
        child.ReadInt32BigEndian().Should().Be(0);
    }

    #endregion

    #region Span Tests

    [Test]
    public void BufferWriter_Span_ReturnsValidSpan()
    {
        var buffer = new byte[10];
        var writer = new BufferWriter(buffer);
        var span = writer.Span;
        span.Length.Should().Be(10);
        span[0] = 42;
        buffer[0].Should().Be(42);
    }

    [Test]
    public void BufferWriter_Buffer_ReturnsValidMemory()
    {
        var buffer = new byte[10];
        var writer = new BufferWriter(buffer);
        var mem = writer.Buffer;
        mem.Length.Should().Be(10);
    }

    #endregion

    #region ReadSpan Tests

    [Test]
    public void BufferReader_ReadSpan_ValidLength()
    {
        var buffer = new byte[10];
        for (int i = 0; i < 10; i++)
            buffer[i] = (byte)i;
        var reader = new BufferReader(buffer);
        var span = reader.ReadSpan(5);
        span.Length.Should().Be(5);
        for (int i = 0; i < 5; i++)
            span[i].Should().Be((byte)i);
    }

    [Test]
    public void BufferReader_ReadSpan_ExceedingBuffer()
    {
        var buffer = new byte[5];
        var reader = new BufferReader(buffer);
        
        Assert.Throws<InvalidOperationException>(() => { reader.ReadSpan(10); });
    }

    #endregion
}
