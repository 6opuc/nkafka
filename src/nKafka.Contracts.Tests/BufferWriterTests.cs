using FluentAssertions;
using nKafka.Contracts;
using NUnit.Framework;
using System.Text;

namespace nKafka.Contracts.Tests;

[TestFixture]
public class BufferWriterTests
{
    private const int BufferSize = 1024;
    private byte[] _buffer = null!;
    private BufferWriter _writer = default;

    [SetUp]
    public void Setup()
    {
        _buffer = new byte[BufferSize];
        _writer = new BufferWriter(_buffer);
    }

    [TearDown]
    public void TearDown()
    {
        _writer.Dispose();
    }

    #region WriteByte Tests

    [Test]
    public void WriteByte_WritesSingleByte()
    {
        _writer.WriteByte(42);
        _writer.Position.Should().Be(1);
        _buffer[0].Should().Be(42);
    }

    [Test]
    public void WriteByte_WritesZero()
    {
        _writer.WriteByte(0);
        _buffer[0].Should().Be(0);
    }

    [Test]
    public void WriteByte_WritesMaxValue()
    {
        _writer.WriteByte(byte.MaxValue);
        _buffer[0].Should().Be(byte.MaxValue);
    }

    #endregion

    #region WriteShort Tests

    [Test]
    public void WriteShort_WritesPositiveValue()
    {
        _writer.WriteShort((short)1000);
        _writer.Position.Should().Be(2);
        _buffer[0].Should().Be(0x03);
        _buffer[1].Should().Be(0xE8);
    }

    [Test]
    public void WriteShort_WritesNegativeValue()
    {
        _writer.WriteShort((short)-1);
        _buffer[0].Should().Be(0xFF);
        _buffer[1].Should().Be(0xFF);
    }

    [Test]
    public void WriteShort_WritesZero()
    {
        _writer.WriteShort(0);
        _buffer[0].Should().Be(0);
        _buffer[1].Should().Be(0);
    }

    #endregion

    #region WriteInt Tests

    [Test]
    public void WriteInt_WritesPositiveValue()
    {
        _writer.WriteInt(0x12345678);
        _writer.Position.Should().Be(4);
        _buffer[0].Should().Be(0x12);
        _buffer[1].Should().Be(0x34);
        _buffer[2].Should().Be(0x56);
        _buffer[3].Should().Be(0x78);
    }

    [Test]
    public void WriteInt_WritesNegativeValue()
    {
        _writer.WriteInt(-1);
        _buffer[0].Should().Be(0xFF);
        _buffer[1].Should().Be(0xFF);
        _buffer[2].Should().Be(0xFF);
        _buffer[3].Should().Be(0xFF);
    }

    [Test]
    public void WriteInt_WritesZero()
    {
        _writer.WriteInt(0);
        _buffer[0].Should().Be(0);
        _buffer[1].Should().Be(0);
        _buffer[2].Should().Be(0);
        _buffer[3].Should().Be(0);
    }

    [Test]
    public void WriteInt_WritesMaxValue()
    {
        _writer.WriteInt(int.MaxValue);
        _buffer[0].Should().Be(0x7F);
        _buffer[1].Should().Be(0xFF);
        _buffer[2].Should().Be(0xFF);
        _buffer[3].Should().Be(0xFF);
    }

    #endregion

    #region WriteLong Tests

    [Test]
    public void WriteLong_WritesPositiveValue()
    {
        _writer.WriteLong(0x123456789ABCDEF0L);
        _writer.Position.Should().Be(8);
        _buffer[0].Should().Be(0x12);
        _buffer[1].Should().Be(0x34);
        _buffer[2].Should().Be(0x56);
        _buffer[3].Should().Be(0x78);
        _buffer[4].Should().Be(0x9A);
        _buffer[5].Should().Be(0xBC);
        _buffer[6].Should().Be(0xDE);
        _buffer[7].Should().Be(0xF0);
    }

    [Test]
    public void WriteLong_WritesNegativeValue()
    {
        _writer.WriteLong(-1);
        for (int i = 0; i < 8; i++)
            _buffer[i].Should().Be(0xFF);
    }

    [Test]
    public void WriteLong_WritesZero()
    {
        _writer.WriteLong(0);
        for (int i = 0; i < 8; i++)
            _buffer[i].Should().Be(0);
    }

    [Test]
    public void WriteLong_WritesMaxValue()
    {
        _writer.WriteLong(long.MaxValue);
        _buffer[0].Should().Be(0x7F);
        for (int i = 1; i < 8; i++)
            _buffer[i].Should().Be(0xFF);
    }

    #endregion

    #region WriteUShort Tests

    [Test]
    public void WriteUShort_WritesPositiveValue()
    {
        _writer.WriteUshort(1000);
        _buffer[0].Should().Be(0x03);
        _buffer[1].Should().Be(0xE8);
    }

    [Test]
    public void WriteUShort_WritesZero()
    {
        _writer.WriteUshort(0);
        _buffer[0].Should().Be(0);
        _buffer[1].Should().Be(0);
    }

    [Test]
    public void WriteUShort_WritesMaxValue()
    {
        _writer.WriteUshort(ushort.MaxValue);
        _buffer[0].Should().Be(0xFF);
        _buffer[1].Should().Be(0xFF);
    }

    #endregion

    #region WriteUInt Tests

    [Test]
    public void WriteUInt_WritesPositiveValue()
    {
        _writer.WriteUint(0x12345678);
        _buffer[0].Should().Be(0x12);
        _buffer[1].Should().Be(0x34);
        _buffer[2].Should().Be(0x56);
        _buffer[3].Should().Be(0x78);
    }

    [Test]
    public void WriteUInt_WritesZero()
    {
        _writer.WriteUint(0);
        for (int i = 0; i < 4; i++)
            _buffer[i].Should().Be(0);
    }

    [Test]
    public void WriteUInt_WritesMaxValue()
    {
        _writer.WriteUint(uint.MaxValue);
        for (int i = 0; i < 4; i++)
            _buffer[i].Should().Be(0xFF);
    }

    #endregion

    #region WriteULong Tests

    [Test]
    public void WriteULong_WritesPositiveValue()
    {
        _writer.WriteLong(0x123456789ABCDEF0L);
        _buffer[0].Should().Be(0x12);
        _buffer[7].Should().Be(0xF0);
    }

    [Test]
    public void WriteULong_WritesZero()
    {
        _writer.WriteLong(0);
        for (int i = 0; i < 8; i++)
            _buffer[i].Should().Be(0);
    }

    [Test]
    public void WriteULong_WritesMaxLongValue()
    {
        _writer.WriteLong(long.MaxValue);
        _buffer[0].Should().Be(0x7F);
        for (int i = 1; i < 8; i++)
            _buffer[i].Should().Be(0xFF);
    }

    #endregion

    #region WriteFloat Tests

    [Test]
    public void WriteFloat_WritesPositiveValue()
    {
        var floatBytes = BitConverter.GetBytes(3.14f);
        _writer.WriteInt(BitConverter.SingleToInt32Bits(3.14f));
        _writer.Position.Should().Be(4);
    }

    [Test]
    public void WriteFloat_WritesZero()
    {
        _writer.WriteInt(BitConverter.SingleToInt32Bits(0.0f));
        for (int i = 0; i < 4; i++)
            _buffer[i].Should().Be(0);
    }

    [Test]
    public void WriteFloat_WritesNegativeValue()
    {
        _writer.WriteInt(BitConverter.SingleToInt32Bits(-1.5f));
        _writer.Position.Should().Be(4);
    }

    #endregion

    #region WriteDouble Tests

    [Test]
    public void WriteDoubleBigEndian_WritesPositiveValue()
    {
        _writer.WriteDoubleBigEndian(3.14159);
        _writer.Position.Should().Be(8);
    }

    [Test]
    public void WriteDoubleBigEndian_WritesZero()
    {
        _writer.WriteDoubleBigEndian(0.0);
        for (int i = 0; i < 8; i++)
            _buffer[i].Should().Be(0);
    }

    [Test]
    public void WriteDoubleBigEndian_WritesNegativeValue()
    {
        _writer.WriteDoubleBigEndian(-2.71828);
        _writer.Position.Should().Be(8);
    }

    [Test]
    public void WriteDoubleBigEndian_WritesMaxValue()
    {
        _writer.WriteDoubleBigEndian(double.MaxValue);
        _writer.Position.Should().Be(8);
    }

    #endregion

    #region WriteVarInt Tests

    [Test]
    public void WriteVarInt_WritesZero()
    {
        _writer.WriteVarInt(0);
        _writer.Position.Should().Be(1);
        _buffer[0].Should().Be(0);
    }

    [Test]
    public void WriteVarInt_WritesSmallPositiveValue()
    {
        _writer.WriteVarInt(127);
        _writer.Position.Should().Be(1);
        _buffer[0].Should().Be(127);
    }

    [Test]
    public void WriteVarInt_WritesValueRequiringTwoBytes()
    {
        _writer.WriteVarInt(128);
        _writer.Position.Should().Be(2);
        _buffer[0].Should().Be(0x80);
        _buffer[1].Should().Be(0x01);
    }

    [Test]
    public void WriteVarInt_WritesNegativeValue()
    {
        _writer.WriteVarInt(-1);
        _writer.Position.Should().Be(10);
        _buffer[0].Should().Be(0xFF);
        _buffer[1].Should().Be(0xFF);
        _buffer[2].Should().Be(0xFF);
        _buffer[3].Should().Be(0xFF);
        _buffer[4].Should().Be(0xFF);
        _buffer[5].Should().Be(0xFF);
        _buffer[6].Should().Be(0xFF);
        _buffer[7].Should().Be(0xFF);
        _buffer[8].Should().Be(0xFF);
        _buffer[9].Should().Be(0x01);
    }

    [Test]
    public void WriteVarInt_WritesMaxValue()
    {
        _writer.WriteVarInt(int.MaxValue);
        _writer.Position.Should().Be(5);
    }

    [Test]
    public void WriteVarInt_WritesMinValue()
    {
        _writer.WriteVarInt(int.MinValue);
        _writer.Position.Should().Be(10);
    }

    #endregion

    #region WriteVarLong Tests

    [Test]
    public void WriteVarLong_WritesZero()
    {
        _writer.WriteVarLong(0);
        _writer.Position.Should().Be(1);
        _buffer[0].Should().Be(0);
    }

    [Test]
    public void WriteVarLong_WritesSmallPositiveValue()
    {
        _writer.WriteVarLong(1000);
        _writer.Position.Should().Be(2);
    }

    [Test]
    public void WriteVarLong_WritesLargeValue()
    {
        _writer.WriteVarLong(long.MaxValue);
        _writer.Position.Should().Be(10);
    }

    [Test]
    public void WriteVarLong_WritesNegativeValue()
    {
        _writer.WriteVarLong(-1000);
        _writer.Position.Should().BeGreaterThan(1);
    }

    [Test]
    public void WriteVarLong_WritesMinValue()
    {
        _writer.WriteVarLong(long.MinValue);
        _writer.Position.Should().Be(10);
    }

    [Test]
    public void WriteVarLong_WritesMaxValue()
    {
        _writer.WriteVarLong(long.MaxValue);
        _writer.Position.Should().Be(10);
    }

    #endregion

    #region WriteUVarInt Tests

    [Test]
    public void WriteUVarInt_WritesZero()
    {
        _writer.WriteUVarInt(0);
        _writer.Position.Should().Be(1);
        _buffer[0].Should().Be(0);
    }

    [Test]
    public void WriteUVarInt_WritesSmallValue()
    {
        _writer.WriteUVarInt(127);
        _writer.Position.Should().Be(1);
        _buffer[0].Should().Be(127);
    }

    [Test]
    public void WriteUVarInt_WritesValueRequiringTwoBytes()
    {
        _writer.WriteUVarInt(128);
        _writer.Position.Should().Be(2);
        _buffer[0].Should().Be(0x80);
        _buffer[1].Should().Be(0x01);
    }

    [Test]
    public void WriteUVarInt_WritesLargeValue()
    {
        _writer.WriteUVarInt(uint.MaxValue);
        _writer.Position.Should().Be(5);
    }

    [Test]
    public void WriteUVarInt_WritesMaxValue()
    {
        _writer.WriteUVarInt(uint.MaxValue);
        _writer.Position.Should().Be(5);
    }

    #endregion

    #region WriteUVarLong Tests

    [Test]
    public void WriteUVarLong_WritesZero()
    {
        _writer.WriteUVarLong(0);
        _writer.Position.Should().Be(1);
        _buffer[0].Should().Be(0);
    }

    [Test]
    public void WriteUVarLong_WritesSmallValue()
    {
        _writer.WriteUVarLong(1000);
        _writer.Position.Should().Be(2);
    }

    [Test]
    public void WriteUVarLong_WritesLargeValue()
    {
        _writer.WriteUVarLong(ulong.MaxValue);
        _writer.Position.Should().Be(10);
    }

    [Test]
    public void WriteUVarLong_WritesMaxLongValue()
    {
        _writer.WriteUVarLong((ulong)long.MaxValue);
        _writer.Position.Should().Be(9);
    }

    [Test]
    public void WriteUVarLong_WritesMaxValue()
    {
        _writer.WriteUVarLong(ulong.MaxValue);
        _writer.Position.Should().Be(10);
    }

    #endregion

    #region WriteString Tests

    [Test]
    public void WriteString_WritesSimpleString()
    {
        _writer.WriteString("Hello");
        _writer.Position.Should().Be(7);
        _buffer[0].Should().Be(0);
        _buffer[1].Should().Be(5);
        Encoding.UTF8.GetBytes("Hello").AsSpan().SequenceEqual(_buffer.AsSpan(2, 5)).Should().BeTrue();
    }

    [Test]
    public void WriteString_WritesNull()
    {
        _writer.WriteString(null);
        _writer.Position.Should().Be(2);
        _buffer[0].Should().Be(0xFF);
        _buffer[1].Should().Be(0xFF);
    }

    [Test]
    public void WriteString_WritesEmptyString()
    {
        _writer.WriteString("");
        _writer.Position.Should().Be(2);
        _buffer[0].Should().Be(0);
        _buffer[1].Should().Be(0);
    }

    #endregion

    #region WriteVarString Tests

    [Test]
    public void WriteVarString_WritesSimpleString()
    {
        _writer.WriteVarString("Hello");
        _writer.Position.Should().BeGreaterThan(2);
    }

    [Test]
    public void WriteVarString_WritesNull()
    {
        _writer.WriteVarString(null);
        _writer.Position.Should().Be(1);
        _buffer[0].Should().Be(0xFF);
    }

    [Test]
    public void WriteVarString_WritesEmptyString()
    {
        _writer.WriteVarString("");
        _writer.Position.Should().Be(2);
        _buffer[0].Should().Be(0);
        _buffer[1].Should().Be(0);
    }

    #endregion

    #region WriteGuid Tests

    [Test]
    public void WriteGuid_WritesValidGuid()
    {
        var guid = new Guid("00010203-0405-0607-0809-0A0B0C0D0E0F");
        _writer.WriteGuid(guid);
        _writer.Position.Should().Be(16);
    }

    [Test]
    public void WriteGuid_WritesNull()
    {
        _writer.WriteGuid((Guid?)null);
        _writer.Position.Should().Be(16);
        for (int i = 0; i < 16; i++)
            _buffer[i].Should().Be(0);
    }

    #endregion

    #region Position Tracking Tests

    [Test]
    public void Position_IncrementsAfterWrites()
    {
        _writer.WriteByte(1);
        _writer.Position.Should().Be(1);
        _writer.WriteShort(2);
        _writer.Position.Should().Be(3);
        _writer.WriteInt(3);
        _writer.Position.Should().Be(7);
    }

    [Test]
    public void Reset_ClearsPosition()
    {
        _writer.WriteInt(42);
        _writer.Reset();
        _writer.Position.Should().Be(0);
    }

    [Test]
    public void Position_CanBeSet()
    {
        _writer.WriteInt(42);
        _writer.Position = 0;
        _writer.Position.Should().Be(0);
        _writer.WriteInt(99);
        _buffer[0].Should().Be(0);
        _buffer[1].Should().Be(0);
        _buffer[2].Should().Be(0);
        _buffer[3].Should().Be(99);
    }

    #endregion

    #region Boundary Conditions Tests

    [Test]
    public void Remaining_ReturnsCorrectValue()
    {
        var initialRemaining = _writer.Remaining;
        _writer.WriteInt(42);
        _writer.Remaining.Should().Be(initialRemaining - 4);
    }

    [Test]
    public void Capacity_ReturnsBufferSize()
    {
        _writer.Capacity.Should().Be(BufferSize);
    }

    [Test]
    public void MultipleWritesToBuffer()
    {
        _writer.WriteByte(1);
        _writer.WriteShort(2);
        _writer.WriteInt(3);
        _writer.WriteLong(4);
        _writer.Position.Should().Be(15);
    }

    [Test]
    public void WriteAtNearCapacity()
    {
        var smallBuffer = new byte[10];
        var smallWriter = new BufferWriter(smallBuffer);
        smallWriter.WriteInt(42);
        smallWriter.WriteLong(99);
        smallWriter.Position.Should().Be(12);
        smallWriter.Dispose();
    }

    #endregion
}
