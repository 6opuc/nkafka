using System.Text;
using FluentAssertions;
using nKafka.Contracts;
using NUnit.Framework;

namespace nKafka.Contracts.Tests;

[TestFixture]
public class BufferReaderTests
{
    private const int BufferSize = 1024;
    private byte[] _buffer = null!;
    private BufferReader _reader = default;
    private BufferWriter _writer = default;

    [SetUp]
    public void Setup()
    {
        _buffer = new byte[BufferSize];
        _reader = new BufferReader(_buffer);
        _writer = new BufferWriter(_buffer);
    }

    [TearDown]
    public void TearDown()
    {
        _writer.Dispose();
    }

    #region ReadByte Tests

    [Test]
    public void ReadByte_ReadsCorrectValue()
    {
        _buffer[0] = 42;
        _reader = new BufferReader(_buffer);
        _reader.ReadByte().Should().Be(42);
    }

    [Test]
    public void ReadByte_ReadsZero()
    {
        _buffer[0] = 0;
        _reader = new BufferReader(_buffer);
        _reader.ReadByte().Should().Be(0);
    }

    [Test]
    public void ReadByte_ReadsMaxValue()
    {
        _buffer[0] = byte.MaxValue;
        _reader = new BufferReader(_buffer);
        _reader.ReadByte().Should().Be(byte.MaxValue);
    }

    #endregion

    #region ReadShort Tests

    [Test]
    public void ReadUInt16_ReadsPositiveValue()
    {
        _buffer[0] = 0x03;
        _buffer[1] = 0xE8;
        _reader = new BufferReader(_buffer);
        _reader.ReadInt16BigEndian().Should().Be(1000);
    }

    [Test]
    public void ReadInt16BigEndian_ReadsNegativeValue()
    {
        _buffer[0] = 0xFF;
        _buffer[1] = 0xFF;
        _reader = new BufferReader(_buffer);
        _reader.ReadInt16BigEndian().Should().Be(-1);
    }

    [Test]
    public void ReadUInt16_ReadsZero()
    {
        _buffer[0] = 0;
        _buffer[1] = 0;
        _reader = new BufferReader(_buffer);
        _reader.ReadInt16BigEndian().Should().Be(0);
    }

    [Test]
    public void ReadUInt16_ReadsMaxValue()
    {
        _buffer[0] = 0x7F;
        _buffer[1] = 0xFF;
        _reader = new BufferReader(_buffer);
        _reader.ReadInt16BigEndian().Should().Be(short.MaxValue);
    }

    #endregion

    #region ReadInt Tests

    [Test]
    public void ReadInt32BigEndian_ReadsPositiveValue()
    {
        _buffer[0] = 0x12;
        _buffer[1] = 0x34;
        _buffer[2] = 0x56;
        _buffer[3] = 0x78;
        _reader = new BufferReader(_buffer);
        _reader.ReadInt32BigEndian().Should().Be(0x12345678);
    }

    [Test]
    public void ReadInt32BigEndian_ReadsNegativeValue()
    {
        for (int i = 0; i < 4; i++)
            _buffer[i] = 0xFF;
        _reader = new BufferReader(_buffer);
        _reader.ReadInt32BigEndian().Should().Be(-1);
    }

    [Test]
    public void ReadInt32BigEndian_ReadsZero()
    {
        for (int i = 0; i < 4; i++)
            _buffer[i] = 0;
        _reader = new BufferReader(_buffer);
        _reader.ReadInt32BigEndian().Should().Be(0);
    }

    [Test]
    public void ReadInt32BigEndian_ReadsMaxValue()
    {
        _buffer[0] = 0x7F;
        _buffer[1] = 0xFF;
        _buffer[2] = 0xFF;
        _buffer[3] = 0xFF;
        _reader = new BufferReader(_buffer);
        _reader.ReadInt32BigEndian().Should().Be(int.MaxValue);
    }

    #endregion

    #region ReadLong Tests

    [Test]
    public void ReadUInt64_ReadsPositiveValue()
    {
        _buffer[0] = 0x12;
        _buffer[1] = 0x34;
        _buffer[2] = 0x56;
        _buffer[3] = 0x78;
        _buffer[4] = 0x9A;
        _buffer[5] = 0xBC;
        _buffer[6] = 0xDE;
        _buffer[7] = 0xF0;
        _reader = new BufferReader(_buffer);
        _reader.ReadInt64BigEndian().Should().Be(0x123456789ABCDEF0L);
    }

    [Test]
    public void ReadInt64BigEndian_ReadsNegativeValue()
    {
        for (int i = 0; i < 8; i++)
            _buffer[i] = 0xFF;
        _reader = new BufferReader(_buffer);
        _reader.ReadInt64BigEndian().Should().Be(-1);
    }

    [Test]
    public void ReadUInt64_ReadsZero()
    {
        for (int i = 0; i < 8; i++)
            _buffer[i] = 0;
        _reader = new BufferReader(_buffer);
        _reader.ReadInt64BigEndian().Should().Be(0);
    }

    [Test]
    public void ReadUInt64_ReadsMaxValue()
    {
        _buffer[0] = 0x7F;
        for (int i = 1; i < 8; i++)
            _buffer[i] = 0xFF;
        _reader = new BufferReader(_buffer);
        _reader.ReadInt64BigEndian().Should().Be(long.MaxValue);
    }

    #endregion


    #region ReadUInt Tests

    [Test]
    public void ReadUInt32BigEndian_ReadsPositiveValue()
    {
        _buffer[0] = 0x12;
        _buffer[1] = 0x34;
        _buffer[2] = 0x56;
        _buffer[3] = 0x78;
        _reader = new BufferReader(_buffer);
        _reader.ReadUInt32BigEndian().Should().Be(0x12345678);
    }

    [Test]
    public void ReadUInt32BigEndian_ReadsZero()
    {
        for (int i = 0; i < 4; i++)
            _buffer[i] = 0;
        _reader = new BufferReader(_buffer);
        _reader.ReadUInt32BigEndian().Should().Be(0);
    }

    [Test]
    public void ReadUInt32BigEndian_ReadsMaxValue()
    {
        for (int i = 0; i < 4; i++)
            _buffer[i] = 0xFF;
        _reader = new BufferReader(_buffer);
        _reader.ReadUInt32BigEndian().Should().Be(uint.MaxValue);
    }

    #endregion


    #region ReadFloat Tests

    [Test]
    public void ReadFloat_ReadsPositiveValue()
    {
        int intBits = BitConverter.SingleToInt32Bits(3.14f);
        _buffer[0] = (byte)(intBits >> 24);
        _buffer[1] = (byte)(intBits >> 16);
        _buffer[2] = (byte)(intBits >> 8);
        _buffer[3] = (byte)intBits;
        _reader = new BufferReader(_buffer);
        float result = BitConverter.Int32BitsToSingle(_reader.ReadInt32BigEndian());
        result.Should().BeApproximately(3.14f, 0.0001f);
    }

    [Test]
    public void ReadFloat_ReadsZero()
    {
        for (int i = 0; i < 4; i++)
            _buffer[i] = 0;
        _reader = new BufferReader(_buffer);
        float result = BitConverter.Int32BitsToSingle(_reader.ReadInt32BigEndian());
        result.Should().Be(0.0f);
    }

    [Test]
    public void ReadFloat_ReadsNegativeValue()
    {
        int intBits = BitConverter.SingleToInt32Bits(-1.5f);
        _buffer[0] = (byte)(intBits >> 24);
        _buffer[1] = (byte)(intBits >> 16);
        _buffer[2] = (byte)(intBits >> 8);
        _buffer[3] = (byte)intBits;
        _reader = new BufferReader(_buffer);
        float result = BitConverter.Int32BitsToSingle(_reader.ReadInt32BigEndian());
        result.Should().BeApproximately(-1.5f, 0.0001f);
    }

    #endregion

    #region ReadDouble Tests

    [Test]
    public void ReadDoubleBigEndian_ReadsPositiveValue()
    {
        _writer.WriteDoubleBigEndian(3.14159);
        _reader = new BufferReader(_buffer);
        _reader.ReadDoubleBigEndian().Should().BeApproximately(3.14159, 0.00001);
    }

    [Test]
    public void ReadDoubleBigEndian_ReadsZero()
    {
        for (int i = 0; i < 8; i++)
            _buffer[i] = 0;
        _reader = new BufferReader(_buffer);
        _reader.ReadDoubleBigEndian().Should().Be(0.0);
    }

    [Test]
    public void ReadDoubleBigEndian_ReadsNegativeValue()
    {
        _writer.WriteDoubleBigEndian(-2.71828);
        _reader = new BufferReader(_buffer);
        _reader.ReadDoubleBigEndian().Should().BeApproximately(-2.71828, 0.00001);
    }

    [Test]
    public void ReadDoubleBigEndian_ReadsMaxValue()
    {
        _writer.WriteDoubleBigEndian(double.MaxValue);
        _reader = new BufferReader(_buffer);
        _reader.ReadDoubleBigEndian().Should().Be(double.MaxValue);
    }

    #endregion

    #region ReadVarInt Tests

    [Test]
    public void ReadVarInt_ReadsZero()
    {
        _buffer[0] = 0;
        _reader = new BufferReader(_buffer);
        _reader.ReadVarInt().Should().Be(0);
    }

    [Test]
    public void ReadVarInt_ReadsSmallPositiveValue()
    {
        _buffer[0] = 127;
        _reader = new BufferReader(_buffer);
        _reader.ReadVarInt().Should().Be(127);
    }

    [Test]
    public void ReadVarInt_ReadsValueRequiringTwoBytes()
    {
        _buffer[0] = 0x80;
        _buffer[1] = 0x01;
        _reader = new BufferReader(_buffer);
        _reader.ReadVarInt().Should().Be(128);
    }

    [Test]
    public void ReadVarInt_ReadsNegativeValue()
    {
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
        _reader = new BufferReader(_buffer);
        _reader.ReadVarInt().Should().Be(-1);
    }

    [Test]
    public void ReadVarInt_ReadsLargeValue()
    {
        _writer.WriteVarInt(1000000);
        _reader = new BufferReader(_buffer);
        _reader.ReadVarInt().Should().Be(1000000);
    }

    [Test]
    public void ReadVarInt_ReadsMaxValue()
    {
        _writer.WriteVarInt(int.MaxValue);
        _reader = new BufferReader(_buffer);
        _reader.ReadVarInt().Should().Be(int.MaxValue);
    }

    #endregion

    #region ReadVarLong Tests

    [Test]
    public void ReadVarLong_ReadsZero()
    {
        _buffer[0] = 0;
        _reader = new BufferReader(_buffer);
        _reader.ReadVarLong().Should().Be(0);
    }

    [Test]
    public void ReadVarLong_ReadsSmallPositiveValue()
    {
        _buffer[0] = 0x80;
        _buffer[1] = 0x07;
        _reader = new BufferReader(_buffer);
        _reader.ReadVarLong().Should().Be(1000);
    }

    [Test]
    public void ReadVarLong_ReadsLargeValue()
    {
        _writer.WriteVarLong(10000000000L);
        _reader = new BufferReader(_buffer);
        _reader.ReadVarLong().Should().Be(10000000000L);
    }

    [Test]
    public void ReadVarLong_ReadsNegativeValue()
    {
        _writer.WriteVarLong(-1000);
        _reader = new BufferReader(_buffer);
        _reader.ReadVarLong().Should().Be(-1000);
    }

    [Test]
    public void ReadVarLong_ReadsMinValue()
    {
        _writer.WriteVarLong(long.MinValue);
        _reader = new BufferReader(_buffer);
        _reader.ReadVarLong().Should().Be(long.MinValue);
    }

    [Test]
    public void ReadVarLong_ReadsMaxValue()
    {
        _writer.WriteVarLong(long.MaxValue);
        _reader = new BufferReader(_buffer);
        _reader.ReadVarLong().Should().Be(long.MaxValue);
    }

    #endregion

    #region ReadUVarInt Tests

    [Test]
    public void ReadUVarInt_ReadsZero()
    {
        _buffer[0] = 0;
        _reader = new BufferReader(_buffer);
        _reader.ReadUVarInt().Should().Be(0);
    }

    [Test]
    public void ReadUVarInt_ReadsSmallValue()
    {
        _buffer[0] = 127;
        _reader = new BufferReader(_buffer);
        _reader.ReadUVarInt().Should().Be(127);
    }

    [Test]
    public void ReadUVarInt_ReadsValueRequiringTwoBytes()
    {
        _buffer[0] = 0x80;
        _buffer[1] = 0x01;
        _reader = new BufferReader(_buffer);
        _reader.ReadUVarInt().Should().Be(128);
    }

    [Test]
    public void ReadUVarInt_ReadsLargeValue()
    {
        _writer.WriteUVarInt(uint.MaxValue);
        _reader = new BufferReader(_buffer);
        _reader.ReadUVarInt().Should().Be(uint.MaxValue);
    }

    [Test]
    public void ReadUVarInt_ReadsMaxValue()
    {
        _writer.WriteUVarInt(uint.MaxValue);
        _reader = new BufferReader(_buffer);
        _reader.ReadUVarInt().Should().Be(uint.MaxValue);
    }

    #endregion

    #region ReadUVarLong Tests

    [Test]
    public void ReadUVarLong_ReadsZero()
    {
        _buffer[0] = 0;
        _reader = new BufferReader(_buffer);
        _reader.ReadUVarLong().Should().Be(0);
    }

    [Test]
    public void ReadUVarLong_ReadsSmallValue()
    {
        _buffer[0] = 0x80;
        _buffer[1] = 0x07;
        _reader = new BufferReader(_buffer);
        _reader.ReadUVarLong().Should().Be(1000);
    }

    [Test]
    public void ReadUVarLong_ReadsLargeValue()
    {
        _writer.WriteUVarLong(ulong.MaxValue);
        _reader = new BufferReader(_buffer);
        _reader.ReadUVarLong().Should().Be(ulong.MaxValue);
    }

    [Test]
    public void ReadUVarLong_ReadsMaxLongValue()
    {
        _writer.WriteUVarLong((ulong)long.MaxValue);
        _reader = new BufferReader(_buffer);
        _reader.ReadUVarLong().Should().Be((ulong)long.MaxValue);
    }

    [Test]
    public void ReadUVarLong_ReadsMaxValue()
    {
        _writer.WriteUVarLong(ulong.MaxValue);
        _reader = new BufferReader(_buffer);
        _reader.ReadUVarLong().Should().Be(ulong.MaxValue);
    }

    #endregion

    #region ReadString Tests

    [Test]
    public void ReadString_ReadsSimpleString()
    {
        byte[] bytes = Encoding.UTF8.GetBytes("Hello");
        _buffer[0] = 0;
        _buffer[1] = (byte)bytes.Length;
        bytes.CopyTo(_buffer, 2);
        _reader = new BufferReader(_buffer);
        _reader.ReadString().Should().Be("Hello");
    }

    [Test]
    public void ReadString_ReadsNull()
    {
        _buffer[0] = 0xFF;
        _buffer[1] = 0xFF;
        _reader = new BufferReader(_buffer);
        _reader.ReadString().Should().BeNull();
    }

    [Test]
    public void ReadString_ReadsEmptyString()
    {
        _buffer[0] = 0;
        _buffer[1] = 0;
        _reader = new BufferReader(_buffer);
        _reader.ReadString().Should().Be("");
    }

    #endregion

    #region ReadVarString Tests

    [Test]
    public void ReadVarString_ReadsSimpleString()
    {
        byte[] bytes = Encoding.UTF8.GetBytes("Hello");
        _buffer[0] = (byte)(bytes.Length | 0x80);
        _buffer[1] = (byte)bytes.Length;
        bytes.CopyTo(_buffer, 2);
        _reader = new BufferReader(_buffer);
        _reader.ReadVarString().Should().Be("Hello");
    }

    [Test]
    public void ReadVarString_ReadsNull()
    {
        _buffer[0] = 0xFF;
        _reader = new BufferReader(_buffer);
        _reader.ReadVarString().Should().BeNull();
    }

    [Test]
    public void ReadVarString_ReadsEmptyString()
    {
        _buffer[0] = 0;
        _buffer[1] = 0;
        _reader = new BufferReader(_buffer);
        _reader.ReadVarString().Should().Be("");
    }

    #endregion

    #region ReadGuid Tests

    [Test]
    public void ReadGuid_ReadsValidGuid()
    {
        var guid = new Guid("00010203-0405-0607-0809-0A0B0C0D0E0F");
        guid.TryWriteBytes(_buffer);
        _reader = new BufferReader(_buffer);
        _reader.ReadGuid().Should().Be(guid);
    }

    [Test]
    public void ReadGuid_ReadsEmptyGuid()
    {
        for (int i = 0; i < 16; i++)
            _buffer[i] = 0;
        _reader = new BufferReader(_buffer);
        _reader.ReadGuid().Should().Be(Guid.Empty);
    }

    #endregion

    #region Position Tracking Tests

    [Test]
    public void Position_IncrementsAfterReads()
    {
        _buffer[0] = 1;
        _buffer[1] = 0;
        _buffer[2] = 2;
        _buffer[3] = 0;
        _reader = new BufferReader(_buffer);
        _reader.Position.Should().Be(0);
        _reader.ReadByte();
        _reader.Position.Should().Be(1);
        _reader.ReadInt16BigEndian();
        _reader.Position.Should().Be(3);
    }

    [Test]
    public void Remaining_ReturnsCorrectValue()
    {
        _reader = new BufferReader(_buffer);
        int initialRemaining = _reader.Remaining;
        _reader.ReadByte();
        _reader.Remaining.Should().Be(initialRemaining - 1);
    }

    [Test]
    public void Advance_PositionsReaderCorrectly()
    {
        _reader = new BufferReader(_buffer);
        _reader.Advance(10);
        _reader.Position.Should().Be(10);
    }

    #endregion

    #region Remaining Tests

    [Test]
    public void CreateRemaining_ReturnsReaderWithRemainingData()
    {
        _writer.Dispose(); var writer = new BufferWriter(_buffer); writer.WriteInt(42);
        _writer.WriteInt(99);
        _reader = new BufferReader(_buffer);
        _reader.ReadByte();
        var child = _reader.CreateRemaining();
        child.Remaining.Should().Be(7);
    }

    [Test]
    public void CreateChild_ReturnsReaderWithSpecifiedLength()
    {
        _writer.Dispose(); var writer = new BufferWriter(_buffer); writer.WriteInt(42);
        _writer.WriteInt(99);
        _reader = new BufferReader(_buffer);
        var child = _reader.CreateChild(4);
        child.Remaining.Should().Be(4);
        _reader.Position.Should().Be(4);
    }

    #endregion
}
