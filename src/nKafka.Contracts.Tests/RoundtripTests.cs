using System.Text;
using FluentAssertions;
using nKafka.Contracts;
using NUnit.Framework;

namespace nKafka.Contracts.Tests;

[TestFixture]
public class RoundtripTests
{
    private const int BufferSize = 4096;
    private byte[] _buffer = null!;

    [SetUp]
    public void Setup()
    {
        _buffer = new byte[BufferSize];
    }

    #region Byte Roundtrip Tests

    [Test]
    public void Roundtrip_Byte_Zero()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteByte(0);
        var reader = new BufferReader(_buffer);
        reader.ReadByte().Should().Be(0);
    }

    [Test]
    public void Roundtrip_Byte_PositiveValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteByte(42);
        var reader = new BufferReader(_buffer);
        reader.ReadByte().Should().Be(42);
    }

    [Test]
    public void Roundtrip_Byte_MaxValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteByte(byte.MaxValue);
        var reader = new BufferReader(_buffer);
        reader.ReadByte().Should().Be(byte.MaxValue);
    }

    #endregion

    #region Short Roundtrip Tests

    [Test]
    public void Roundtrip_Short_Zero()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteShort(0);
        var reader = new BufferReader(_buffer);
        reader.ReadInt16BigEndian().Should().Be(0);
    }

    [Test]
    public void Roundtrip_Short_PositiveValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteShort(1000);
        var reader = new BufferReader(_buffer);
        reader.ReadInt16BigEndian().Should().Be(1000);
    }

    [Test]
    public void Roundtrip_Short_NegativeValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteShort(-1000);
        var reader = new BufferReader(_buffer);
        reader.ReadInt16BigEndian().Should().Be(-1000);
    }

    [Test]
    public void Roundtrip_Short_MaxValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteShort(short.MaxValue);
        var reader = new BufferReader(_buffer);
        reader.ReadInt16BigEndian().Should().Be(short.MaxValue);
    }

    [Test]
    public void Roundtrip_Short_MinValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteShort(short.MinValue);
        var reader = new BufferReader(_buffer);
        reader.ReadInt16BigEndian().Should().Be(short.MinValue);
    }

    #endregion

    #region Int Roundtrip Tests

    [Test]
    public void Roundtrip_Int_Zero()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteInt(0);
        var reader = new BufferReader(_buffer);
        reader.ReadInt32BigEndian().Should().Be(0);
    }

    [Test]
    public void Roundtrip_Int_PositiveValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteInt(0x12345678);
        var reader = new BufferReader(_buffer);
        reader.ReadInt32BigEndian().Should().Be(0x12345678);
    }

    [Test]
    public void Roundtrip_Int_NegativeValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteInt(-123456);
        var reader = new BufferReader(_buffer);
        reader.ReadInt32BigEndian().Should().Be(-123456);
    }

    [Test]
    public void Roundtrip_Int_MaxValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteInt(int.MaxValue);
        var reader = new BufferReader(_buffer);
        reader.ReadInt32BigEndian().Should().Be(int.MaxValue);
    }

    [Test]
    public void Roundtrip_Int_MinValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteInt(int.MinValue);
        var reader = new BufferReader(_buffer);
        reader.ReadInt32BigEndian().Should().Be(int.MinValue);
    }

    #endregion

    #region Long Roundtrip Tests

    [Test]
    public void Roundtrip_Long_Zero()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteLong(0);
        var reader = new BufferReader(_buffer);
        reader.ReadInt64BigEndian().Should().Be(0);
    }

    [Test]
    public void Roundtrip_Long_PositiveValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteLong(0x123456789ABCDEF0L);
        var reader = new BufferReader(_buffer);
        reader.ReadInt64BigEndian().Should().Be(0x123456789ABCDEF0L);
    }

    [Test]
    public void Roundtrip_Long_NegativeValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteLong(-123456789012345L);
        var reader = new BufferReader(_buffer);
        reader.ReadInt64BigEndian().Should().Be(-123456789012345L);
    }

    [Test]
    public void Roundtrip_Long_MaxValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteLong(long.MaxValue);
        var reader = new BufferReader(_buffer);
        reader.ReadInt64BigEndian().Should().Be(long.MaxValue);
    }

    [Test]
    public void Roundtrip_Long_MinValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteLong(long.MinValue);
        var reader = new BufferReader(_buffer);
        reader.ReadInt64BigEndian().Should().Be(long.MinValue);
    }

    #endregion

    #region UShort Roundtrip Tests

    [Test]
    public void Roundtrip_UShort_Zero()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteUshort(0);
        var reader = new BufferReader(_buffer);
        reader.ReadInt16BigEndian().Should().Be(0);
    }

    [Test]
    public void Roundtrip_UShort_PositiveValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteUshort(1000);
        var reader = new BufferReader(_buffer);
        reader.ReadInt16BigEndian().Should().Be(1000);
    }

    [Test]
    public void Roundtrip_UShort_MaxValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteUshort(ushort.MaxValue);
        var reader = new BufferReader(_buffer);
        BitConverter.ToUInt16(_buffer, 0).Should().Be(ushort.MaxValue);
    }

    #endregion

    #region UInt Roundtrip Tests

    [Test]
    public void Roundtrip_UInt_Zero()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteUint(0);
        var reader = new BufferReader(_buffer);
        reader.ReadUInt32BigEndian().Should().Be(0);
    }

    [Test]
    public void Roundtrip_UInt_PositiveValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteUint(0x12345678);
        var reader = new BufferReader(_buffer);
        reader.ReadUInt32BigEndian().Should().Be(0x12345678);
    }

    [Test]
    public void Roundtrip_UInt_MaxValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteUint(uint.MaxValue);
        var reader = new BufferReader(_buffer);
        reader.ReadUInt32BigEndian().Should().Be(uint.MaxValue);
    }

    #endregion

    #region ULong Roundtrip Tests

    [Test]
    public void Roundtrip_ULong_Zero()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteLong(0);
        var reader = new BufferReader(_buffer);
        reader.ReadInt64BigEndian().Should().Be(0);
    }

    [Test]
    public void Roundtrip_ULong_PositiveValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteLong(0x123456789ABCDEF0L);
        var reader = new BufferReader(_buffer);
        reader.ReadInt64BigEndian().Should().Be(0x123456789ABCDEF0);
    }

    [Test]
    public void Roundtrip_ULong_MaxLongValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteLong(long.MaxValue);
        var reader = new BufferReader(_buffer);
        reader.ReadInt64BigEndian().Should().Be(long.MaxValue);
    }

    #endregion

    #region Float Roundtrip Tests

    [Test]
    public void Roundtrip_Float_Zero()
    {
        var writer = new BufferWriter(_buffer);
        int intBits = BitConverter.SingleToInt32Bits(0.0f);
        writer.WriteInt(intBits);
        var reader = new BufferReader(_buffer);
        float result = BitConverter.Int32BitsToSingle(reader.ReadInt32BigEndian());
        result.Should().Be(0.0f);
    }

    [Test]
    public void Roundtrip_Float_PositiveValue()
    {
        var writer = new BufferWriter(_buffer);
        int intBits = BitConverter.SingleToInt32Bits(3.14159f);
        writer.WriteInt(intBits);
        var reader = new BufferReader(_buffer);
        float result = BitConverter.Int32BitsToSingle(reader.ReadInt32BigEndian());
        result.Should().BeApproximately(3.14159f, 0.00001f);
    }

    [Test]
    public void Roundtrip_Float_NegativeValue()
    {
        var writer = new BufferWriter(_buffer);
        int intBits = BitConverter.SingleToInt32Bits(-2.71828f);
        writer.WriteInt(intBits);
        var reader = new BufferReader(_buffer);
        float result = BitConverter.Int32BitsToSingle(reader.ReadInt32BigEndian());
        result.Should().BeApproximately(-2.71828f, 0.00001f);
    }

    #endregion

    #region Double Roundtrip Tests

    [Test]
    public void Roundtrip_Double_Zero()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteDoubleBigEndian(0.0);
        var reader = new BufferReader(_buffer);
        reader.ReadDoubleBigEndian().Should().Be(0.0);
    }

    [Test]
    public void Roundtrip_Double_PositiveValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteDoubleBigEndian(3.14159265359);
        var reader = new BufferReader(_buffer);
        reader.ReadDoubleBigEndian().Should().BeApproximately(3.14159265359, 0.00000000001);
    }

    [Test]
    public void Roundtrip_Double_NegativeValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteDoubleBigEndian(-2.71828182846);
        var reader = new BufferReader(_buffer);
        reader.ReadDoubleBigEndian().Should().BeApproximately(-2.71828182846, 0.00000000001);
    }

    [Test]
    public void Roundtrip_Double_MaxValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteDoubleBigEndian(double.MaxValue);
        var reader = new BufferReader(_buffer);
        reader.ReadDoubleBigEndian().Should().Be(double.MaxValue);
    }

    #endregion

    #region VarInt Roundtrip Tests

    [Test]
    public void Roundtrip_VarInt_Zero()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteVarInt(0);
        var reader = new BufferReader(_buffer);
        reader.ReadVarInt().Should().Be(0);
    }

    [Test]
    public void Roundtrip_VarInt_SmallPositiveValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteVarInt(127);
        var reader = new BufferReader(_buffer);
        reader.ReadVarInt().Should().Be(127);
    }

    [Test]
    public void Roundtrip_VarInt_LargePositiveValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteVarInt(1000000);
        var reader = new BufferReader(_buffer);
        reader.ReadVarInt().Should().Be(1000000);
    }

    [Test]
    public void Roundtrip_VarInt_NegativeValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteVarInt(-1);
        var reader = new BufferReader(_buffer);
        reader.ReadVarInt().Should().Be(-1);
    }

    [Test]
    public void Roundtrip_VarInt_MaxValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteVarInt(int.MaxValue);
        var reader = new BufferReader(_buffer);
        reader.ReadVarInt().Should().Be(int.MaxValue);
    }

    [Test]
    public void Roundtrip_VarInt_MinValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteVarInt(int.MinValue);
        var reader = new BufferReader(_buffer);
        reader.ReadVarInt().Should().Be(int.MinValue);
    }

    #endregion

    #region VarLong Roundtrip Tests

    [Test]
    public void Roundtrip_VarLong_Zero()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteVarLong(0);
        var reader = new BufferReader(_buffer);
        reader.ReadVarLong().Should().Be(0);
    }

    [Test]
    public void Roundtrip_VarLong_SmallPositiveValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteVarLong(1000);
        var reader = new BufferReader(_buffer);
        reader.ReadVarLong().Should().Be(1000);
    }

    [Test]
    public void Roundtrip_VarLong_LargePositiveValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteVarLong(10000000000L);
        var reader = new BufferReader(_buffer);
        reader.ReadVarLong().Should().Be(10000000000L);
    }

    [Test]
    public void Roundtrip_VarLong_NegativeValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteVarLong(-1000);
        var reader = new BufferReader(_buffer);
        reader.ReadVarLong().Should().Be(-1000);
    }

    [Test]
    public void Roundtrip_VarLong_MaxValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteVarLong(long.MaxValue);
        var reader = new BufferReader(_buffer);
        reader.ReadVarLong().Should().Be(long.MaxValue);
    }

    [Test]
    public void Roundtrip_VarLong_MinValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteVarLong(long.MinValue);
        var reader = new BufferReader(_buffer);
        reader.ReadVarLong().Should().Be(long.MinValue);
    }

    #endregion

    #region UVarInt Roundtrip Tests

    [Test]
    public void Roundtrip_UVarInt_Zero()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteUVarInt(0);
        var reader = new BufferReader(_buffer);
        reader.ReadUVarInt().Should().Be(0);
    }

    [Test]
    public void Roundtrip_UVarInt_SmallValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteUVarInt(127);
        var reader = new BufferReader(_buffer);
        reader.ReadUVarInt().Should().Be(127);
    }

    [Test]
    public void Roundtrip_UVarInt_LargeValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteUVarInt(1000000);
        var reader = new BufferReader(_buffer);
        reader.ReadUVarInt().Should().Be(1000000);
    }

    [Test]
    public void Roundtrip_UVarInt_MaxValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteUVarInt(uint.MaxValue);
        var reader = new BufferReader(_buffer);
        reader.ReadUVarInt().Should().Be(uint.MaxValue);
    }

    #endregion

    #region UVarLong Roundtrip Tests

    [Test]
    public void Roundtrip_UVarLong_Zero()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteUVarLong(0);
        var reader = new BufferReader(_buffer);
        reader.ReadUVarLong().Should().Be(0);
    }

    [Test]
    public void Roundtrip_UVarLong_SmallValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteUVarLong(1000);
        var reader = new BufferReader(_buffer);
        reader.ReadUVarLong().Should().Be(1000);
    }

    [Test]
    public void Roundtrip_UVarLong_LargeValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteUVarLong(ulong.MaxValue);
        var reader = new BufferReader(_buffer);
        reader.ReadUVarLong().Should().Be(ulong.MaxValue);
    }

    #endregion

    #region String Roundtrip Tests

    [Test]
    public void Roundtrip_String_Empty()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteString("");
        var reader = new BufferReader(_buffer);
        reader.ReadString().Should().Be("");
    }

    [Test]
    public void Roundtrip_String_Simple()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteString("Hello, World!");
        var reader = new BufferReader(_buffer);
        reader.ReadString().Should().Be("Hello, World!");
    }

    [Test]
    public void Roundtrip_String_Unicode()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteString("Hello, 世界！🌍");
        var reader = new BufferReader(_buffer);
        reader.ReadString().Should().Be("Hello, 世界！🌍");
    }

    #endregion

    #region VarString Roundtrip Tests

    [Test]
    public void Roundtrip_VarString_Empty()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteVarString("");
        var reader = new BufferReader(_buffer);
        reader.ReadVarString().Should().Be("");
    }

    [Test]
    public void Roundtrip_VarString_Simple()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteVarString("Hello, World!");
        var reader = new BufferReader(_buffer);
        reader.ReadVarString().Should().Be("Hello, World!");
    }

    [Test]
    public void Roundtrip_VarString_Unicode()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteVarString("Hello, 世界！🌍");
        var reader = new BufferReader(_buffer);
        reader.ReadVarString().Should().Be("Hello, 世界！🌍");
    }

    #endregion

    #region Guid Roundtrip Tests

    [Test]
    public void Roundtrip_Guid_Empty()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteGuid(Guid.Empty);
        var reader = new BufferReader(_buffer);
        reader.ReadGuid().Should().Be(Guid.Empty);
    }

    [Test]
    public void Roundtrip_Guid_Valid()
    {
        var guid = new Guid("00010203-0405-0607-0809-0A0B0C0D0E0F");
        var writer = new BufferWriter(_buffer);
        writer.WriteGuid(guid);
        var reader = new BufferReader(_buffer);
        reader.ReadGuid().Should().Be(guid);
    }

    #endregion

    #region Bool Roundtrip Tests

    [Test]
    public void Roundtrip_Bool_True()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteBool(true);
        var reader = new BufferReader(_buffer);
        reader.ReadBool().Should().BeTrue();
    }

    [Test]
    public void Roundtrip_Bool_False()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteBool(false);
        var reader = new BufferReader(_buffer);
        reader.ReadBool().Should().BeFalse();
    }

    #endregion

    #region Length Roundtrip Tests

    [Test]
    public void Roundtrip_Length_Zero()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteLength(0);
        var reader = new BufferReader(_buffer);
        reader.ReadLength().Should().Be(0);
    }

    [Test]
    public void Roundtrip_Length_PositiveValue()
    {
        var writer = new BufferWriter(_buffer);
        writer.WriteLength(100);
        var reader = new BufferReader(_buffer);
        reader.ReadLength().Should().Be(100);
    }

    #endregion

    #region Complex Roundtrip Tests

    [Test]
    public void Roundtrip_ComplexMessage()
    {
        var writer = new BufferWriter(_buffer);

        writer.WriteByte(42);
        writer.WriteShort(1000);
        writer.WriteInt(0x12345678);
        writer.WriteLong(0x123456789ABCDEF0L);
        writer.WriteVarInt(1000000);
        writer.WriteVarLong(10000000000L);
        writer.WriteString("Hello");
        writer.WriteGuid(Guid.NewGuid());

        var reader = new BufferReader(_buffer);
        reader.ReadByte().Should().Be(42);
        reader.ReadInt16BigEndian().Should().Be(1000);
        reader.ReadInt32BigEndian().Should().Be(0x12345678);
        reader.ReadInt64BigEndian().Should().Be(0x123456789ABCDEF0L);
        reader.ReadVarInt().Should().Be(1000000);
        reader.ReadVarLong().Should().Be(10000000000L);
        reader.ReadString().Should().Be("Hello");
    }

    [Test]
    public void Roundtrip_MultipleSequences()
    {
        var writer = new BufferWriter(_buffer);

        writer.WriteInt(1);
        writer.WriteInt(2);
        writer.WriteInt(3);
        writer.WriteLong(4);
        writer.WriteLong(5);

        var reader = new BufferReader(_buffer);
        reader.ReadInt32BigEndian().Should().Be(1);
        reader.ReadInt32BigEndian().Should().Be(2);
        reader.ReadInt32BigEndian().Should().Be(3);
        reader.ReadInt64BigEndian().Should().Be(4);
        reader.ReadInt64BigEndian().Should().Be(5);
    }

    #endregion
}
