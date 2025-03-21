using FluentAssertions;

namespace nKafka.Contracts.Tests;

public class PrimitiveSerializerTests
{
    #region Strings

    [Test]
    [TestCaseSource(nameof(GetStringCases))]
    public void SerializeString_ByTestCase_ProducesExpectedOutput(SerializeTestCase<string?> testCase)
    {
        using var stream = new MemoryStream(0);
        PrimitiveSerializer.SerializeString(stream, testCase.Value);

        var actual = stream.ToArray();

        actual.Should().BeEquivalentTo(testCase.Bytes);
    }

    [Test]
    [TestCaseSource(nameof(GetStringCases))]
    public void DeserializeString_ByTestCase_ProducesExpectedOutput(SerializeTestCase<string?> testCase)
    {
        using var stream = new MemoryStream(testCase.Bytes, 0, testCase.Bytes.Length, false, true);
        var actual = PrimitiveSerializer.DeserializeString(stream);

        actual.Should().BeEquivalentTo(testCase.Value);
    }

    public static IEnumerable<SerializeTestCase<string?>> GetStringCases()
    {
        yield return new SerializeTestCase<string?>("ABCDE", [0, 5, 65, 66, 67, 68, 69]);
        yield return new SerializeTestCase<string?>(null, [255, 255]);
        yield return new SerializeTestCase<string?>(string.Empty, [0, 0]);
    }

    [Test]
    [TestCaseSource(nameof(GetVarStringCases))]
    public void SerializeVarString_ByTestCase_ProducesExpectedOutput(SerializeTestCase<string?> testCase)
    {
        using var stream = new MemoryStream(0);
        PrimitiveSerializer.SerializeVarString(stream, testCase.Value);

        var actual = stream.ToArray();

        actual.Should().BeEquivalentTo(testCase.Bytes);
    }

    [Test]
    [TestCaseSource(nameof(GetVarStringCases))]
    public void DeserializeVarString_ByTestCase_ProducesExpectedOutput(SerializeTestCase<string?> testCase)
    {
        using var stream = new MemoryStream(testCase.Bytes, 0, testCase.Bytes.Length, false, true);
        var actual = PrimitiveSerializer.DeserializeVarString(stream);

        actual.Should().BeEquivalentTo(testCase.Value);
    }

    public static IEnumerable<SerializeTestCase<string?>> GetVarStringCases()
    {
        yield return new SerializeTestCase<string?>("ABCDE", [6, 65, 66, 67, 68, 69]);
        yield return new SerializeTestCase<string?>(null, [0]);
        yield return new SerializeTestCase<string?>(string.Empty, [1]);
    }

    #endregion Strings

    #region VarLong

    [Test]
    [TestCaseSource(nameof(GetVarLongCases))]
    public void SerializeVarLong_ByTestCase_ProducesExpectedOutput(SerializeTestCase<long> testCase)
    {
        using var stream = new MemoryStream(0);
        PrimitiveSerializer.SerializeVarLong(stream, testCase.Value);

        var actual = stream.ToArray();

        actual.Should().BeEquivalentTo(testCase.Bytes);
    }

    [Test]
    [TestCaseSource(nameof(GetVarLongCases))]
    public void DeserializeVarLong_ByTestCase_ProducesExpectedOutput(SerializeTestCase<long> testCase)
    {
        using var stream = new MemoryStream(testCase.Bytes, 0, testCase.Bytes.Length, false, true);
        var actual = PrimitiveSerializer.DeserializeVarLong(stream);

        actual.Should().Be(testCase.Value);
    }

    public static IEnumerable<SerializeTestCase<long>> GetVarLongCases()
    {
        yield return new SerializeTestCase<long>(0L, [0x00]);
        yield return new SerializeTestCase<long>(1L, [0x02]);
        yield return new SerializeTestCase<long>(-1L, [0x01]);
        yield return new SerializeTestCase<long>(-64L, [0x7f]);
        yield return new SerializeTestCase<long>(127, [0xfe, 0x01]);
        yield return new SerializeTestCase<long>(300, [216, 4]);
        yield return new SerializeTestCase<long>(123456789, [0xAA, 0xB4, 0xDE, 0x75]);
        yield return new SerializeTestCase<long>(int.MaxValue-1,
            [0xFC, 0xFF, 0xFF, 0xFF, 0x0F]);
        yield return new SerializeTestCase<long>(int.MaxValue,
            [0xFE, 0xFF, 0xFF, 0xFF, 0x0F]);
        yield return new SerializeTestCase<long>((long)int.MaxValue+1,
            [0x80, 0x80, 0x80, 0x80, 0x10]);
        yield return new SerializeTestCase<long>(long.MaxValue-1,
            [0xfc, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]);
        yield return new SerializeTestCase<long>(long.MaxValue,
            [0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]);
    }

    #endregion VarLong
    
    #region UVarLong

    [Test]
    [TestCaseSource(nameof(GetUVarLongCases))]
    public void SerializeUVarLong_ByTestCase_ProducesExpectedOutput(SerializeTestCase<ulong> testCase)
    {
        using var stream = new MemoryStream(0);
        PrimitiveSerializer.SerializeUVarLong(stream, testCase.Value);

        var actual = stream.ToArray();

        actual.Should().BeEquivalentTo(testCase.Bytes);
    }

    [Test]
    [TestCaseSource(nameof(GetUVarLongCases))]
    public void DeserializeUVarLong_ByTestCase_ProducesExpectedOutput(SerializeTestCase<ulong> testCase)
    {
        using var stream = new MemoryStream(testCase.Bytes, 0, testCase.Bytes.Length, false, true);
        var actual = PrimitiveSerializer.DeserializeUVarLong(stream);

        actual.Should().Be(testCase.Value);
    }

    public static IEnumerable<SerializeTestCase<ulong>> GetUVarLongCases()
    {
        yield return new SerializeTestCase<ulong>(0L, [0x00]);
        yield return new SerializeTestCase<ulong>(1L, [0x01]);
        yield return new SerializeTestCase<ulong>(127, [0x7f]);
        yield return new SerializeTestCase<ulong>(300, [0xAC, 0x02]);
        yield return new SerializeTestCase<ulong>(123456789, [0x95, 0x9A, 0xEF, 0x3A]);
        yield return new SerializeTestCase<ulong>(uint.MaxValue-1,
            [0xFE, 0xFF, 0xFF, 0xFF, 0x0F]);
        yield return new SerializeTestCase<ulong>(uint.MaxValue,
            [0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
        yield return new SerializeTestCase<ulong>((ulong)uint.MaxValue+1,
            [0x80, 0x80, 0x80, 0x80, 0x10]);
        yield return new SerializeTestCase<ulong>(ulong.MaxValue-1,
            [0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]);
        yield return new SerializeTestCase<ulong>(ulong.MaxValue,
            [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]);
    }

    #endregion UVarLong

    #region Int

    [Test]
    [TestCaseSource(nameof(GetIntCases))]
    public void SerializeInt_ByTestCase_ProducesExpectedOutput(SerializeTestCase<int> testCase)
    {
        using var stream = new MemoryStream(0);
        PrimitiveSerializer.SerializeInt(stream, testCase.Value);

        var actual = stream.ToArray();

        actual.Should().BeEquivalentTo(testCase.Bytes);
    }

    [Test]
    [TestCaseSource(nameof(GetIntCases))]
    public void DeserializeInt_ByTestCase_ProducesExpectedOutput(SerializeTestCase<int> testCase)
    {
        using var stream = new MemoryStream(testCase.Bytes, 0, testCase.Bytes.Length, false, true);
        var actual = PrimitiveSerializer.DeserializeInt(stream);

        actual.Should().Be(testCase.Value);
    }

    public static IEnumerable<SerializeTestCase<int>> GetIntCases()
    {
        yield return new SerializeTestCase<int>(0, [0x00, 0x00, 0x00, 0x00]);
        yield return new SerializeTestCase<int>(1, [0x00, 0x00, 0x00, 0x01]);
        yield return new SerializeTestCase<int>(-1, [0xFF, 0xFF, 0xFF, 0xFF]);
        yield return new SerializeTestCase<int>(-64, [0xFF, 0xFF, 0xFF, 0xC0]);
        yield return new SerializeTestCase<int>(127, [0x00, 0x00, 0x00, 0x7F]);
        yield return new SerializeTestCase<int>(300, [0x00, 0x00, 0x01, 0x2C]);
        yield return new SerializeTestCase<int>(int.MaxValue, [0x7F, 0xFF, 0xFF, 0xFF]);
    }

    #endregion Int
    
    #region Long

    [Test]
    [TestCaseSource(nameof(GetLongCases))]
    public void SerializeLong_ByTestCase_ProducesExpectedOutput(SerializeTestCase<long> testCase)
    {
        using var stream = new MemoryStream(0);
        PrimitiveSerializer.SerializeLong(stream, testCase.Value);

        var actual = stream.ToArray();

        actual.Should().BeEquivalentTo(testCase.Bytes);
    }

    [Test]
    [TestCaseSource(nameof(GetLongCases))]
    public void DeserializeLong_ByTestCase_ProducesExpectedOutput(SerializeTestCase<long> testCase)
    {
        using var stream = new MemoryStream(testCase.Bytes, 0, testCase.Bytes.Length, false, true);
        var actual = PrimitiveSerializer.DeserializeLong(stream);

        actual.Should().Be(testCase.Value);
    }

    public static IEnumerable<SerializeTestCase<long>> GetLongCases()
    {
        yield return new SerializeTestCase<long>(0, [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        yield return new SerializeTestCase<long>(1, [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]);
        yield return new SerializeTestCase<long>(-1, [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
        yield return new SerializeTestCase<long>(-64, [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xC0]);
        yield return new SerializeTestCase<long>(127, [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7F]);
        yield return new SerializeTestCase<long>(300, [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x2C]);
        yield return new SerializeTestCase<long>(int.MaxValue, [0x00, 0x00, 0x00, 0x00, 0x7F, 0xFF, 0xFF, 0xFF]);
        yield return new SerializeTestCase<long>(long.MaxValue, [0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
    }

    #endregion Long
    
    #region Double

    [Test]
    [TestCaseSource(nameof(GetDoubleCases))]
    public void SerializeDouble_ByTestCase_ProducesExpectedOutput(SerializeTestCase<double> testCase)
    {
        using var stream = new MemoryStream(0);
        PrimitiveSerializer.SerializeDouble(stream, testCase.Value);

        var actual = stream.ToArray();

        actual.Should().BeEquivalentTo(testCase.Bytes);
    }

    [Test]
    [TestCaseSource(nameof(GetDoubleCases))]
    public void DeserializeDouble_ByTestCase_ProducesExpectedOutput(SerializeTestCase<double> testCase)
    {
        using var stream = new MemoryStream(testCase.Bytes, 0, testCase.Bytes.Length, false, true);
        var actual = PrimitiveSerializer.DeserializeDouble(stream);

        actual.Should().Be(testCase.Value);
    }

    public static IEnumerable<SerializeTestCase<double>> GetDoubleCases()
    {
        
        yield return new SerializeTestCase<double>(0, [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        yield return new SerializeTestCase<double>(1, [0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        yield return new SerializeTestCase<double>(-1, [0xBF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        yield return new SerializeTestCase<double>(-64, [0xC0, 0x50, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        yield return new SerializeTestCase<double>(127, [0x40, 0x5F, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00]);
        yield return new SerializeTestCase<double>(300, [0x40, 0x72, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00]);
        yield return new SerializeTestCase<double>(int.MaxValue, [0x41, 0xDF, 0xFF, 0xFF, 0xFF, 0xC0, 0x00, 0x00]);
        yield return new SerializeTestCase<double>(long.MaxValue, [0x43, 0xE0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        yield return new SerializeTestCase<double>(double.MaxValue, [0x7F, 0xEF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
        
        yield break;
    }

    #endregion Double
    
    #region Guid

    [Test]
    [TestCaseSource(nameof(GetGuidCases))]
    public void SerializeGuid_ByTestCase_ProducesExpectedOutput(SerializeTestCase<Guid> testCase)
    {
        using var stream = new MemoryStream(0);
        PrimitiveSerializer.SerializeGuid(stream, testCase.Value);

        var actual = stream.ToArray();

        actual.Should().BeEquivalentTo(testCase.Bytes);
    }

    [Test]
    [TestCaseSource(nameof(GetGuidCases))]
    public void DeserializeGuid_ByTestCase_ProducesExpectedOutput(SerializeTestCase<Guid> testCase)
    {
        using var stream = new MemoryStream(testCase.Bytes, 0, testCase.Bytes.Length, false, true);
        var actual = PrimitiveSerializer.DeserializeGuid(stream);

        actual.Should().Be(testCase.Value);
    }

    public static IEnumerable<SerializeTestCase<Guid>> GetGuidCases()
    {
        yield return new SerializeTestCase<Guid>(new Guid("f3c3e4cc-09d9-8f40-8194-17b59016aba8"), [0xCC, 0xE4, 0xC3, 0xF3, 0xD9, 0x09, 0x40, 0x8F, 0x81, 0x94, 0x17, 0xB5, 0x90, 0x16, 0xAB, 0xA8]);
    }

    #endregion Guid
    
    public class SerializeTestCase<T>
    {
        public T Value { get; }
        public byte[] Bytes { get; }


        public SerializeTestCase(T value, byte[] bytes)
        {
            Value = value;
            Bytes = bytes;
        }

        public override string ToString()
        {
            return Value?.ToString() ?? "{null}";
        }
    }
}