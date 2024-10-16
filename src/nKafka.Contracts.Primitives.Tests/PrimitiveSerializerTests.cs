using FluentAssertions;

namespace nKafka.Contracts.Primitives.Tests;

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
        using var stream = new MemoryStream(testCase.Bytes, 0, testCase.Bytes.Length, false, true );
        var actual = PrimitiveSerializer.DeserializeString(stream);

        actual.Should().BeEquivalentTo(testCase.Value);
    }

    public static IEnumerable<SerializeTestCase<string?>> GetStringCases()
    {
        yield return new SerializeTestCase<string?>("ABCDE", [0, 5, 65, 66, 67, 68, 69]);
        yield return new SerializeTestCase<string?>(null, [255, 255]);
        yield return new SerializeTestCase<string?>(string.Empty, [0, 0]);
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
        using var stream = new MemoryStream(testCase.Bytes, 0, testCase.Bytes.Length, false, true );
        var actual = PrimitiveSerializer.DeserializeVarLong(stream);

        actual.Should().Be(testCase.Value);
    }

    public static IEnumerable<SerializeTestCase<long>> GetVarLongCases()
    {
        yield return new SerializeTestCase<long>(0L, [0x00]);
        yield return new SerializeTestCase<long>(1L, [0x02]);
        yield return new SerializeTestCase<long>(-1L, [0x01]);
        yield return new SerializeTestCase<long>(-64L, [0x7f]);
        yield return new SerializeTestCase<long>(127, [0xfe, 0x01 ]);
        yield return new SerializeTestCase<long>(long.MaxValue, [0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]);
    }
    
    #endregion VarLong


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