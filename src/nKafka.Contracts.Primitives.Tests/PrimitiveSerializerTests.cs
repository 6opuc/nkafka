using FluentAssertions;

namespace nKafka.Contracts.Primitives.Tests;

public class PrimitiveSerializerTests
{
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