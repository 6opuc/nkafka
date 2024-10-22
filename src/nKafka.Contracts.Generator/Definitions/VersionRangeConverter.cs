using System.Text.Json;
using System.Text.Json.Serialization;

namespace nKafka.Contracts.Generator.Definitions;

public class VersionRangeConverter : JsonConverter<VersionRange>
{
    public override VersionRange Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        reader.TokenType switch
        {
            JsonTokenType.None => VersionRange.None,
            JsonTokenType.Null => VersionRange.None,
            JsonTokenType.Number => new VersionRange(reader.GetInt16()),
            JsonTokenType.String => VersionRange.TryParse(reader.GetString() ?? string.Empty, out var result)
                ? result
                : throw new InvalidOperationException(
                    $"Token {reader.TokenType} can not be converted to version range."),
            _ => throw new InvalidOperationException(
                $"Token {reader.TokenType} can not be converted to version range.")
        };

    public override void Write(Utf8JsonWriter writer, VersionRange value, JsonSerializerOptions options)
    {
        writer.WriteStringValue(value.ToString());
    }
}