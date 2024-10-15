using System.Text.Json;
using System.Text.Json.Serialization;
using nKafka.Contracts.Primitives;

namespace nKafka.Contracts.Generator.Definitions;

public class VersionRangeConverter : JsonConverter<VersionRange>
{
    public override VersionRange Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        reader.TokenType switch
        {
            JsonTokenType.Number => new VersionRange(reader.GetInt32()),
            JsonTokenType.String => VersionRange.TryParse(reader.GetString() ?? string.Empty, out var result)
                ? result
                : throw new InvalidOperationException(
                    $"Token {reader.TokenType} can not be converted to int."),
            _ => throw new InvalidOperationException(
                $"Token {reader.TokenType} can not be converted to int.")
        };

    public override void Write(Utf8JsonWriter writer, VersionRange value, JsonSerializerOptions options)
    {
        writer.WriteStringValue(value.ToString());
    }
}