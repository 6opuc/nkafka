using System.Text.Json;
using System.Text.Json.Serialization;

namespace nKafka.Contracts.Generator.Definitions;

public class BooleanConverter : JsonConverter<bool>
{
    public override bool Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        reader.TokenType switch
        {
            JsonTokenType.String => bool.Parse(reader.GetString() ?? "false"),
            JsonTokenType.True => true,
            JsonTokenType.False => false,
            _ => throw new InvalidOperationException(
                $"Token {reader.TokenType} can not be converted to boolean.")
        };

    public override void Write(Utf8JsonWriter writer, bool value, JsonSerializerOptions options)
    {
        writer.WriteBooleanValue(value);
    }
}