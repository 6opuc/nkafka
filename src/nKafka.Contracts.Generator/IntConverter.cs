using System.Text.Json;
using System.Text.Json.Serialization;

namespace nKafka.Contracts.Generator;

public class IntConverter : JsonConverter<int>
{
    public override int Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        reader.TokenType switch
        {
            JsonTokenType.Number => reader.GetInt32(),
            JsonTokenType.String => int.Parse(reader.GetString() ?? "0"),
            _ => throw new InvalidOperationException(
                $"Token {reader.TokenType} can not be converted to int.")
        };

    public override void Write(Utf8JsonWriter writer, int value, JsonSerializerOptions options)
    {
        writer.WriteNumberValue(value);
    }
}