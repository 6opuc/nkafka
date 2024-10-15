using System.Text.Json;
using System.Text.Json.Serialization;

namespace nKafka.Contracts.Generator.Definitions;

public class MessageDefinitionSerializerOptions
{
    public static readonly JsonSerializerOptions
        Default = new()
        {
            ReadCommentHandling = JsonCommentHandling.Skip,
            PropertyNameCaseInsensitive = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            WriteIndented = true,
            Converters =
            {
                new StringConverter(),
                new BooleanConverter(),
                new IntConverter(),
                new VersionRangeConverter(),
            }
        };
}