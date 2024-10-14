namespace nKafka.Contracts.Generator;

public class MessageDefinition
{
    public int ApiKey { get; set; }
    public required string Name { get; set; }
    public required string Type { get; set; }
    public List<string> Listeners { get; set; } = new();
    public string? ValidVersions { get; set; }
    public string? DeprecatedVersions { get; set; }
    public string? FlexibleVersions { get; set; }
    public List<FieldDefinition> Fields { get; set; } = new();
    public List<CommonStructDefinition> CommonStructs { get; set; }
}