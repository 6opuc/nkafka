namespace nKafka.Contracts.Generator.Definitions;

public class MessageDefinition
{
    public int ApiKey { get; set; }
    public string? Name { get; set; }
    public string? Type { get; set; }
    public List<string> Listeners { get; set; } = [];
    public string? ValidVersions { get; set; }
    public string? DeprecatedVersions { get; set; }
    public string? FlexibleVersions { get; set; }
    public List<FieldDefinition> Fields { get; set; } = new();
    public List<CommonStructDefinition> CommonStructs { get; set; } = [];
}