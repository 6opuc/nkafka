namespace nKafka.Contracts.Generator.Definitions;

public class CommonStructDefinition
{
    public string? Name { get; set; }
    public VersionRange Versions { get; set; }
    public List<FieldDefinition> Fields { get; set; } = [];
}