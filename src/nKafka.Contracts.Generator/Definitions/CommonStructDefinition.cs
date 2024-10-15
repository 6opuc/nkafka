namespace nKafka.Contracts.Generator.Definitions;

public class CommonStructDefinition
{
    public string? Name { get; set; }
    public string? Versions { get; set; }
    public List<FieldDefinition> Fields { get; set; } = [];
}