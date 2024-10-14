namespace nKafka.Contracts.Generator;

public class CommonStructDefinition
{
    public required string Name { get; set; }
    public required string Versions { get; set; }
    public List<FieldDefinition> Fields { get; set; } = new();
}