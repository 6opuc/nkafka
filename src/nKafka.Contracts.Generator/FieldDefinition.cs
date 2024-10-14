namespace nKafka.Contracts.Generator;

public class FieldDefinition
{
    public required string Name { get; set; }
    public required string Type { get; set; }
    public string? About { get; set; }
    public string? Versions { get; set; }
    public string? NullableVersions { get; set; }
    public string? FlexibleVersions { get; set; }
    public int? Tag { get; set; }
    public string? TaggedVersions { get; set; }
    public bool Ignorable { get; set; }
    public string? Default { get; set; }
    public string? EntityType { get; set; }
    public bool MapKey { get; set; }
    public List<FieldDefinition> Fields { get; set; } = new();
}