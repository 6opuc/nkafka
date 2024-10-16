using nKafka.Contracts.Primitives;

namespace nKafka.Contracts.Generator.Definitions;

public class FieldDefinition
{
    public string? Name { get; set; }
    public string? Type { get; set; }
    public string? About { get; set; }
    public VersionRange Versions { get; set; }
    public VersionRange NullableVersions { get; set; }
    public VersionRange FlexibleVersions { get; set; }
    public int? Tag { get; set; }
    public VersionRange TaggedVersions { get; set; }
    public bool Ignorable { get; set; }
    public string? Default { get; set; }
    public string? EntityType { get; set; }
    public bool MapKey { get; set; }
    public List<FieldDefinition> Fields { get; set; } = new();

    public override string ToString()
    {
        return $"{Name}: {Type}";
    }
}