﻿namespace nKafka.Contracts.Generator.Definitions;

public class MessageDefinition
{
    public short? ApiKey { get; set; }
    public string? Name { get; set; }
    public string? Type { get; set; }
    public List<string> Listeners { get; set; } = [];
    public VersionRange? ValidVersions { get; set; }
    public VersionRange? DeprecatedVersions { get; set; }
    public VersionRange? FlexibleVersions { get; set; }
    public List<FieldDefinition> Fields { get; set; } = new();
    public List<CommonStructDefinition> CommonStructs { get; set; } = [];
}