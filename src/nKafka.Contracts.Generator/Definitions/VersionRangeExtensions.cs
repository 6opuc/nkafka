using nKafka.Contracts.Primitives;

namespace nKafka.Contracts.Generator.Definitions;

public static class VersionRangeExtensions
{
    public static string ToLiteral(this VersionRange? versions)
    {
        return versions == null || versions.Value.IsEmpty
            ? "VersionRange.Empty"
            : $"new VersionRange({versions.Value.From}, {versions.Value.To})";
    }
}