using nKafka.Contracts.Primitives;

namespace nKafka.Contracts.Generator.Definitions;

public static class VersionRangeExtensions
{
    public static string ToLiteral(this VersionRange versions)
    {
        return versions.IsNone
            ? "VersionRange.None"
            : versions.To != null
                ? $"new VersionRange({versions.From}, {versions.To})"
                : $"new VersionRange({versions.From}, null)";
    }
}