namespace nKafka.Contracts.Generator.Definitions;

public static class VersionRangeExtensions
{
    public static string ToLiteral(this VersionRange? versions)
    {
        if (versions == null)
        {
            return "null";
        }
        
        return versions.Value.IsNone
            ? "VersionRange.None"
            : versions.Value.To != null
                ? $"new VersionRange({versions.Value.From}, {versions.Value.To})"
                : $"new VersionRange({versions.Value.From}, null)";
    }

    public static bool Includes(this VersionRange? versions, short version)
    {
        return versions != null && versions.Value.Includes(version);
    }
}