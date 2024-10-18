using System.Collections;

namespace nKafka.Contracts;

public readonly struct VersionRange : IEnumerable<int>
{
    public static readonly VersionRange None = new VersionRange();

    public int? From { get; }
    public int? To { get; }
    public bool IsNone => From == null;


    public VersionRange(int from, int? to = null)
    {
        From = from;
        To = to;
    }

    public static bool TryParse(string s, out VersionRange result)
    {
        result = None;

        if (string.Equals(s, "none", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        char[] delimiters = ['-', '+'];

        var versions = s.Split(
            delimiters,
            StringSplitOptions.RemoveEmptyEntries);

        if (versions.Length > 2)
        {
            return false;
        }

        if (!int.TryParse(versions.First(), out var from))
        {
            return false;
        }

        if (versions.Length == 1)
        {
            result = delimiters.Any(s.Contains)
                ? new VersionRange(from)
                : new VersionRange(from, from);

            return true;
        }

        if (!int.TryParse(versions.Last(), out var to))
        {
            return false;
        }

        result = new VersionRange(from, to);
        return true;
    }

    public bool Includes(int version)
    {
        if (IsNone)
        {
            return false;
        }

        if (From > version || To < version)
        {
            return false;
        }

        return true;
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public IEnumerator<int> GetEnumerator()
    {
        if (!To.HasValue || !From.HasValue)
        {
            yield break;
        }

        for (var v = From.Value; v <= To.Value; v++)
        {
            yield return v;
        }
    }

    public override string ToString()
    {
        if (IsNone)
        {
            return "none";
        }
        
        if (!To.HasValue)
        {
            return $"{From}+";
        }

        if (To == From)
        {
            return $"{From}";
        }
        
        return $"{From}-{To}";
    }
}