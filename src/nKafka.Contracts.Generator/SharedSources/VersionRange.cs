using System.Collections;

namespace nKafka.Contracts;

public readonly struct VersionRange : IEnumerable<short>
{
    public static readonly VersionRange None = new VersionRange();

    public short? From { get; }
    public short? To { get; }
    public bool IsNone => From == null;


    public VersionRange(short from, short? to = null)
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

        if (!short.TryParse(versions.First(), out var from))
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

        if (!short.TryParse(versions.Last(), out var to))
        {
            return false;
        }

        result = new VersionRange(from, to);
        return true;
    }

    public bool Includes(short version)
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

    public VersionRange Intersect(VersionRange other)
    {
        if (IsNone)
        {
            return None;
        }

        if (other.IsNone)
        {
            return None;
        }
        
        var from = Math.Max(From ?? 0, other.From ?? 0);
        var to = (To ?? short.MaxValue) > (other.To ?? short.MaxValue)
            ? other.To
            : To;

        return new VersionRange(from, to);
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public IEnumerator<short> GetEnumerator()
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