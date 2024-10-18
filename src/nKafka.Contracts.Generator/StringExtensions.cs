namespace nKafka.Contracts.Generator;

public static class StringExtensions
{
    public static string? FirstCharToLowerCase(this string? str)
    {
        if ( !string.IsNullOrEmpty(str) && char.IsUpper(str[0]))
            return str.Length == 1 ? char.ToLower(str[0]).ToString() : char.ToLower(str[0]) + str.Substring(1);

        return str;
    }
}