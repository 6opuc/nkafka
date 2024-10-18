namespace nKafka.Contracts.Generator;

public static class StringExtensions
{
    public static string? FirstCharToLowerCase(this string? str)
    {
        if (string.IsNullOrEmpty(str))
        {
            return str;
        }

        if (!char.IsUpper(str![0]))
        {
            return str;
        }

        if (str.Length == 1)
        {
            return char.ToLower(str[0]).ToString();
        }

        return char.ToLower(str[0]) + str.Substring(1);
    }
}