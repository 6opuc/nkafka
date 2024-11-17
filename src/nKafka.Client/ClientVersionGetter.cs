namespace nKafka.Client;

public static class ClientVersionGetter
{
    private static readonly string? _version = System.Reflection.Assembly.GetExecutingAssembly()?.GetName()?.Version?.ToString();

    public static string Version => _version ?? "unavailable";
}