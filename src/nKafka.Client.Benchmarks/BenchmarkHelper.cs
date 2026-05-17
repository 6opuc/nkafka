namespace nKafka.Client.Benchmarks;

internal static class BenchmarkHelper
{
    internal static readonly string CACertPath = ResolvePath();

    internal static string GetCACertPath() => CACertPath;

    private static string ResolvePath()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null && !dir.EnumerateDirectories("infra").Any())
        {
            dir = dir.Parent;
        }
        if (dir == null)
        {
            throw new InvalidOperationException(
                "Could not locate 'infra' directory to resolve CA cert path.");
        }
        return Path.Combine(dir.FullName, "infra", "secrets", "ca-cert.pem");
    }
}
