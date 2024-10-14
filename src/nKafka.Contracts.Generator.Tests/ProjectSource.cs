using System.Runtime.CompilerServices;

namespace nKafka.Contracts.Generator.Tests;

internal static class ProjectSource
{
    private static string CallerFilePath([CallerFilePath] string? callerFilePath = null) =>
        callerFilePath ?? throw new ArgumentNullException(nameof(callerFilePath));

    public static string ProjectDirectory => Path.GetDirectoryName( CallerFilePath() )!;

    public static string RepositoryDirectory => Path.Combine(ProjectDirectory, "../..");
    
    public static string MessageDefinitionsDirectory = Path.Combine(RepositoryDirectory, "apache_kafka_message_definitions");
}