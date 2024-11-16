using Microsoft.Extensions.Logging;

namespace nKafka.Client.IntegrationTests;

public class TestLoggerFactory : ILoggerFactory
{
    public static TestLoggerFactory Instance { get; } = new ();
    
    public void Dispose()
    {
    }

    public ILogger CreateLogger(string categoryName)
    {
        return new TestLogger();
    }

    public void AddProvider(ILoggerProvider provider)
    {
    }
}