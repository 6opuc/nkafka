using Microsoft.Extensions.Logging;

namespace nKafka.Client.IntegrationTests;

public class TestLogger : ILogger, IDisposable
{
    private readonly Action<string> _output = TestContext.Progress.WriteLine;

    public void Dispose()
    {
    }

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
        Func<TState, Exception?, string> formatter) => _output(formatter(state, exception));

    public bool IsEnabled(LogLevel logLevel) => true;

    public IDisposable BeginScope<TState>(TState state) where TState : notnull => this;
}