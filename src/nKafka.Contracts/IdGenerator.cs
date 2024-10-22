namespace nKafka.Contracts;

public static class IdGenerator
{
    private static int _current;

    public static int Next()
    {
        return Interlocked.Increment(ref _current);
    }
}