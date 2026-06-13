using System.Diagnostics;

namespace nKafka.Client;

public static class KafkaTracing
{
    public const string InstrumentName = "nKafka";
    public static readonly ActivitySource Source = new(InstrumentName, "1.0.0");
}
