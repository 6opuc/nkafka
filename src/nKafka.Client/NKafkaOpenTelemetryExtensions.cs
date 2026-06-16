using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace nKafka.Client;

public static class NKafkaOpenTelemetryExtensions
{
    public static MeterProviderBuilder AddNKafka(this MeterProviderBuilder builder)
    {
        return builder.AddMeter(KafkaMetrics.Meter.Name);
    }

    public static TracerProviderBuilder AddNKafka(this TracerProviderBuilder builder)
    {
        return builder.AddSource(KafkaTracing.InstrumentName);
    }
}
