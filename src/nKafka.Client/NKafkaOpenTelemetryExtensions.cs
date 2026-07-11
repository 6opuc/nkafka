using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace nKafka.Client;

public static class NKafkaOpenTelemetryExtensions
{
    public static MeterProviderBuilder AddNKafka(this MeterProviderBuilder builder)
    {
        KafkaMetrics.Enabled = true;
        return builder.AddMeter(KafkaMetrics.Meter.Name);
    }

    public static TracerProviderBuilder AddNKafka(this TracerProviderBuilder builder)
    {
        return builder.AddSource(KafkaTracing.InstrumentName);
    }
}
