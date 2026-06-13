using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace nKafka.Client;

public static class KafkaMetrics
{
    public static readonly Meter Meter = new("nKafka");

    public static readonly Histogram<double> FetchRoundTripMs = Meter.CreateHistogram<double>("nKafka.fetch.round_trip_ms");
    public static readonly Histogram<double> DeserializeTimeMs = Meter.CreateHistogram<double>("nKafka.deserialize.time_ms");
    public static readonly Histogram<double> CommitTimeMs = Meter.CreateHistogram<double>("nKafka.commit.time_ms");
    public static readonly Histogram<double> HeartbeatTimeMs = Meter.CreateHistogram<double>("nKafka.heartbeat.time_ms");
    public static readonly Counter<long> MessagesConsumed = Meter.CreateCounter<long>("nKafka.messages.consumed");
    public static readonly Counter<long> BytesReceived = Meter.CreateCounter<long>("nKafka.bytes.received");
    public static readonly Counter<long> Fetches = Meter.CreateCounter<long>("nKafka.fetches");
    public static readonly Counter<long> Heartbeats = Meter.CreateCounter<long>("nKafka.heartbeats");
    public static readonly Counter<long> Commits = Meter.CreateCounter<long>("nKafka.commits");
}
