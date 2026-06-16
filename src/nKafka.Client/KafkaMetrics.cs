using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace nKafka.Client;

public static class KafkaMetrics
{
    public const string MessagingSystem = "kafka";
    public const string OperationReceive = "receive";
    public const string OperationProcess = "process";
    public const string OperationAck = "ack";
    public const string OperationHeartbeat = "heartbeat";

    public static readonly Meter Meter = new("messaging");

    public static readonly Histogram<double> ClientOperationDurationMs =
        Meter.CreateHistogram<double>("messaging.client.operation.duration");

    public static readonly Histogram<double> ProcessDurationMs =
        Meter.CreateHistogram<double>("messaging.process.duration");

    public static readonly Counter<long> MessagesConsumed =
        Meter.CreateCounter<long>("messaging.client.consumed.messages");

    public static void RecordClientOperation(string operationName, double durationMs, string? consumerGroup = null, string? serverAddress = null, int? serverPort = null, string? partitionId = null, string? errorType = null)
    {
        var tags = new TagList
        {
            { "messaging.operation.name", operationName },
            { "messaging.system", MessagingSystem },
        };

        if (consumerGroup != null) tags.Add("messaging.consumer.group.name", consumerGroup);
        if (serverAddress != null) tags.Add("server.address", serverAddress);
        if (serverPort != null) tags.Add("server.port", serverPort.Value);
        if (partitionId != null) tags.Add("messaging.destination.partition.id", partitionId);
        if (errorType != null) tags.Add("error.type", errorType);

        ClientOperationDurationMs.Record(durationMs, tags);
    }

    public static void RecordProcessDuration(string operationName, double durationMs, string? consumerGroup = null, string? serverAddress = null, int? serverPort = null, string? partitionId = null, string? errorType = null)
    {
        var tags = new TagList
        {
            { "messaging.operation.name", operationName },
            { "messaging.system", MessagingSystem },
        };

        if (consumerGroup != null) tags.Add("messaging.consumer.group.name", consumerGroup);
        if (serverAddress != null) tags.Add("server.address", serverAddress);
        if (serverPort != null) tags.Add("server.port", serverPort.Value);
        if (partitionId != null) tags.Add("messaging.destination.partition.id", partitionId);
        if (errorType != null) tags.Add("error.type", errorType);

        ProcessDurationMs.Record(durationMs, tags);
    }

    public static void AddMessagesConsumed(long count, string operationName = OperationReceive, string? consumerGroup = null, string? serverAddress = null, int? serverPort = null, string? partitionId = null, string? errorType = null)
    {
        var tags = new TagList
        {
            { "messaging.operation.name", operationName },
            { "messaging.system", MessagingSystem },
        };

        if (consumerGroup != null) tags.Add("messaging.consumer.group.name", consumerGroup);
        if (serverAddress != null) tags.Add("server.address", serverAddress);
        if (serverPort != null) tags.Add("server.port", serverPort.Value);
        if (partitionId != null) tags.Add("messaging.destination.partition.id", partitionId);
        if (errorType != null) tags.Add("error.type", errorType);

        MessagesConsumed.Add(count, tags);
    }
}
