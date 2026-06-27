using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace nKafka.Client;

public static class KafkaMetrics
{
    public static bool Enabled { get; set; }

    public const string MessagingSystem = "kafka";
    public const string OperationReceive = "receive";
    public const string OperationProcess = "process";
    public const string OperationSettle = "settle";

    public static readonly Meter Meter = new("messaging");

    public static readonly Histogram<double> ClientOperationDurationMs =
        Meter.CreateHistogram<double>("messaging.client.operation.duration");

    public static readonly Histogram<double> ProcessDurationMs =
        Meter.CreateHistogram<double>("messaging.process.duration");

    public static readonly Counter<long> MessagesConsumed =
        Meter.CreateCounter<long>("messaging.client.consumed.messages");

    public static void RecordClientOperation(KafkaTelemetryContext context, string operationName, double durationMs, string? errorType = null)
    {
        if (!Enabled)
        {
            return;
        }

        var tags = new TagList
        {
            { "messaging.operation.name", operationName },
            { "messaging.operation.type", operationName },
            { "messaging.system", MessagingSystem },
            { "messaging.consumer.group.name", context.ConsumerGroupId },
            { "messaging.client.id", context.ClientId },
        };

        if (context.ServerAddress != null)
        {
            tags.Add("server.address", context.ServerAddress);
        }

        if (context.ServerPort != null)
        {
            tags.Add("server.port", context.ServerPort.Value);
        }

        if (context.TopicName != null)
        {
            tags.Add("messaging.destination.name", context.TopicName);
        }

        if (context.PartitionId != null)
        {
            tags.Add("messaging.destination.partition.id", context.PartitionId);
        }

        if (errorType != null)
        {
            tags.Add("error.type", errorType);
        }

        ClientOperationDurationMs.Record(durationMs, tags);
    }

    public static void RecordProcessDuration(KafkaTelemetryContext context, string operationName, double durationMs, string? errorType = null)
    {
        if (!Enabled)
        {
            return;
        }

        var tags = new TagList
        {
            { "messaging.operation.name", operationName },
            { "messaging.operation.type", operationName },
            { "messaging.system", MessagingSystem },
            { "messaging.consumer.group.name", context.ConsumerGroupId },
            { "messaging.client.id", context.ClientId },
        };

        if (context.ServerAddress != null)
        {
            tags.Add("server.address", context.ServerAddress);
        }

        if (context.ServerPort != null)
        {
            tags.Add("server.port", context.ServerPort.Value);
        }

        if (context.TopicName != null)
        {
            tags.Add("messaging.destination.name", context.TopicName);
        }

        if (context.PartitionId != null)
        {
            tags.Add("messaging.destination.partition.id", context.PartitionId);
        }

        if (errorType != null)
        {
            tags.Add("error.type", errorType);
        }

        ProcessDurationMs.Record(durationMs, tags);
    }

    public static void AddMessagesConsumed(KafkaTelemetryContext context, long count, string operationName = OperationReceive, string? errorType = null)
    {
        if (!Enabled)
        {
            return;
        }

        var tags = new TagList
        {
            { "messaging.operation.name", operationName },
            { "messaging.operation.type", operationName },
            { "messaging.system", MessagingSystem },
            { "messaging.consumer.group.name", context.ConsumerGroupId },
            { "messaging.client.id", context.ClientId },
        };

        if (context.ServerAddress != null)
        {
            tags.Add("server.address", context.ServerAddress);
        }

        if (context.ServerPort != null)
        {
            tags.Add("server.port", context.ServerPort.Value);
        }

        if (context.TopicName != null)
        {
            tags.Add("messaging.destination.name", context.TopicName);
        }

        if (context.PartitionId != null)
        {
            tags.Add("messaging.destination.partition.id", context.PartitionId);
        }

        if (errorType != null)
        {
            tags.Add("error.type", errorType);
        }

        MessagesConsumed.Add(count, tags);
    }
}
