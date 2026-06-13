using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace nKafka.Client;

public static class KafkaTracing
{
    public const string InstrumentName = "nKafka";
    public static readonly ActivitySource Source = new(InstrumentName, "1.0.0");

    public const string ActivityJoinGroup = "join_group";
    public const string ActivityBootstrapConnection = "bootstrap_connection";
    public const string ActivityRefreshMetadata = "refresh_metadata";
    public const string ActivityJoinGroupRequest = "join_group_request";
    public const string ActivitySyncGroup = "sync_group";
    public const string ActivityRefreshCoordinator = "refresh_coordinator";
    public const string ActivityRequestMetadata = "request_metadata";
    public const string ActivityFindCoordinator = "find_coordinator";
    public const string ActivityHeartbeat = "heartbeat";
    public const string ActivityFetch = "fetch";
    public const string ActivityFetchWait = "fetch_wait";
    public const string ActivityDeserialize = "deserialize";
    public const string ActivityShutdown = "shutdown";
    public const string ActivityStopFetching = "stop_fetching";
    public const string ActivityLeaveGroup = "leave_group";

    public static void AddMessagingAttributes(
        this Activity activity,
        KafkaTelemetryContext context)
    {
        activity?.AddTag("messaging.system", KafkaMetrics.MessagingSystem);
        activity?.AddTag("messaging.operation.name", activity.OperationName);
        activity?.AddTag("messaging.consumer.group.name", context.ConsumerGroupId);
        activity?.AddTag("messaging.client.id", context.ClientId);

        if (context.TopicName != null)
        {
            activity?.AddTag("messaging.destination.name", context.TopicName);
        }

        if (context.PartitionId != null)
        {
            activity?.AddTag("messaging.destination.partition.id", context.PartitionId);
        }
    }

    public static string BuildSpanName(string operationName, KafkaTelemetryContext context)
    {
        return context.TopicName != null
            ? $"{operationName} {context.TopicName}"
            : operationName;
    }

    public static Activity? StartAsCurrent(string? name = null, ActivityKind kind = ActivityKind.Client, [CallerMemberName] string? caller = null)
    {
        var activityName = name ?? caller;
        if (string.IsNullOrEmpty(activityName))
        {
            return null;
        }
        return Source.StartActivity(activityName, kind);
    }
}
