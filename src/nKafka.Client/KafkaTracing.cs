using System.Diagnostics;

namespace nKafka.Client;

public static class KafkaTracing
{
    public const string InstrumentName = "nKafka";
    public static readonly ActivitySource Source = new(InstrumentName, "1.0.0");

    public static void AddMessagingAttributes(this Activity activity, KafkaTelemetryContext context, string operationName)
    {
        activity?.AddTag("messaging.system", "kafka");
        activity?.AddTag("messaging.operation.name", operationName);
        activity?.AddTag("messaging.operation.type", operationName);
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
}
