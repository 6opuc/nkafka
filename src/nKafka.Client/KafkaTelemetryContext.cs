namespace nKafka.Client;

public readonly record struct KafkaTelemetryContext(
    string ConsumerGroupId,
    string ClientId,
    string? ServerAddress,
    int? ServerPort,
    string? TopicName,
    string? PartitionId);
