using nKafka.Contracts.MessageDefinitions.ConsumerProtocolAssignmentNested;

namespace nKafka.Client;

public interface IGroupCoordinator : IAsyncDisposable
{
    ValueTask JoinGroupAsync(CancellationToken cancellationToken);

    ValueTask LeaveGroupAsync(CancellationToken cancellationToken);

    int GenerationId { get; }

    string MemberId { get; }

    bool IsLeader { get; }

    string? LeaderId { get; }

    IDictionary<string, TopicPartition>? AssignedPartitions { get; }

    event Action? RebalanceRequired;
}
