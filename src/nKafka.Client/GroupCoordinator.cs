using System.Text;
using Microsoft.Extensions.Logging;
using nKafka.Contracts;
using nKafka.Contracts.Exceptions;
using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageDefinitions.ConsumerProtocolAssignmentNested;
using nKafka.Contracts.MessageDefinitions.FetchRequestNested;
using nKafka.Contracts.MessageDefinitions.JoinGroupRequestNested;
using nKafka.Contracts.MessageDefinitions.LeaveGroupRequestNested;
using nKafka.Contracts.MessageDefinitions.MetadataRequestNested;
using nKafka.Contracts.MessageDefinitions.MetadataResponseNested;
using nKafka.Contracts.MessageDefinitions.SyncGroupRequestNested;

namespace nKafka.Client;

public sealed class GroupCoordinator : IGroupCoordinator
{
    private readonly ConsumerConfig _config;
    private readonly IOffsetStorage _offsetStorage;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger _logger;
    private readonly KafkaTelemetryContext _context;
    private readonly string[] _topics;

    private Connection? _coordinatorConnection;
    private IDictionary<string, MetadataResponseTopic>? _topicsMetadata;
    private IDictionary<Guid, MetadataResponseTopic>? _topicsMetadataById;
    private readonly Dictionary<int, IConnection> _brokerConnections = new();

    private GroupMembership? _groupMembership;
    private ConsumerProtocolAssignment? _rejoinAssignments;
    private Task? _heartbeatsBackgroundTask;
    private CancellationTokenSource? _heartbeatCts;

    public int GenerationId => _groupMembership?.GenerationId ?? 0;

    public string MemberId => _groupMembership?.MemberId ?? string.Empty;

    public bool IsLeader => _groupMembership?.MemberId == _groupMembership?.LeaderId;

    public string? LeaderId => _groupMembership?.LeaderId;

    public IDictionary<string, TopicPartition>? AssignedPartitions => _rejoinAssignments?.AssignedPartitions;

    public event Action? RebalanceRequired;

    public GroupCoordinator(
        ConsumerConfig config,
        IOffsetStorage offsetStorage,
        ILoggerFactory loggerFactory)
    {
        _config = config;
        _offsetStorage = offsetStorage;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<GroupCoordinator>();
        _context = new KafkaTelemetryContext(
            config.GroupId,
            config.InstanceId ?? config.ClientId,
            config.BootstrapServers.Split(",", StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries).FirstOrDefault(),
            null,
            null,
            null);
        _topics = config.Topics.Split(",", StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
    }

    public async ValueTask JoinGroupAsync(CancellationToken cancellationToken)
    {
        await RefreshMetadataAsync(cancellationToken);
        await RejoinGroupAsync(cancellationToken);
    }

    public async ValueTask LeaveGroupAsync(CancellationToken cancellationToken)
    {
        if (_groupMembership == null)
        {
            return;
        }

        using var leaveGroupActivity = KafkaTracing.StartAsCurrent(KafkaTracing.ActivityLeaveGroup);
        leaveGroupActivity?.AddMessagingAttributes(_context);

        _logger.LogInformation("Leaving consumer group.");

        var request = new LeaveGroupRequest
        {
            GroupId = _config.GroupId,
            MemberId = _groupMembership.MemberId,
            Members =
            [
                new MemberIdentity
                {
                    MemberId = _groupMembership.MemberId,
                    GroupInstanceId = null,
                    Reason = null,
                }
            ],
        };
        using var response = await SendCoordinatorRequestAsync(request, cancellationToken);

        if (response.Message.ErrorCode != 0)
        {
            throw new ProtocolException($"Failed to leave consumer group. Error code {response.Message.ErrorCode}");
        }

        _groupMembership = null;
    }

    private async ValueTask RejoinGroupAsync(CancellationToken cancellationToken)
    {
        using var joinGroupActivity = KafkaTracing.StartAsCurrent(KafkaTracing.ActivityJoinGroup);
        joinGroupActivity?.AddMessagingAttributes(_context);

        using var joinGroupResponse = await JoinGroupRequestAsync(_coordinatorConnection!, cancellationToken);
        _groupMembership = new GroupMembership
        {
            MemberId = joinGroupResponse.Message.MemberId!,
            LeaderId = joinGroupResponse.Message.Leader!,
            GenerationId = joinGroupResponse.Message.GenerationId,
            Members = joinGroupResponse.Message.Members == null
                ? Array.Empty<GroupMembershipMember>()
                : joinGroupResponse.Message.Members
                    .Select(m => new GroupMembershipMember
                    {
                        MemberId = m.MemberId!,
                        Topics = m.Metadata?.Topics == null
                            ? Array.Empty<string>()
                            : m.Metadata.Topics.Select(t => t!).ToList()
                    })
                    .ToList(),
        };
        IList<SyncGroupRequestAssignment> assignments = Array.Empty<SyncGroupRequestAssignment>();
        if (_groupMembership.MemberId == _groupMembership.LeaderId)
        {
            _logger.LogInformation("Promoted as a leader.");
            assignments = ReassignGroup();
        }

        await SyncGroup(_coordinatorConnection!, assignments, cancellationToken);

        _heartbeatsBackgroundTask = null;
        _heartbeatCts?.Cancel();
        _heartbeatCts?.Dispose();
        _heartbeatCts = new CancellationTokenSource();
        StartSendingHeartbeats();
    }

    private async ValueTask RefreshMetadataAsync(CancellationToken cancellationToken)
    {
        using var refreshMetadataActivity = KafkaTracing.StartAsCurrent(KafkaTracing.ActivityRefreshMetadata);
        refreshMetadataActivity?.AddMessagingAttributes(_context);

        await RefreshCoordinatorAsync(cancellationToken);

        using var metadataResponse = await RequestMetadata(_coordinatorConnection!, cancellationToken);
        _topicsMetadata = metadataResponse.Message.Topics ?? throw new ProtocolException("Metadata response did not contain topics.");
        _topicsMetadataById = metadataResponse.Message.Topics
            .GroupBy(x => x.Value.TopicId)
            .Where(x => x.Key.HasValue)
            .ToDictionary(g => g.Key!.Value, g => g.First().Value);

        var knownBrokerIds = new HashSet<int>(metadataResponse.Message.Brokers!.Keys);
        var connectionsToClose = _brokerConnections
            .Where(kvp => !knownBrokerIds.Contains(kvp.Key))
            .ToList();
        foreach (var (removedId, connection) in connectionsToClose)
        {
            await connection.DisposeAsync();
            _brokerConnections.Remove(removedId);
        }

        foreach (var broker in metadataResponse.Message.Brokers!.Values)
        {
            if (_brokerConnections.ContainsKey(broker.NodeId!.Value))
            {
                continue;
            }

            var connectionConfig = new ConnectionConfig(
                _coordinatorConnection!.Config.Protocol,
                broker.Host!,
                broker.Port!.Value,
                _config.ClientId,
                _config.ResponseBufferSize,
                _config.RequestBufferSize,
                _config.Tls,
                _config.Sasl,
                _config.CheckCrcs);
            var connection = new Connection(connectionConfig, _loggerFactory);
            await connection.OpenAsync(cancellationToken);
            _brokerConnections[broker.NodeId!.Value] = connection;
        }
    }

    public IDictionary<string, MetadataResponseTopic> TopicsMetadata => _topicsMetadata ?? throw new InvalidOperationException("Metadata not loaded.");

    public IDictionary<Guid, MetadataResponseTopic> TopicsMetadataById => _topicsMetadataById ?? throw new InvalidOperationException("Metadata not loaded.");

    public Dictionary<int, IConnection> BrokerConnections => _brokerConnections;

    public string? ResolveTopicName(string? name, Guid? topicId)
    {
        if (!string.IsNullOrEmpty(name))
        {
            return name;
        }

        if (topicId != null && _topicsMetadataById?.TryGetValue(topicId.Value, out var metadata) == true)
        {
            return metadata.Name;
        }

        return null;
    }

    private async ValueTask<Connection> OpenBootstrapConnectionAsync(CancellationToken cancellationToken)
    {
        using var bootstrapActivity = KafkaTracing.StartAsCurrent(KafkaTracing.ActivityBootstrapConnection);
        bootstrapActivity?.AddMessagingAttributes(_context);

        _logger.LogInformation("Opening bootstrap connection.");

        var connectionStrings = _config.BootstrapServers.Split(
            ",",
            StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        foreach (var connectionString in connectionStrings)
        {
            var connectionConfig = ConnectionConfig.FromConnectionString(
                connectionString,
                _config.ClientId,
                _config.ResponseBufferSize,
                _config.RequestBufferSize)
                with
            { Tls = _config.Tls, Sasl = _config.Sasl, RequestApiVersionsOnOpen = false };
            try
            {
                var connection = new Connection(connectionConfig, _loggerFactory);
                await connection.OpenAsync(cancellationToken);
                return connection;
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "Failed to open connection to '{connectionString}'.", connectionString);
            }
        }

        throw new ConnectionException("No connection could be established.");
    }

    private async ValueTask<(string HostName, int Port)> FindCoordinatorAsync(Connection connection, CancellationToken cancellationToken)
    {
        using var findCoordinatorActivity = KafkaTracing.StartAsCurrent(KafkaTracing.ActivityFindCoordinator);
        findCoordinatorActivity?.AddMessagingAttributes(_context);
        var request = new FindCoordinatorRequest
        {
            Key = _config.GroupId,
            KeyType = 0,
            CoordinatorKeys = [_config.GroupId],
        };
        using var response = await connection.SendAsync(request, cancellationToken);

        if (response.Version < 4)
        {
            if (response.Message.ErrorCode != 0)
            {
                throw new ProtocolException($"Failed to find group coordinator. Error code {response.Message.ErrorCode}");
            }

            return (response.Message.Host!, response.Message.Port!.Value);
        }

        var coordinator = response.Message.Coordinators?.FirstOrDefault(x => x.Key == _config.GroupId);
        if (coordinator == null)
        {
            throw new ProtocolException(
                $"Failed to find group coordinator. Response did not match coordinator key '{_config.GroupId}'.");
        }
        if (coordinator.ErrorCode != 0)
        {
            throw new ProtocolException($"Failed to find group coordinator. Error code {coordinator.ErrorCode}");
        }

        return (coordinator.Host!, coordinator.Port!.Value);
    }

    private async ValueTask RefreshCoordinatorAsync(CancellationToken cancellationToken)
    {
        using var refreshCoordinatorActivity = KafkaTracing.StartAsCurrent(KafkaTracing.ActivityRefreshCoordinator);
        refreshCoordinatorActivity?.AddMessagingAttributes(_context);

        await using var bootstrapConnection = await OpenBootstrapConnectionAsync(cancellationToken);
        var (newHost, newPort) = await FindCoordinatorAsync(bootstrapConnection, cancellationToken);
        if (_coordinatorConnection != null && _coordinatorConnection.Config.Host == newHost && _coordinatorConnection.Config.Port == newPort)
        {
            return;
        }

        var newConfig = new ConnectionConfig(
            bootstrapConnection.Config.Protocol,
            newHost,
            newPort,
            _config.ClientId,
            _config.ResponseBufferSize,
            _config.RequestBufferSize,
            _config.Tls,
            _config.Sasl,
            _config.CheckCrcs);

        if (_coordinatorConnection != null)
        {
            _logger.LogInformation("Coordinator changed from {oldHost}:{oldPort} to {newHost}:{newPort}. Reconnecting.", _coordinatorConnection.Config.Host, _coordinatorConnection.Config.Port, newHost, newPort);

            await _coordinatorConnection.DisposeAsync();
            _coordinatorConnection = null;
        }
        else
        {
            _logger.LogInformation("Connecting to coordinator {newHost}:{newPort}.", newHost, newPort);
        }

        var newConnection = new Connection(newConfig, _loggerFactory);
        await newConnection.OpenAsync(cancellationToken);

        _coordinatorConnection = newConnection;
    }

    private async ValueTask<IDisposableMessage<MetadataResponse>> RequestMetadata(IConnection connection, CancellationToken cancellationToken)
    {
        using var requestMetadataActivity = KafkaTracing.StartAsCurrent(KafkaTracing.ActivityRequestMetadata);
        requestMetadataActivity?.AddMessagingAttributes(_context);

        _logger.LogInformation("Requesting metadata.");

        var request = new MetadataRequest
        {
            Topics = _topics.Select(x => new MetadataRequestTopic
            {
                Name = x,
                TopicId = Guid.Empty,
            }).ToArray(),
            AllowAutoTopicCreation = false,
            IncludeClusterAuthorizedOperations = false,
            IncludeTopicAuthorizedOperations = false,
        };

        var response = await connection.SendAsync(request, cancellationToken);

        foreach (var topicName in _topics)
        {
            if (!response.Message.Topics!.TryGetValue(topicName, out var topic))
            {
                _logger.LogInformation($"No topic metadata found for topic {topicName}.");
                continue;
            }

            if (topic.ErrorCode != 0)
            {
                throw new ProtocolException($"Metadata request failed for topic {topic.Name}. Error code {topic.ErrorCode}.");
            }

            foreach (var partition in topic.Partitions!)
            {
                if (partition.ErrorCode != 0)
                {
                    throw new ProtocolException(
                        $"Metadata request failed for topic {topic.Name} partition {partition.PartitionIndex}. Error code {partition.ErrorCode}");
                }
            }
        }

        return response;
    }

    private async ValueTask<IDisposableMessage<JoinGroupResponse>> JoinGroupRequestAsync(IConnection connection, CancellationToken cancellationToken)
    {
        using var joinGroupRequestActivity = KafkaTracing.StartAsCurrent(KafkaTracing.ActivityJoinGroupRequest);
        joinGroupRequestActivity?.AddMessagingAttributes(_context);

        _logger.LogInformation("Joining consumer group.");
        var request = new JoinGroupRequest
        {
            GroupId = _config.GroupId,
            SessionTimeoutMs = (int)_config.SessionTimeout.TotalMilliseconds,
            RebalanceTimeoutMs = (int)_config.MaxPollInterval.TotalMilliseconds,
            MemberId = string.Empty,
            GroupInstanceId = _config.InstanceId,
            ProtocolType = "consumer",
            Protocols = new Dictionary<string, JoinGroupRequestProtocol>
            {
                {
                    "nkafka-consumer", new JoinGroupRequestProtocol
                    {
                        Name = "nkafka-consumer",
                        Metadata = new ConsumerProtocolSubscription
                        {
                            Topics = _topics,
                            UserData = null,
                            OwnedPartitions = null,
                            GenerationId = -1,
                            RackId = null,
                        },
                    }
                }
            },
            Reason = null
        };
        var response = await SendCoordinatorRequestAsync(request, cancellationToken);

        if (response.Version == 4 && response.Message.ErrorCode == (short)ErrorCode.MemberIdRequired)
        {
            request.MemberId = response.Message.MemberId;
            response.Dispose();
            response = await SendCoordinatorRequestAsync(request, cancellationToken);
        }

        if (response.Message.ErrorCode != 0)
        {
            throw new ProtocolException($"Failed to join consumer group. Error code {response.Message.ErrorCode}");
        }

        return response;
    }

    private async ValueTask<IDisposableMessage<TResponse>> SendCoordinatorRequestAsync<TResponse>(IRequest<TResponse> request, CancellationToken cancellationToken)
    {
        using var scope = BeginDefaultLoggingScope();
        return await _coordinatorConnection!.SendAsync(request, cancellationToken);
    }

    private class GroupMembership
    {
        public required string MemberId { get; init; }
        public required string LeaderId { get; init; }
        public int? GenerationId { get; init; }
        public required IList<GroupMembershipMember> Members { get; init; }
    }

    private class GroupMembershipMember
    {
        public required string MemberId { get; init; }
        public required IList<string> Topics { get; init; }
    }

    private IDisposable? BeginDefaultLoggingScope() => _logger.BeginScope(_config.InstanceId);

    private IList<SyncGroupRequestAssignment> ReassignGroup()
    {
        var assignments = _groupMembership!.Members.Select(x =>
            new SyncGroupRequestAssignment
            {
                MemberId = x.MemberId,
                Assignment = new ConsumerProtocolAssignment
                {
                    AssignedPartitions = new Dictionary<string, TopicPartition>(),
                }
            })
            .ToDictionary(x => x.MemberId!);
        foreach (var topicName in _topics)
        {
            if (!_topicsMetadata!.TryGetValue(topicName, out var topic))
            {
                continue;
            }

            var members = _groupMembership!.Members.Where(x => x.Topics.Contains(topicName)).ToList();
            if (members.Count == 0)
            {
                continue;
            }

            foreach (var partition in topic.Partitions!)
            {
                var memberIndex = partition.PartitionIndex!.Value % members.Count;
                var member = members[memberIndex];
                var assignment = assignments[member.MemberId];
                if (!assignment.Assignment!.AssignedPartitions!.TryGetValue(topicName, out var topicPartition))
                {
                    topicPartition = new TopicPartition
                    {
                        Topic = topicName,
                        Partitions = new List<int>(),
                    };
                    assignment.Assignment.AssignedPartitions[topicName] = topicPartition;
                }
                topicPartition.Partitions!.Add(partition.PartitionIndex.Value);
            }
        }

        return assignments.Values.ToList();
    }

    private async ValueTask SyncGroup(IConnection connection, IList<SyncGroupRequestAssignment> assignments, CancellationToken cancellationToken)
    {
        using var syncGroupActivity = KafkaTracing.StartAsCurrent(KafkaTracing.ActivitySyncGroup);
        syncGroupActivity?.AddMessagingAttributes(_context);

        _logger.LogInformation("Synchronizing consumer group.");

        var request = new SyncGroupRequest
        {
            GroupId = _config.GroupId,
            GenerationId = _groupMembership!.GenerationId,
            MemberId = _groupMembership.MemberId,
            GroupInstanceId = null,
            ProtocolType = "consumer",
            ProtocolName = "nkafka-consumer",
            Assignments = assignments,
        };
        using var response = await connection.SendAsync(request, cancellationToken);

        if (response.Message.ErrorCode != 0)
        {
            throw new ProtocolException($"Failed to synchronize consumer group. Error code {response.Message.ErrorCode}");
        }

        _rejoinAssignments = response.Message.Assignment!;

        var context = new StringBuilder();
        foreach (var topic in _rejoinAssignments.AssignedPartitions!)
        {
            context.Append(topic.Value.Topic);
            context.Append("[");
            foreach (var partition in topic.Value.Partitions!)
            {
                context.Append(partition);
                context.Append(",");
            }
            context.Append("] ");
        }
        _logger.LogInformation("Assigned partitions: {context}", context);
    }

    private void StartSendingHeartbeats()
    {
        if (_heartbeatsBackgroundTask != null && !_heartbeatsBackgroundTask.IsCompleted)
        {
            return;
        }

        var cancellationToken = _heartbeatCts!.Token;
        _heartbeatsBackgroundTask = Task.Run(
            async () =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        using var heartbeatActivity = KafkaTracing.StartAsCurrent(KafkaTracing.ActivityHeartbeat);
                        heartbeatActivity?.AddMessagingAttributes(_context);
                        var heartbeatConnection = _coordinatorConnection!;

                        var request = new HeartbeatRequest
                        {
                            GroupId = _config.GroupId,
                            GenerationId = _groupMembership!.GenerationId,
                            MemberId = _groupMembership.MemberId,
                            GroupInstanceId = null,
                        };
                        using var response = await heartbeatConnection.SendAsync(request, cancellationToken);

                        if (response.Message.ErrorCode == 0)
                        {
                            _logger.LogDebug(
                                "Heartbeat was sent. Waiting for {@interval}ms.",
                                (int)_config.HeartbeatInterval.TotalMilliseconds);
                        }
                        else if (response.Message.ErrorCode == (short)ErrorCode.RebalanceInProgress)
                        {
                            _logger.LogInformation("The group is rebalancing, so a rejoin is needed.");
                            RebalanceRequired?.Invoke();
                            break;
                        }
                        else if (response.Message.ErrorCode == (short)ErrorCode.UnknownMemberId)
                        {
                            _logger.LogInformation("Member ID is unknown, triggering group rejoin.");
                            RebalanceRequired?.Invoke();
                            break;
                        }
                        else
                        {
                            _logger.LogError(
                                "Error in heartbeat response: {@errorCode}. Waiting for {@interval}ms.",
                                response.Message.ErrorCode,
                                (int)_config.HeartbeatInterval.TotalMilliseconds);
                        }

                        await Task.Delay(_config.HeartbeatInterval, cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }

                _logger.LogDebug("Heartbeats sending was stopped.");
            }, cancellationToken);
    }

    public void RequestRejoin()
    {
        RebalanceRequired?.Invoke();
    }

    public async ValueTask DisposeAsync()
    {
        _heartbeatCts?.Cancel();
        _heartbeatCts?.Dispose();

        if (_heartbeatsBackgroundTask != null)
        {
            try
            {
                await _heartbeatsBackgroundTask;
            }
            catch (OperationCanceledException)
            {
            }
        }

        _heartbeatsBackgroundTask = null;

        foreach (var connection in _brokerConnections.Values)
        {
            await connection.DisposeAsync();
        }
        _brokerConnections.Clear();

        if (_coordinatorConnection != null)
        {
            await _coordinatorConnection.DisposeAsync();
            _coordinatorConnection = null;
        }
    }
}
