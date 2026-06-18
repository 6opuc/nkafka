using System.Diagnostics;
using System.Text;
using System.Threading.Channels;
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

public class Consumer<TMessage> : IConsumer<TMessage>
{
    private readonly ConsumerConfig _config;
    private readonly IMessageDeserializer<TMessage> _deserializer;
    private readonly IOffsetStorage _offsetStorage;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger _logger;
    private readonly KafkaTelemetryContext _context;

    private CancellationTokenSource? _stop;
    private Task? _heartbeatsBackgroundTask;
    private readonly Dictionary<int, IConnection> _connections = new();
    private IConnection? _coordinatorConnection;
    private readonly string[] _topics;
    private GroupMembership? _groupMembership;
    private IDictionary<string, MetadataResponseTopic>? _topicsMetadata;
    private IDictionary<Guid, MetadataResponseTopic>? _topicsMetadataById;
    private List<Task>? _fetchTasks;

    private readonly Channel<FetchResult> _consumeChannel =
        Channel.CreateBounded<FetchResult>(new BoundedChannelOptions(1)
        {
            SingleReader = true,
        });
    private FetchResult? _fetchResult;
    private IDisposableMessage<FetchResponse>? _fetchResponse => _fetchResult?.Response;
    private IEnumerator<MessageDeserializationContext>? _messageDeserializeEnumerator = null;

    private int _heartbeatCount;

    public int GenerationId => _groupMembership?.GenerationId ?? 0;

    public Consumer(
        ConsumerConfig config,
        IMessageDeserializer<TMessage> deserializer,
        IOffsetStorage offsetStorage,
        ILoggerFactory loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(deserializer);
        ArgumentNullException.ThrowIfNull(offsetStorage);
        ArgumentNullException.ThrowIfNull(loggerFactory);
        _config = config;
        _deserializer = deserializer;
        _offsetStorage = offsetStorage;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<Consumer<TMessage>>();
        _context = new KafkaTelemetryContext(
            _config.GroupId,
            _config.InstanceId ?? _config.ClientId,
            _config.BootstrapServers.Split(",", StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries).FirstOrDefault(),
            null,
            null,
            null);
        _topics = _config.Topics.Split(",", StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
    }

    public async ValueTask JoinGroupAsync(CancellationToken cancellationToken)
    {
        _stop = new CancellationTokenSource();
        using var _lifetime = BeginDefaultLoggingScope();
        await RejoinGroupAsync(cancellationToken);
    }

    private async ValueTask RejoinGroupAsync(CancellationToken cancellationToken)
    {
        using var _joinGroupActivity = KafkaTracing.Source.StartActivity("Consumer.JoinGroup", ActivityKind.Client);

        using var _bootstrapActivity = KafkaTracing.Source.StartActivity("Consumer.BootstrapConnection", ActivityKind.Client);
        await using var bootstrapConnection = await OpenBootstrapConnectionAsync(cancellationToken);
        _bootstrapActivity?.AddTag("nKafka.phase", "bootstrap");

        using var _coordinatorConnActivity = KafkaTracing.Source.StartActivity("Consumer.OpenCoordinatorConnection", ActivityKind.Client);
        _coordinatorConnection = await OpenCoordinatorConnectionAsync(bootstrapConnection, cancellationToken);
        _coordinatorConnActivity?.AddTag("nKafka.phase", "coordinator_connection");

        using var _refreshMetadataActivity = KafkaTracing.Source.StartActivity("Consumer.RefreshMetadata", ActivityKind.Client);
        await RefreshMetadataAsync(cancellationToken);
        _refreshMetadataActivity?.AddTag("nKafka.phase", "refresh_metadata");

        using var _joinGroupReqActivity = KafkaTracing.Source.StartActivity("Consumer.JoinGroupRequest", ActivityKind.Client);
        using var joinGroupResponse = await JoinGroupRequestAsync(_coordinatorConnection, cancellationToken);
        _joinGroupReqActivity?.AddTag("nKafka.phase", "join_group_request");
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

        using var _syncGroupActivity = KafkaTracing.Source.StartActivity("Consumer.SyncGroup", ActivityKind.Client);
        await SyncGroup(_coordinatorConnection, assignments, cancellationToken);
        _syncGroupActivity?.AddTag("nKafka.phase", "sync_group");

        await StopFetchingAsync();

        _stop?.Dispose();
        _stop = new CancellationTokenSource();

        if (_rejoinAssignments != null)
        {
            await StartFetchingAsync(_rejoinAssignments.AssignedPartitions!);
        }

        _heartbeatsBackgroundTask = null;
        StartSendingHeartbeats();
    }

    private IDisposable? BeginDefaultLoggingScope()
    {
        return _logger.BeginScope(_config.InstanceId);
    }

    private async ValueTask RefreshMetadataAsync(CancellationToken cancellationToken)
    {
        using var _refreshCoordinatorActivity = KafkaTracing.Source.StartActivity("Consumer.RefreshCoordinator", ActivityKind.Client);
        var coordinator = await RefreshCoordinatorAsync(cancellationToken);
        _refreshCoordinatorActivity?.AddTag("nKafka.phase", "refresh_coordinator");

        using var _requestMetadataActivity = KafkaTracing.Source.StartActivity("Consumer.RequestMetadata", ActivityKind.Client);
        using var metadataResponse = await RequestMetadata(coordinator, cancellationToken);
        _requestMetadataActivity?.AddTag("nKafka.phase", "request_metadata");
        if (metadataResponse.Message.Topics == null)
        {
            throw new ProtocolException("Metadata response did not contain topics.");
        }
        _topicsMetadata = metadataResponse.Message.Topics;
        _topicsMetadataById = metadataResponse.Message.Topics
            .GroupBy(x => x.Value.TopicId)
            .Where(x => x.Key.HasValue)
            .ToDictionary(g => g.Key!.Value, g => g.First().Value);

        var knownBrokerIds = new HashSet<int>(metadataResponse.Message.Brokers!.Keys);
        var connectionsToClose = _connections
            .Where(kvp => !knownBrokerIds.Contains(kvp.Key))
            .ToList();
        foreach (var (removedId, connection) in connectionsToClose)
        {
            await connection.DisposeAsync();
            _connections.Remove(removedId);
        }

        foreach (var broker in metadataResponse.Message.Brokers!.Values)
        {
            if (_connections.ContainsKey(broker.NodeId!.Value))
            {
                continue;
            }

            var connectionConfig = new ConnectionConfig(
                _config.Protocol,
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
            _connections[broker.NodeId!.Value] = connection;
        }
    }

    private async ValueTask<Connection> OpenBootstrapConnectionAsync(
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Opening bootstrap connection.");

        string[] connectionStrings = _config.BootstrapServers.Split(
            ",",
            StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        foreach (string connectionString in connectionStrings)
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

    private async ValueTask<IConnection> OpenCoordinatorConnectionAsync(
        IConnection connection,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Opening coordinator connection.");

        using var _findCoordinatorActivity = KafkaTracing.Source.StartActivity("Consumer.FindCoordinator", ActivityKind.Client);
        var request = new FindCoordinatorRequest
        {
            Key = _config.GroupId,
            KeyType = 0,
            CoordinatorKeys = [_config.GroupId],
        };

        using var response = await connection.SendAsync(request, CancellationToken.None);
        _findCoordinatorActivity?.AddTag("nKafka.phase", "find_coordinator");

        ConnectionConfig coordinatorConnectionConfig;
        if (response.Version < 4)
        {
            if (response.Message.ErrorCode != 0)
            {
                throw new ProtocolException($"Failed to find group coordinator. Error code {response.Message.ErrorCode}");
            }

            coordinatorConnectionConfig = new ConnectionConfig(
                            _config.Protocol,
                            response.Message.Host!,
                            response.Message.Port!.Value,
                            _config.ClientId,
                            _config.ResponseBufferSize,
                            _config.RequestBufferSize,
                            _config.Tls,
                            _config.Sasl,
                            _config.CheckCrcs);
        }
        else
        {
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

            coordinatorConnectionConfig = new ConnectionConfig(
                _config.Protocol,
                coordinator.Host!,
                coordinator.Port!.Value,
                _config.ClientId,
                _config.ResponseBufferSize,
                _config.RequestBufferSize,
                _config.Tls,
                _config.Sasl,
                _config.CheckCrcs);
        }

        var coordinatorConnection = new Connection(coordinatorConnectionConfig, _loggerFactory);
        await coordinatorConnection.OpenAsync(cancellationToken);

        _coordinatorConnection = coordinatorConnection;

        return coordinatorConnection;
    }

    private async ValueTask<IConnection> RefreshCoordinatorAsync(CancellationToken cancellationToken)
    {
        await using var bootstrapConnection = await OpenBootstrapConnectionAsync(cancellationToken);
        using var _findCoordinatorActivity = KafkaTracing.Source.StartActivity("Consumer.FindCoordinator", ActivityKind.Client);
        using var coordinatorResponse = await bootstrapConnection.SendAsync(
            new FindCoordinatorRequest
            {
                Key = _config.GroupId,
                KeyType = 0,
                CoordinatorKeys = [_config.GroupId],
            },
            CancellationToken.None);
        _findCoordinatorActivity?.AddTag("nKafka.phase", "find_coordinator");

        string newHost;
        int newPort;

        if (coordinatorResponse.Version < 4)
        {
            if (coordinatorResponse.Message.ErrorCode != 0)
            {
                throw new ProtocolException($"Failed to find group coordinator during refresh. Error code {coordinatorResponse.Message.ErrorCode}");
            }

            newHost = coordinatorResponse.Message.Host!;
            newPort = coordinatorResponse.Message.Port!.Value;
        }
        else
        {
            var coordinator = coordinatorResponse.Message.Coordinators?.FirstOrDefault(x => x.Key == _config.GroupId);
            if (coordinator == null)
            {
                throw new ProtocolException(
                    $"Failed to find group coordinator during refresh. Response did not match coordinator key '{_config.GroupId}'.");
            }

            if (coordinator.ErrorCode != 0)
            {
                throw new ProtocolException($"Failed to find group coordinator during refresh. Error code {coordinator.ErrorCode}");
            }

            newHost = coordinator.Host!;
            newPort = coordinator.Port!.Value;
        }

        if (_coordinatorConnection is Connection { Config: var oldConfig } && oldConfig.Host == newHost && oldConfig.Port == newPort)
        {
            return _coordinatorConnection!;
        }

        _logger.LogInformation("Coordinator changed from {oldHost}:{oldPort} to {newHost}:{newPort}. Reconnecting.", _coordinatorConnection is Connection c ? c.Config.Host : "<unknown>", _coordinatorConnection is Connection c2 ? c2.Config.Port : -1, newHost, newPort);

        if (_coordinatorConnection != null)
        {
            await _coordinatorConnection.DisposeAsync();
            _coordinatorConnection = null;
        }

        var newConfig = new ConnectionConfig(
            _config.Protocol,
            newHost,
            newPort,
            _config.ClientId,
            _config.ResponseBufferSize,
            _config.RequestBufferSize,
            _config.Tls,
            _config.Sasl,
            _config.CheckCrcs);

        var newConnection = new Connection(newConfig, _loggerFactory);
        await newConnection.OpenAsync(cancellationToken);

        _coordinatorConnection = newConnection;

        return newConnection;
    }

    private async ValueTask<IDisposableMessage<MetadataResponse>> RequestMetadata(IConnection connection,
        CancellationToken cancellationToken)
    {
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

        foreach (string topicName in _topics)
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

    private async Task<IDisposableMessage<JoinGroupResponse>> JoinGroupRequestAsync(
        IConnection connection,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Joining consumer group.");
        var request = new JoinGroupRequest
        {
            GroupId = _config.GroupId,
            SessionTimeoutMs = _config.SessionTimeoutMs,
            RebalanceTimeoutMs = _config.MaxPollIntervalMs,
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
                            UserData = null,            // Null for non-leader consumers; leader sends assignment in SyncGroup
                            OwnedPartitions = null,     // Assigned by group leader during SyncGroup phase
                            GenerationId = -1,          // First join per Kafka protocol (Consumer Group Protocol spec)
                            RackId = null,              // Null when consumer.rack.id is not configured
                        },
                    }
                }
            },
            Reason = null
        };
        var response = await connection.SendAsync(request, cancellationToken);

        if (response.Version == 4 && response.Message.ErrorCode == (short)ErrorCode.MemberIdRequired)
        {
            // retry with given member id
            request.MemberId = response.Message.MemberId;
            response.Dispose();

            response = await connection.SendAsync(request, cancellationToken);
        }

        if (response.Message.ErrorCode != 0)
        {
            throw new ProtocolException($"Failed to join consumer group. Error code {response.Message.ErrorCode}");
        }

        return response;
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

    private IList<SyncGroupRequestAssignment> ReassignGroup()
    {
        // Topic-coordinator binding is not needed here: partition assignment is independent
        // of broker routing. The Kafka consumer protocol handles coordinator discovery
        // and request routing separately from partition distribution.
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
        foreach (string topicName in _topics)
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
                int memberIndex = partition.PartitionIndex!.Value % members.Count;
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

    private async Task SyncGroup(
        IConnection connection,
        IList<SyncGroupRequestAssignment> assignments,
        CancellationToken cancellationToken)
    {
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
            foreach (int partition in topic.Value.Partitions!)
            {
                context.Append(partition);
                context.Append(",");
            }
            context.Append("] ");
        }
        _logger.LogInformation("Assigned partitions: {context}", context);
    }

    private ConsumerProtocolAssignment? _rejoinAssignments;

    private void StartSendingHeartbeats()
    {
        if (_heartbeatsBackgroundTask != null && !_heartbeatsBackgroundTask.IsCompleted)
        {
            return;
        }

        var cancellationToken = _stop!.Token;
        _heartbeatsBackgroundTask = Task.Run(
            async () =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        using var _heartbeatActivity = KafkaTracing.Source.StartActivity("Consumer.Heartbeat", ActivityKind.Client);
                        var heartbeatConnection = _coordinatorConnection!;
                        _heartbeatActivity?.AddMessagingAttributes(_context, "heartbeat");

                        var request = new HeartbeatRequest
                        {
                            GroupId = _config.GroupId,
                            GenerationId = _groupMembership!.GenerationId,
                            MemberId = _groupMembership.MemberId,
                            GroupInstanceId = null,
                        };
                        using var response = await heartbeatConnection.SendAsync(request, cancellationToken);
                        _heartbeatActivity?.AddTag("nKafka.phase", "heartbeat");
                        Interlocked.Increment(ref _heartbeatCount);

                        if (response.Message.ErrorCode == 0)
                        {
                            _logger.LogDebug(
                                "Heartbeat was sent. Waiting for {@interval}ms.",
                                _config.HeartbeatIntervalMs);
                        }
                        else if (response.Message.ErrorCode == (short)ErrorCode.RebalanceInProgress)
                        {
                            _logger.LogInformation("The group is rebalancing, so a rejoin is needed.");
                            await RejoinGroupAsync(CancellationToken.None);
                            break;
                        }
                        else if (response.Message.ErrorCode == (short)ErrorCode.UnknownMemberId)
                        {
                            _logger.LogInformation("Member ID is unknown, triggering group rejoin.");
                            await RejoinGroupAsync(CancellationToken.None);
                            break;
                        }
                        else
                        {
                            _logger.LogError(
                                "Error in heartbeat response: {@errorCode}. Waiting for {@interval}ms.",
                                response.Message.ErrorCode,
                                _config.HeartbeatIntervalMs);
                        }

                        await Task.Delay(_config.HeartbeatIntervalMs, cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }

                _logger.LogDebug("Heartbeats sending was stopped.");
            }, cancellationToken);
    }

    private async Task StartFetchingAsync(IDictionary<string, TopicPartition> assignedPartitions)
    {
        var cancellationToken = _stop!.Token;

        var topicPartitionOffsets = new List<(string Topic, int Partition, long Offset, int NodeId)>();
        var sessionManagers = new Dictionary<int, FetchSessionManager>(_connections.Count);

        foreach (var topicAssignment in assignedPartitions)
        {
            if (_topicsMetadata?.TryGetValue(topicAssignment.Key, out var topicMetadata) != true ||
                topicMetadata == null)
            {
                continue;
            }

            foreach (int partition in topicAssignment.Value.Partitions!)
            {
                var partitionMetadata = topicMetadata!.Partitions?.FirstOrDefault(x => x.PartitionIndex == partition);
                if (partitionMetadata == null)
                {
                    continue;
                }

                int nodeId = partitionMetadata.LeaderId!.Value;
                if (!_connections.TryGetValue(nodeId, out var connection))
                {
                    _logger.LogWarning("Broker node {nodeId} not found in connections, skipping partition {topic}/{partition}.", nodeId, topicAssignment.Key, partition);
                    continue;
                }

                if (!sessionManagers.TryGetValue(nodeId, out var sessionManager))
                {
                    bool useSessions = connection.SupportsApiKeyVersion(ApiKey.Fetch, 4);
                    sessionManager = new FetchSessionManager(0, 0, useSessions);
                    sessionManagers[nodeId] = sessionManager;
                }

                long offset = await _offsetStorage.GetAsync(
                    connection,
                    _config.GroupId,
                    topicAssignment.Key,
                    partition,
                    cancellationToken);
                topicPartitionOffsets.Add((topicAssignment.Key, partition, offset, nodeId));
            }
        }

        var nodeTopics = new Dictionary<int, Dictionary<string, List<(int Partition, long Offset)>>>();
        foreach (var (topic, partition, offset, nodeId) in topicPartitionOffsets)
        {
            if (!nodeTopics.TryGetValue(nodeId, out var topicMap))
            {
                topicMap = new Dictionary<string, List<(int Partition, long Offset)>>();
                nodeTopics[nodeId] = topicMap;
            }

            if (!topicMap.TryGetValue(topic, out var partitions))
            {
                partitions = new List<(int Partition, long Offset)>();
                topicMap[topic] = partitions;
            }
            partitions.Add((partition, offset));
        }

        _fetchTasks = new List<Task>(sessionManagers.Count);
        foreach (var pair in sessionManagers)
        {
            int nodeId = pair.Key;
            var sessionManager = pair.Value;
            var topicMap = nodeTopics.GetValueOrDefault(nodeId, new Dictionary<string, List<(int Partition, long Offset)>>());
            var fetchTask = Task.Run(() => FetchLoopAsync(nodeId, topicMap, sessionManager, cancellationToken), cancellationToken);
            _fetchTasks.Add(fetchTask);
        }
    }

    private async Task FetchLoopAsync(int nodeId, Dictionary<string, List<(int Partition, long Offset)>> topicMap, FetchSessionManager sessionManager, CancellationToken cancellationToken)
    {
        var context = new StringBuilder();
        foreach (var topic in topicMap)
        {
            context.Append(topic.Key);
            context.Append("[");
            foreach (var p in topic.Value)
            {
                context.Append(p.Partition);
                context.Append(",");
            }
            context.Append("] ");
        }
        using (_logger.BeginScope(context.ToString()))
        {
            _logger.LogInformation("Fetching started.");
            int consecutiveErrors = 0;
            bool firstRequest = true;
            if (!_connections.TryGetValue(nodeId, out var connection))
            {
                _logger.LogError("Broker node {nodeId} not found in connections during fetch loop.", nodeId);
                throw new ProtocolException($"Broker node {nodeId} not found in connections during fetch loop.");
            }

            while (!cancellationToken.IsCancellationRequested)
            {
                using var _fetchActivity = KafkaTracing.Source.StartActivity("Consumer.Fetch", ActivityKind.Client);
                _fetchActivity?.AddMessagingAttributes(_context, KafkaMetrics.OperationReceive);
                try
                {
                    var fetchRequest = CreateFetchRequest(sessionManager, topicMap, firstRequest);
                    firstRequest = false;

                    int fetchGenerationId = _groupMembership?.GenerationId ?? 0;
                    var response = await connection.SendAsync(fetchRequest, cancellationToken);
                    _fetchActivity!.AddTag("nKafka.phase", "fetch");

                    if (response.Message.SessionId != null)
                    {
                        sessionManager.OnResponseReceived(response.Message);
                    }

                    if (response.Message.ErrorCode != 0)
                    {
                        if (response.Message.ErrorCode == (short)ErrorCode.FetchSessionIdNotFound ||
                            response.Message.ErrorCode == (short)ErrorCode.InvalidFetchSessionEpoch)
                        {
                            sessionManager.OnError(response.Message.ErrorCode.Value);
                            _logger.LogWarning("Fetch session error: {@errorCode}. Reinitializing session.", response.Message.ErrorCode);
                            firstRequest = true;
                            continue;
                        }

                        if (response.Message.ErrorCode == (short)ErrorCode.NotLeaderForPartition)
                        {
                            _logger.LogWarning("Partition leader changed (errorCode: {@errorCode}). Refreshing metadata.", response.Message.ErrorCode);
                            consecutiveErrors = 0;
                            firstRequest = true;
                            sessionManager.OnError(0);
                            await RefreshMetadataAsync(cancellationToken);
                            continue;
                        }

                        _logger.LogError(
                            "Error in fetch response: {@errorCode}.",
                            response.Message.ErrorCode);
                    }
                    else
                    {

                        long responseBytes = 0;
                        long responseMessages = 0;

                        foreach (var topicResponse in response.Message.Responses!)
                        {
                            string? topicName = ResolveTopicName(topicResponse.Topic, topicResponse.TopicId);

                            if (string.IsNullOrEmpty(topicName) || !topicMap.TryGetValue(topicName, out var topicPartitions))
                            {
                                continue;
                            }

                            for (int pi = 0; pi < topicPartitions.Count; pi++)
                            {
                                var partitionResponse = topicResponse.Partitions!.FirstOrDefault(
                                    x => x.PartitionIndex == topicPartitions[pi].Partition);
                                if (partitionResponse == null)
                                {
                                    continue;
                                }

                                if (partitionResponse.Records != null)
                                {
                                    responseBytes += partitionResponse.Records.SizeInBytes;
                                    responseMessages += partitionResponse.Records.RecordCount;
                                }

                                long? lastOffset = partitionResponse.Records?.LastOffset;
                                if (lastOffset.HasValue)
                                {
                                    topicPartitions[pi] = (topicPartitions[pi].Partition, lastOffset.Value + 1);
                                }
                            }
                        }

                    }

                    consecutiveErrors = 0;
                    if (fetchGenerationId == (_groupMembership?.GenerationId ?? 0))
                    {
                        await _consumeChannel.Writer.WriteAsync(new FetchResult(response, fetchGenerationId), cancellationToken);
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception exception)
                {
                    _fetchActivity!.SetStatus(ActivityStatusCode.Error, exception.Message);
                    _fetchActivity!.AddTag("exception.message", exception.Message);
                    sessionManager.OnError(0);
                    consecutiveErrors++;

                    if (consecutiveErrors >= _config.MaxFetchRetries)
                    {
                        _logger.LogError(exception,
                            "Fetch loop failed after {retries} retries. Writing error to channel.", consecutiveErrors);
                        try
                        {
                            await _consumeChannel.Writer.WriteAsync(new FetchResult(exception), cancellationToken);
                        }
                        catch (OperationCanceledException)
                        {
                            // Channel was closed during cancellation, that's fine
                        }
                        break;
                    }

                    var delay = _config.FetchRetryBaseDelay * Math.Pow(2, consecutiveErrors - 1);
                    _logger.LogWarning(exception,
                        "Fetch loop error (attempt {attempt}/{maxRetries}). Retrying in {delay}ms.",
                        consecutiveErrors, _config.MaxFetchRetries, delay.TotalMilliseconds);

                    try
                    {
                        await Task.Delay(delay, cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            }

            _logger.LogDebug("Fetching was stopped.");
        }
    }

    private FetchRequest CreateFetchRequest(FetchSessionManager sessionManager, Dictionary<string, List<(int Partition, long Offset)>> topicMap, bool isFirstRequest)
    {
        if (!sessionManager.IsActive || isFirstRequest)
        {
            var fetchTopics = new List<FetchTopic>();
            foreach (var topicEntry in topicMap)
            {
                string topicName = topicEntry.Key;
                if (!_topicsMetadata!.TryGetValue(topicName, out var topicMetadata))
                {
                    continue;
                }

                var partitions = topicEntry.Value.Select(p => new FetchPartition
                {
                    Partition = p.Partition,
                    CurrentLeaderEpoch = -1,
                    FetchOffset = p.Offset,
                    LastFetchedEpoch = -1,
                    LogStartOffset = -1,
                    PartitionMaxBytes = _config.FetchPartitionMaxBytes,
                    ReplicaDirectoryId = Guid.Empty,
                }).ToList();

                fetchTopics.Add(new FetchTopic
                {
                    Topic = topicName,
                    TopicId = topicMetadata.TopicId,
                    Partitions = partitions,
                });
            }

            return sessionManager.CreateInitialRequest(
                (int)_config.MaxWaitTime.TotalMilliseconds,
                1,
                0x7fffffff,
                fetchTopics);
        }

        return sessionManager.CreateSubsequentRequest(
            (int)_config.MaxWaitTime.TotalMilliseconds,
            1,
            0x7fffffff);
    }

    public async ValueTask<ConsumeResult<TMessage>?> ConsumeAsync(
        CancellationToken cancellationToken)
    {
        using var _consumeActivity = KafkaTracing.Source.StartActivity("Consumer.Consume", ActivityKind.Client);
        _consumeActivity?.AddMessagingAttributes(_context, KafkaMetrics.OperationReceive);
        _consumeActivity?.AddTag("nKafka.phase", "consume");

        var consumerResult = ConsumeFromBuffer();
        if (consumerResult != null)
        {
            KafkaMetrics.AddMessagesConsumed(_context, 1);
            _consumeActivity?.AddTag("nKafka.result", "cached");
            return consumerResult;
        }

        using var _fetchWaitActivity = KafkaTracing.Source.StartActivity("Consumer.FetchWait", ActivityKind.Client);
        await EnsureEnumeratorAsync(cancellationToken);
        _fetchWaitActivity?.AddTag("nKafka.phase", "fetch_wait");

        using var _deserializeActivity = KafkaTracing.Source.StartActivity("Consumer.Deserialize", ActivityKind.Client);
        var deserializeSw = System.Diagnostics.Stopwatch.StartNew();
        _messageDeserializeEnumerator = GetMessageEnumerator();

        var result = ConsumeFromBuffer();

        if (result is { } r)
        {
            var contextWithMessage = _context with
            {
                TopicName = r.Topic,
                PartitionId = r.Partition.ToString(),
            };
            KafkaMetrics.AddMessagesConsumed(contextWithMessage, 1);
            _consumeActivity?.AddTag("nKafka.result", "message");
        }
        else
        {
            _consumeActivity?.AddTag("nKafka.result", "empty");
        }

        deserializeSw.Stop();
        KafkaMetrics.RecordProcessDuration(_context, KafkaMetrics.OperationProcess, deserializeSw.Elapsed.TotalMilliseconds);

        return result;
    }

    private ConsumeResult<TMessage>? ConsumeFromBuffer()
    {
        if (_messageDeserializeEnumerator == null)
        {
            return null;
        }

        if (_messageDeserializeEnumerator.MoveNext())
        {
            var deserializationContext = _messageDeserializeEnumerator.Current;
            var message = _deserializer.Deserialize(deserializationContext);
            return new ConsumeResult<TMessage>
            {
                Topic = deserializationContext.Topic,
                Partition = deserializationContext.Partition,
                Offset = deserializationContext.Offset,
                Timestamp = deserializationContext.Timestamp,
                Message = message,
            };
        }

        _messageDeserializeEnumerator.Dispose();
        _messageDeserializeEnumerator = null;
        _fetchResult?.Dispose();
        _fetchResult = null;

        return null;
    }

    public async ValueTask<IConsumerBatch<TMessage>> ConsumeBatchAsync(CancellationToken cancellationToken)
    {
        using var _consumeBatchActivity = KafkaTracing.Source.StartActivity("Consumer.ConsumeBatch", ActivityKind.Client);
        using var _fetchWaitActivity = KafkaTracing.Source.StartActivity("Consumer.FetchWait", ActivityKind.Client);
        await EnsureEnumeratorAsync(cancellationToken);
        _fetchWaitActivity?.AddTag("nKafka.phase", "fetch_wait");
        _consumeBatchActivity?.AddTag("nKafka.phase", "consume_batch");
        return new ConsumerBatch(this);
    }

    private async ValueTask EnsureEnumeratorAsync(CancellationToken cancellationToken)
    {
        if (_messageDeserializeEnumerator != null) return;

        while (true)
        {
            try
            {
                using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeoutCts.CancelAfter(30000);
                _fetchResult = await _consumeChannel.Reader.ReadAsync(timeoutCts.Token);

                if (_fetchResult == null)
                {
                    continue;
                }

                if (!_fetchResult.IsSuccess)
                {
                    var ex = _fetchResult.Exception;
                    _fetchResult.Dispose();
                    _fetchResult = null;
                    throw new InvalidOperationException("Fetch loop failed.", ex);
                }

                if (_fetchResult.GenerationId == (_groupMembership?.GenerationId ?? 0))
                {
                    break;
                }

                _logger.LogWarning("Discarding stale fetch result from old generation: {OldGen} vs {CurrentGen}",
                    _fetchResult.GenerationId, _groupMembership?.GenerationId);
                _fetchResult.Dispose();
                _fetchResult = null;
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                continue;
            }
        }

        _messageDeserializeEnumerator = GetMessageEnumerator();
    }

    private IEnumerator<MessageDeserializationContext> GetMessageEnumerator()
    {
        if (_fetchResponse == null ||
            (_stop?.IsCancellationRequested ?? true) ||
            _fetchResponse.Message.ErrorCode != 0)
        {
            yield break;
        }

        foreach (var response in _fetchResponse.Message.Responses!)
        {
            string? topic = ResolveTopicName(response.Topic, response.TopicId);
            if (topic == null)
            {
                continue;
            }

            foreach (var partition in response.Partitions!)
            {
                foreach (var recordBatch in partition.Records!.RecordBatches!)
                {
                    var firstTimestamp = DateTimeOffset.FromUnixTimeMilliseconds(recordBatch.FirstTimestamp).DateTime;
                    foreach (var record in recordBatch.Records!)
                    {
                        yield return new MessageDeserializationContext
                        {
                            Topic = topic,
                            Partition = partition.PartitionIndex!.Value,
                            Offset = recordBatch.BaseOffset + record.OffsetDelta,
                            Timestamp = firstTimestamp.AddMilliseconds(record.TimestampDelta),
                            Key = record.Key,
                            Value = record.Value,
                            Headers = record.Headers,
                        };
                    }
                }
            }
        }
    }

    private string? ResolveTopicName(string? name, Guid? topicId)
    {
        if (!string.IsNullOrEmpty(name)) return name;
        if (topicId != null && _topicsMetadataById!.TryGetValue(topicId.Value, out var metadata))
            return metadata.Name;
        return null;
    }

    public async ValueTask CommitAsync(ConsumeResult<TMessage> consumeResult, CancellationToken cancellationToken)
    {
        if (_topicsMetadata?.TryGetValue(consumeResult.Topic, out var topicMetadata) != true ||
            topicMetadata == null)
        {
            _logger.LogError(
                "Topic metadata was not found. Commit failed. {topic} {partition} {offset}",
                consumeResult.Topic,
                consumeResult.Partition,
                consumeResult.Offset);
            return;
        }

        var partitionMetadata = topicMetadata!.Partitions?.FirstOrDefault(x => x.PartitionIndex == consumeResult.Partition);
        if (partitionMetadata == null)
        {
            _logger.LogError(
                "Partition metadata was not found. Commit failed. {topic} {partition} {offset}",
                consumeResult.Topic,
                consumeResult.Partition,
                consumeResult.Offset);
            return;
        }

        if (!_connections.TryGetValue(partitionMetadata.LeaderId!.Value, out var connection))
        {
            _logger.LogError(
                "Broker node {nodeId} not found in connections. Commit failed. {topic} {partition} {offset}",
                partitionMetadata.LeaderId,
                consumeResult.Topic,
                consumeResult.Partition,
                consumeResult.Offset);
            return;
        }
        var sw = System.Diagnostics.Stopwatch.StartNew();
        using var _commitActivity = KafkaTracing.Source.StartActivity("Consumer.Commit", ActivityKind.Client);
        var commitContext = _context with
        {
            TopicName = consumeResult.Topic,
            PartitionId = consumeResult.Partition.ToString(),
        };
        _commitActivity?.AddMessagingAttributes(commitContext, KafkaMetrics.OperationSettle);
        using var _offsetCommitActivity = KafkaTracing.Source.StartActivity("Consumer.OffsetCommit", ActivityKind.Client);
        await _offsetStorage.SetAsync(
            connection,
            _config.GroupId,
            consumeResult.Topic,
            consumeResult.Partition,
            consumeResult.Offset,
            cancellationToken);
        _offsetCommitActivity?.AddTag("nKafka.phase", "offset_commit");
        _commitActivity?.AddTag("nKafka.phase", "commit");
        sw.Stop();
        KafkaMetrics.RecordClientOperation(_context, KafkaMetrics.OperationSettle, sw.Elapsed.TotalMilliseconds);
    }

    public async ValueTask DisposeAsync()
    {
        using var _ = BeginDefaultLoggingScope();
        using var _shutdownActivity = KafkaTracing.Source.StartActivity("Consumer.Shutdown", ActivityKind.Client);

        using var _stopFetchingActivity = KafkaTracing.Source.StartActivity("Consumer.StopFetching", ActivityKind.Client);
        await StopFetchingAsync();
        _stopFetchingActivity?.AddTag("nKafka.phase", "stop_fetching");

        _consumeChannel.Writer.TryComplete();

        if (_heartbeatsBackgroundTask != null)
        {
            await _heartbeatsBackgroundTask;
            _heartbeatsBackgroundTask = null;
        }

        await LeaveGroupAsync(CancellationToken.None);

        await CloseConnectionsAsync();
    }

    private async ValueTask StopFetchingAsync()
    {
        if (_stop == null)
        {
            return;
        }

        await _stop.CancelAsync();

        if (_fetchTasks != null)
        {
            await Task.WhenAll(_fetchTasks.Select(t => t.ContinueWith(task =>
            {
                if (task.IsFaulted && task.Exception!.InnerException is not OperationCanceledException and not TaskCanceledException)
                {
                    throw task.Exception!.InnerException!;
                }
            }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default)));
            _fetchTasks = null;
        }

        if (_fetchResult != null)
        {
            _fetchResult.Dispose();
            _fetchResult = null;
        }

        if (_messageDeserializeEnumerator != null)
        {
            _messageDeserializeEnumerator.Dispose();
            _messageDeserializeEnumerator = null;
        }
    }

    private async ValueTask LeaveGroupAsync(CancellationToken cancellationToken)
    {
        if (_groupMembership == null)
        {
            return;
        }

        using var _leaveGroupActivity = KafkaTracing.Source.StartActivity("Consumer.LeaveGroup", ActivityKind.Client);

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
        using var response = await _coordinatorConnection!.SendAsync(request, cancellationToken);
        _leaveGroupActivity?.AddTag("nKafka.phase", "leave_group_request");

        if (response.Message.ErrorCode != 0)
        {
            throw new ProtocolException($"Failed to leave consumer group. Error code {response.Message.ErrorCode}");
        }

        _groupMembership = null;
    }

    private async ValueTask CloseConnectionsAsync()
    {
        if (_connections.Count > 0)
        {
            var closeRequests = _connections.Values
                .Select(x => x.DisposeAsync().AsTask());
            await Task.WhenAll(closeRequests);

            _connections.Clear();
        }

        if (_coordinatorConnection != null)
        {
            await _coordinatorConnection.DisposeAsync();
            _coordinatorConnection = null;
        }
    }

    private sealed class ConsumerBatch(Consumer<TMessage> consumer) : IConsumerBatch<TMessage>
    {
        private readonly Consumer<TMessage> _consumer = consumer;
        private IEnumerator<ConsumeResult<TMessage>>? _enumerator;
        private bool _disposed;

        public IEnumerator<ConsumeResult<TMessage>> GetEnumerator()
        {
            _enumerator = GetEnumeratorCore();
            return _enumerator;
        }

        private IEnumerator<ConsumeResult<TMessage>> GetEnumeratorCore()
        {
            try
            {
                while (_consumer._messageDeserializeEnumerator?.MoveNext() == true)
                {
                    var deserializationContext = _consumer._messageDeserializeEnumerator.Current;
                    var message = _consumer._deserializer.Deserialize(deserializationContext);
                    yield return new ConsumeResult<TMessage>
                    {
                        Topic = deserializationContext.Topic,
                        Partition = deserializationContext.Partition,
                        Offset = deserializationContext.Offset,
                        Timestamp = deserializationContext.Timestamp,
                        Message = message,
                    };
                }
            }
            finally
            {
                Dispose();
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _enumerator?.Dispose();
            _enumerator = null;
            _consumer._messageDeserializeEnumerator?.Dispose();
            _consumer._messageDeserializeEnumerator = null;
            _consumer._fetchResult?.Dispose();
            _consumer._fetchResult = null;
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
