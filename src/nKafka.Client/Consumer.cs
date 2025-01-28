using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using nKafka.Contracts;
using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageDefinitions.ApiVersionsResponseNested;
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
    
    private CancellationTokenSource? _stopSendingHeartBeats;
    private Task? _heartbeatsBackgroundTask;
    private readonly Dictionary<int, IConnection> _connections = new ();
    private int _coordinatorNodeId;
    private readonly string[] _topics;
    private GroupMembership? _groupMembership;
    private IDictionary<string, MetadataResponseTopic>? _topicsMetadata;
    
    private IList<(int NodeId, string Topic, Guid? TopicId, int Partition, long Offset)> _fetchQueue = [];
    private int _fetchIndex = 0;
    private IDisposableMessage<FetchResponse>? _fetchResponse = null;
    private IEnumerator<MessageDeserializationContext>? _messageDeserializeEnumerator = null;
    
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
        _topics = _config.Topics.Split(",", StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
    }
    
    public async ValueTask JoinGroupAsync(CancellationToken cancellationToken)
    {
        using var _ = BeginDefaultLoggingScope();
        await OpenConnectionsAsync(cancellationToken);
        var connection = GetCoordinatorConnection();
        using var joinGroupResponse = await JoinGroupRequestAsync(connection, cancellationToken);
        _groupMembership = new GroupMembership
        {
            MemberId = joinGroupResponse.Message.MemberId!,
            LeaderId = joinGroupResponse.Message.Leader!,
            GenerationId = joinGroupResponse.Message.GenerationId,
        };
        if (_groupMembership.MemberId == _groupMembership.LeaderId)
        {
            _logger.LogInformation("Promoted as a leader.");
            IList<SyncGroupRequestAssignment> assignments = ReassignGroup();
            await SyncGroup(connection, assignments, cancellationToken);
        }
        
        StartSendingHeartbeats();
    }

    private IDisposable? BeginDefaultLoggingScope()
    {
        return _logger.BeginScope(_config.InstanceId);
    }

    private IConnection GetCoordinatorConnection()
    {
        return _connections[_coordinatorNodeId];
    }

    private async ValueTask OpenConnectionsAsync(
        CancellationToken cancellationToken)
    {
        await using (var bootstrapConnection = await OpenBootstrapConnectionAsync(cancellationToken))
        {
            await OpenCoordinatorConnectionAsync(bootstrapConnection, cancellationToken);
        }
        using var metadataResponse = await RequestMetadata(GetCoordinatorConnection(), cancellationToken);
        _topicsMetadata = metadataResponse.Message.Topics;
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
                _config.RequestBufferSize)
            {
                CheckCrcs = _config.CheckCrcs,
            };
            var connection = new Connection(connectionConfig, _loggerFactory);
            await connection.OpenAsync(cancellationToken);
            _connections[broker.NodeId!.Value] = connection;
        }
    }

    private async ValueTask<Connection> OpenBootstrapConnectionAsync(
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Opening bootstrap connection.");
        
        var connectionStrings = _config.BootstrapServers.Split(
            ",",
            StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        foreach (var connectionString in connectionStrings)
        {
            var connectionConfig = new ConnectionConfig(
                connectionString,
                _config.ClientId,
                _config.RequestBufferSize,
                _config.RequestBufferSize)
            {
                RequestApiVersionsOnOpen = false,
            };
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

        throw new Exception("No connection could be established.");
    }
    
    private async ValueTask<IConnection> OpenCoordinatorConnectionAsync(
        IConnection connection,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Opening coordinator connection.");
        
        var request = new FindCoordinatorRequest
        {
            Key = _config.GroupId,
            KeyType = 0,
            CoordinatorKeys = [_config.GroupId],
        };
        
        using var response = await connection.SendAsync(request, CancellationToken.None);

        ConnectionConfig coordinatorConnectionConfig;
        if (response.Version < 4)
        {
            if (response.Message.ErrorCode != 0)
            {
                throw new Exception($"Failed to find group coordinator. Error code {response.Message.ErrorCode}");
            }

            _coordinatorNodeId = response.Message.NodeId!.Value;
            coordinatorConnectionConfig = new ConnectionConfig(
                _config.Protocol,
                response.Message.Host!,
                response.Message.Port!.Value,
                _config.ClientId,
                _config.ResponseBufferSize,
                _config.RequestBufferSize)
            {
                CheckCrcs = _config.CheckCrcs,
            };
        }
        else
        {
            var coordinator = response.Message.Coordinators?.FirstOrDefault(x => x.Key == _config.GroupId);
            if (coordinator == null)
            {
                throw new Exception($"Failed to find group coordinator. Response did not match coordinator key '{_config.GroupId}'.");
            }

            if (coordinator.ErrorCode != 0)
            {
                throw new Exception($"Failed to find group coordinator. Error code {coordinator.ErrorCode}");
            }

            _coordinatorNodeId = coordinator.NodeId!.Value;
            coordinatorConnectionConfig = new ConnectionConfig(
                _config.Protocol,
                coordinator.Host!,
                coordinator.Port!.Value,
                _config.ClientId,
                _config.ResponseBufferSize,
                _config.RequestBufferSize)
            {
                CheckCrcs = _config.CheckCrcs,
            };
        }

        var coordinatorConnection = new Connection(coordinatorConnectionConfig, _loggerFactory);
        await coordinatorConnection.OpenAsync(cancellationToken);
        
        _connections[_coordinatorNodeId] = coordinatorConnection;

        return coordinatorConnection;
    }
    
    private async ValueTask<IDisposableMessage<MetadataResponse>> RequestMetadata(IConnection connection, CancellationToken cancellationToken)
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

        foreach (var topicName in _topics)
        {
            if (!response.Message.Topics!.TryGetValue(topicName, out var topic))
            {
                _logger.LogInformation($"No topic metadata found for topic {topicName}.");
                continue;
            }
            
            if (topic.ErrorCode != 0)
            {
                throw new Exception($"Metadata request failed for topic {topic.Name}. Error code {topic.ErrorCode}.");
            }

            foreach (var partition in topic.Partitions!)
            {
                if (partition.ErrorCode != 0)
                {
                    throw new Exception($"Metadata request failed for topic {topic.Name} partition {partition.PartitionIndex}. Error code {partition.ErrorCode}");
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
                                UserData = null, // ???
                                OwnedPartitions = null, // ???
                                GenerationId = -1, // ???
                                RackId = null // ???
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
            throw new Exception($"Failed to join consumer group. Error code {response.Message.ErrorCode}");
        }
        
        return response;
    }

    private class GroupMembership
    {
        public required string MemberId { get; init; }
        public required string LeaderId { get; init; }
        public int? GenerationId { get; init; }
    }

    private IList<SyncGroupRequestAssignment> ReassignGroup()
    {
        #warning implement reassignment logic for all members. take into account current assignments if possible
        var requestedAssignment = new ConsumerProtocolAssignment
        {
            AssignedPartitions = new Dictionary<string, TopicPartition>(),
        };
        foreach (var topicName in _topics)
        {
            if (!_topicsMetadata!.TryGetValue(topicName, out var topic))
            {
                continue;
            }
            
            requestedAssignment.AssignedPartitions[topicName] = new TopicPartition
            {
                Topic = topicName,
                Partitions = topic.Partitions!.Select(x => x.PartitionIndex!.Value).ToArray(),
            };
        }

        return
        [
            new SyncGroupRequestAssignment
            {
                MemberId = _groupMembership!.MemberId,
                Assignment = requestedAssignment,
            }
        ];
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
            throw new Exception($"Failed to synchronize consumer group. Error code {response.Message.ErrorCode}");
        }

        var actualAssignments = response.Message.Assignment!;

        _fetchQueue = new List<(int NodeId, string Topic, Guid? TopicId, int Partition, long lastOffset)>();
        foreach (var topicAssignment in actualAssignments.AssignedPartitions!)
        {
            if (_topicsMetadata?.TryGetValue(topicAssignment.Key, out var topicMetadata) != true)
            {
                continue;
            }

            foreach (var partition in topicAssignment.Value.Partitions!)
            {
                var partitionMetadata = topicMetadata?.Partitions?.FirstOrDefault(x => x.PartitionIndex == partition);
                if (partitionMetadata == null)
                {
                    continue;
                }

                #warning start offset does not work
                var offset = await _offsetStorage.GetOffset(_config.GroupId, topicAssignment.Key, partition);
                _fetchQueue.Add((partitionMetadata.LeaderId!.Value, topicAssignment.Key, topicMetadata?.TopicId, partition, offset));
            }
        }

        _fetchIndex = 0;
        _messageDeserializeEnumerator = null;
    }
    
    private void StartSendingHeartbeats()
    {
        _stopSendingHeartBeats = new CancellationTokenSource();

        var cancellationToken = _stopSendingHeartBeats.Token;
        _heartbeatsBackgroundTask = Task.Run(
            async () =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var connection = GetCoordinatorConnection();
                        
                        var request = new HeartbeatRequest
                        {
                            GroupId = _config.GroupId,
                            GenerationId = _groupMembership!.GenerationId,
                            MemberId = _groupMembership.MemberId,
                            GroupInstanceId = null, // ???
                        };
                        using var response = await connection.SendAsync(request, CancellationToken.None);
                        
                        if (response.Message.ErrorCode == 0)
                        {
                            _logger.LogDebug(
                                "Heartbeat was sent. Waiting for {@interval}ms.",
                                _config.HeartbeatIntervalMs);
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

    public async ValueTask<ConsumeResult<TMessage>?> ConsumeAsync(
        TimeSpan maxWaitTime,
        CancellationToken cancellationToken)
    {
#warning consume queue: Deserialization of fetch response should publish to consume queue
#warning read from all partitions at once. if we reach the end of partition, we should wait for all partitions
        

        var consumerResult = ConsumeFromBuffer();
        if (consumerResult != null)
        {
            return consumerResult;
        }

        if (_fetchQueue.Count == 0)
        {
            return null;
        }
        var fetchSource = _fetchQueue[_fetchIndex%_fetchQueue.Count];
        
        var request = new FetchRequest
        {
            ClusterId = null, // ???
            ReplicaId = -1, // ???
            ReplicaState = null, // ???
            MaxWaitMs = (int)maxWaitTime.TotalMilliseconds,
            MinBytes = 1, // ???
            MaxBytes = 0x7fffffff,
            IsolationLevel = 0, // !!!
            SessionId = 0, // ???
            SessionEpoch = -1, // ???
            Topics =
            [
                new FetchTopic
                {
                    Topic = fetchSource.Topic,
                    TopicId = fetchSource.TopicId,
                    Partitions =
                    [
                        new FetchPartition
                        {
                            Partition = fetchSource.Partition,
                            CurrentLeaderEpoch = -1, // ???
                            FetchOffset = fetchSource.Offset, // ???
                            LastFetchedEpoch = -1, // ???
                            LogStartOffset = -1, // ???
                            PartitionMaxBytes = 512 * 1024, // !!!
                            ReplicaDirectoryId = Guid.Empty, // ???
                        }
                    ]
                },
            ],
            ForgottenTopicsData = [], // ???
            RackId = string.Empty, // ???
        };
        var connection = _connections[fetchSource.NodeId];
        _fetchResponse = await connection.SendAsync(request, cancellationToken);

        var lastOffset = _fetchResponse.Message
            .Responses?.LastOrDefault()?
            .Partitions?.LastOrDefault()?
            .Records?.LastOffset;
        if (lastOffset.HasValue)
        {
            _fetchQueue[_fetchIndex % _fetchQueue.Count] = fetchSource with
            {
                Offset = lastOffset.Value + 1,
            };
        }

        _fetchIndex += 1;

        _messageDeserializeEnumerator = GetMessageEnumerator(fetchSource.Topic);
        
        return ConsumeFromBuffer();
    }

    private IEnumerator<MessageDeserializationContext> GetMessageEnumerator(string topic)
    {
        if (_fetchResponse == null ||
            _fetchResponse.Message.ErrorCode != 0)
        {
            yield break;
        }

        foreach (var response in _fetchResponse.Message.Responses!)
        {
            foreach (var partition in response.Partitions!)
            {
                foreach (var recordBatch in partition.Records!.RecordBatches!)
                {
                    foreach (var record in recordBatch.Records!)
                    {
                        yield return new MessageDeserializationContext
                        {
                            Topic = topic,
                            Partition = partition.PartitionIndex!.Value,
                            Offset = recordBatch.BaseOffset + record.OffsetDelta,
                            Timestamp = DateTimeOffset
                                .FromUnixTimeMilliseconds(recordBatch.FirstTimestamp + record.TimestampDelta)
                                .DateTime,
                            Key = record.Key,
                            Value = record.Value,
                            Headers = record.Headers,
                        };
                    }
                }
            }
        }
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
        _fetchResponse?.Dispose();

        return null;
    }

    public async ValueTask DisposeAsync()
    {
        using var _ = BeginDefaultLoggingScope();
        
        if (_stopSendingHeartBeats != null)
        {
            await _stopSendingHeartBeats.CancelAsync();
            if (_heartbeatsBackgroundTask != null)
            {
                await _heartbeatsBackgroundTask;
                _heartbeatsBackgroundTask = null;
            }
        }
        
        await LeaveGroupAsync(GetCoordinatorConnection(), CancellationToken.None);

        await CloseConnectionsAsync();
        
        _fetchResponse?.Dispose();
    }

    private async ValueTask LeaveGroupAsync(IConnection connection, CancellationToken cancellationToken)
    {
        if (_groupMembership == null)
        {
            return;
        }
        _logger.LogInformation("Leaving consumer group.");
        
        var request = new LeaveGroupRequest
        {
            GroupId = _config.GroupId,
            MemberId = _groupMembership.MemberId,
            Members = [
                new MemberIdentity
                {
                    MemberId = _groupMembership.MemberId,
                    GroupInstanceId = null,
                    Reason = null,
                }
            ],
        };
        using var response = await connection.SendAsync(request, cancellationToken);

        if (response.Message.ErrorCode != 0)
        {
            throw new Exception($"Failed to leave consumer group. Error code {response.Message.ErrorCode}");
        }

        _groupMembership = null;
    }

    private async ValueTask CloseConnectionsAsync()
    {
        if (_connections.Count == 0)
        {
            return;
        }

        var closeRequests = _connections.Values
            .Select(x => x.DisposeAsync().AsTask());
        await Task.WhenAll(closeRequests);
        
        _connections.Clear();
    }
}