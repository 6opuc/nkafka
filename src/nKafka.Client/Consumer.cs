using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using Microsoft.Extensions.Logging;
using nKafka.Contracts;
using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageDefinitions.ApiVersionsResponseNested;
using nKafka.Contracts.MessageDefinitions.ConsumerProtocolAssignmentNested;
using nKafka.Contracts.MessageDefinitions.FetchRequestNested;
using nKafka.Contracts.MessageDefinitions.JoinGroupRequestNested;
using nKafka.Contracts.MessageDefinitions.LeaveGroupRequestNested;
using nKafka.Contracts.MessageDefinitions.MetadataRequestNested;
using nKafka.Contracts.MessageDefinitions.SyncGroupRequestNested;
using nKafka.Contracts.RequestClients;

namespace nKafka.Client;

public class Consumer<TMessage> : IConsumer<TMessage>
{
    private readonly ConsumerConfig _config;
    private readonly IMessageDeserializer<TMessage> _deserializer;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger _logger;
    
    private CancellationTokenSource? _stop;
    private Task? _heartbeatsBackgroundTask;
    private readonly Dictionary<int, IConnection> _connections = new ();
    private IDictionary<short, ApiVersion>? _apiVersionsSupported = new Dictionary<short, ApiVersion>();
    private ConcurrentDictionary<ApiKey, short> _apiVersionsChosen = new ();
    private int _coordinatorNodeId;
    private string[] _topics;
    private JoinGroupResponse? _joinGroupResponse;
    private MetadataResponse? _metadataResponse;
    
    private IList<(int NodeId, string Topic, Guid? TopicId, int Partition, long Offset)> _fetchQueue = [];
    private int _fetchIndex = 0;
    private FetchResponse? _fetchResponse = null;
    private IEnumerator<MessageDeserializationContext>? _messageDeserializeEnumerator = null;
    
    public Consumer(
        ConsumerConfig config,
        IMessageDeserializer<TMessage> deserializer,
        ILoggerFactory loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(deserializer);
        ArgumentNullException.ThrowIfNull(loggerFactory);
        _config = config;
        _deserializer = deserializer;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<Consumer<TMessage>>();
        _topics = _config.Topics.Split(",", StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
    }
    
    public async ValueTask JoinGroupAsync(CancellationToken cancellationToken)
    {
        #warning move to heartbeats?
        _stop = new CancellationTokenSource();
        
        using var _ = BeginDefaultLoggingScope();
        await OpenConnectionsAsync(cancellationToken);
        var connection = GetCoordinatorConnection();
        _joinGroupResponse = await JoinGroupRequestAsync(connection, cancellationToken);
        if (_joinGroupResponse.MemberId == _joinGroupResponse.Leader)
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
        await using var bootstrapConnection = await OpenBootstrapConnectionAsync(cancellationToken);
        await RequestApiVersionsAsync(bootstrapConnection, cancellationToken);
        await OpenCoordinatorConnectionAsync(bootstrapConnection, cancellationToken);
        _metadataResponse = await RequestMetadata(bootstrapConnection, cancellationToken);
        foreach (var broker in _metadataResponse.Brokers!.Values)
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
                _config.RequestBufferSize);
            var connection = new Connection(connectionConfig, _loggerFactory);
            await connection.OpenAsync(cancellationToken);
            _connections[broker.NodeId!.Value] = connection;
#warning reuse bootstrap connection
        }
    }

    private async ValueTask<IConnection> OpenBootstrapConnectionAsync(
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
                _config.RequestBufferSize,
                _config.RequestBufferSize);
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

    private async ValueTask RequestApiVersionsAsync(
        IConnection connection,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Requesting API versions.");
        
        var requestClient = new ApiVersionsRequestClient(0, new ApiVersionsRequest
        {
            ClientSoftwareName = "nKafka.Client",
            ClientSoftwareVersion = ClientVersionGetter.Version,
        });
        
        var response = await connection.SendAsync(requestClient, cancellationToken);
        if (response.ErrorCode != 0)
        {
            throw new Exception($"Failed to choose API versions for '{_config.BootstrapServers}'. Error code: {response.ErrorCode}");
        }

        _apiVersionsSupported = response.ApiKeys;
        _apiVersionsChosen = new ConcurrentDictionary<ApiKey, short>();
    }
    
    private async ValueTask<IConnection> OpenCoordinatorConnectionAsync(
        IConnection connection,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Opening coordinator connection.");
        
        var apiVersion = GetApiVersion(
            FindCoordinatorRequestClient.Api,
            FindCoordinatorRequestClient.ValidVersions);
        var requestClient = new FindCoordinatorRequestClient(
            apiVersion,
            new FindCoordinatorRequest
            {
                Key = _config.GroupId,
                KeyType = 0,
                CoordinatorKeys = [_config.GroupId],
            });
        
        var response = await connection.SendAsync(requestClient, CancellationToken.None);

        ConnectionConfig coordinatorConnectionConfig;
        if (apiVersion < 4)
        {
            if (response.ErrorCode != 0)
            {
                throw new Exception($"Failed to find group coordinator. Error code {response.ErrorCode}");
            }

            _coordinatorNodeId = response.NodeId!.Value;
            coordinatorConnectionConfig = new ConnectionConfig(
                _config.Protocol,
                response.Host!,
                response.Port!.Value,
                _config.ClientId,
                _config.ResponseBufferSize,
                _config.RequestBufferSize);
        }
        else
        {
            var coordinator = response.Coordinators?.FirstOrDefault(x => x.Key == _config.GroupId);
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
                _config.RequestBufferSize);
        }

        var coordinatorConnection = new Connection(coordinatorConnectionConfig, _loggerFactory);
        await coordinatorConnection.OpenAsync(cancellationToken);
        
        _connections[_coordinatorNodeId] = coordinatorConnection;

        return coordinatorConnection;
    }
    
    private short GetApiVersion(ApiKey api, VersionRange validVersionRange)
    {
        #warning refactor and generalize, don't use concurrent dictionary at all, initialize once for connection
        if (_apiVersionsChosen.TryGetValue(api, out var apiVersion))
        {
            return apiVersion;
        }

        apiVersion = 0;
        if (_apiVersionsSupported != null &&
            _apiVersionsSupported.TryGetValue((short)api, out var supportedVersions))
        {
            var supportedVersionRange = new VersionRange(
                supportedVersions.MinVersion!.Value, supportedVersions.MaxVersion!.Value);
            var intersection = validVersionRange.Intersect(supportedVersionRange);
            if (!intersection.IsNone)
            {
                apiVersion = intersection.To!.Value;
            }
        }

        _apiVersionsChosen[api] = apiVersion;
        return apiVersion;
    }
    
    private async ValueTask<MetadataResponse> RequestMetadata(IConnection connection, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Requesting metadata.");
        
        var apiVersion = GetApiVersion(
            MetadataRequestClient.Api,
            MetadataRequestClient.ValidVersions);
        var requestClient = new MetadataRequestClient(apiVersion, new MetadataRequest
        {
            Topics = _topics.Select(x => new MetadataRequestTopic
            {
                Name = x,
                TopicId = Guid.Empty,
            }).ToArray(),
            AllowAutoTopicCreation = false,
            IncludeClusterAuthorizedOperations = false,
            IncludeTopicAuthorizedOperations = false,
        });
        
        var response = await connection.SendAsync(requestClient, cancellationToken);

        foreach (var topicName in _topics)
        {
            if (!response.Topics!.TryGetValue(topicName, out var topic))
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
    
    private async Task<JoinGroupResponse> JoinGroupRequestAsync(
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
        
        var apiVersion = GetApiVersion(
            JoinGroupRequestClient.Api,
            JoinGroupRequestClient.ValidVersions);
        var requestClient = new JoinGroupRequestClient(apiVersion, request);
        var response = await connection.SendAsync(requestClient, cancellationToken);
        
        if (apiVersion == 4 && response.ErrorCode == (short)ErrorCode.MemberIdRequired)
        {
            // retry with given member id
            request.MemberId = response.MemberId;
            response = await connection.SendAsync(requestClient, cancellationToken);
        }

        if (response.ErrorCode != 0)
        {
            throw new Exception($"Failed to join consumer group. Error code {response.ErrorCode}");
        }
        
        return response;
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
            if (!_metadataResponse!.Topics!.TryGetValue(topicName, out var topic))
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
                MemberId = _joinGroupResponse!.MemberId,
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
        
        var apiVersion = GetApiVersion(
            SyncGroupRequestClient.Api,
            SyncGroupRequestClient.ValidVersions);
        var requestClient = new SyncGroupRequestClient(apiVersion, new SyncGroupRequest
        {
            GroupId = _config.GroupId,
            GenerationId = _joinGroupResponse!.GenerationId,
            MemberId = _joinGroupResponse.MemberId,
            GroupInstanceId = null,
            ProtocolType = "consumer",
            ProtocolName = "nkafka-consumer",
            Assignments = assignments,
        });
        var response = await connection.SendAsync(requestClient, cancellationToken);

        if (response.ErrorCode != 0)
        {
            throw new Exception($"Failed to synchronize consumer group. Error code {response.ErrorCode}");
        }

        var actualAssignments = response.Assignment!;

        _fetchQueue = new List<(int NodeId, string Topic, Guid? TopicId, int Partition, long lastOffset)>();
        foreach (var topicAssignment in actualAssignments.AssignedPartitions!)
        {
            if (_metadataResponse?.Topics?.TryGetValue(topicAssignment.Key, out var topicMetadata) != true)
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
                
                _fetchQueue.Add((partitionMetadata.LeaderId!.Value, topicAssignment.Key, topicMetadata?.TopicId, partition, 0));
            }
        }

        _fetchIndex = 0;
        _messageDeserializeEnumerator = null;
    }
    
    private void StartSendingHeartbeats()
    {
        //return;
        #warning if no poll request for more than MaxPollIntervalMs, then leave the group
        
        if (_stop == null)
        {
            return;
        }

        var cancellationToken = _stop.Token;
        _heartbeatsBackgroundTask = Task.Run(
            async () =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var connection = GetCoordinatorConnection();
                        
                        var apiVersion = GetApiVersion(
                            HeartbeatRequestClient.Api,
                            HeartbeatRequestClient.ValidVersions);
                        var requestClient = new HeartbeatRequestClient(apiVersion, new HeartbeatRequest
                        {
                            GroupId = _config.GroupId,
                            GenerationId = _joinGroupResponse.GenerationId,
                            MemberId = _joinGroupResponse.MemberId,
                            GroupInstanceId = null, // ???
                        });
                        var response = await connection.SendAsync(requestClient, CancellationToken.None);
                        
                        if (response.ErrorCode == 0)
                        {
                            _logger.LogDebug(
                                "Heartbeat was sent. Waiting for {@interval}ms.",
                                _config.HeartbeatIntervalMs);
                        }
                        else
                        {
                            _logger.LogError(
                                "Error in heartbeat response: {@errorCode}. Waiting for {@interval}ms.",
                                response.ErrorCode,
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

    public async ValueTask<ConsumeResult<TMessage>?> ConsumeAsync(CancellationToken cancellationToken)
    {
#warning consume queue: Deserialization of fetch response should publish to consume queue

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
        
        var apiVersion = GetApiVersion(
            FetchRequestClient.Api,
            FetchRequestClient.ValidVersions);
        var requestClient = new FetchRequestClient(apiVersion, new FetchRequest
        {
            ClusterId = null, // ???
            ReplicaId = -1, // ???
            ReplicaState = null, // ???
            MaxWaitMs = 0, // ???
            MinBytes = 0, // ???
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
        });
        var connection = _connections[fetchSource.NodeId];
        _fetchResponse = await connection.SendAsync(requestClient, CancellationToken.None);

        var lastOffset = _fetchResponse
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
            _fetchResponse.ErrorCode != 0)
        {
            yield break;
        }

        foreach (var response in _fetchResponse.Responses!)
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

        return null;
    }

    public async ValueTask DisposeAsync()
    {
        using var _ = BeginDefaultLoggingScope();
        
        if (_stop != null)
        {
            await _stop.CancelAsync();
            if (_heartbeatsBackgroundTask != null)
            {
                await _heartbeatsBackgroundTask;
                _heartbeatsBackgroundTask = null;
            }
        }
        
        await LeaveGroupAsync(GetCoordinatorConnection(), CancellationToken.None);

        await CloseConnectionsAsync();
    }

    private async ValueTask LeaveGroupAsync(IConnection connection, CancellationToken cancellationToken)
    {
        if (_joinGroupResponse == null)
        {
            return;
        }
        _logger.LogInformation("Leaving consumer group.");
        
        var apiVersion = GetApiVersion(
            LeaveGroupRequestClient.Api,
            LeaveGroupRequestClient.ValidVersions);
        var requestClient = new LeaveGroupRequestClient(apiVersion, new LeaveGroupRequest
        {
            GroupId = _config.GroupId,
            MemberId = _joinGroupResponse.MemberId,
            Members = [
                new MemberIdentity
                {
                    MemberId = _joinGroupResponse.MemberId,
                    GroupInstanceId = null,
                    Reason = null,
                }
            ],
        });
        var response = await connection.SendAsync(requestClient, cancellationToken);

        if (response.ErrorCode != 0)
        {
            throw new Exception($"Failed to leave consumer group. Error code {response.ErrorCode}");
        }

        _joinGroupResponse = null;
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