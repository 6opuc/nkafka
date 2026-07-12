using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using nKafka.Contracts;
using nKafka.Contracts.Exceptions;
using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageDefinitions.ConsumerProtocolAssignmentNested;
using nKafka.Contracts.MessageDefinitions.FetchRequestNested;

namespace nKafka.Client;

public sealed class Consumer<TMessage> : IConsumer<TMessage>
{
    private readonly ConsumerConfig _config;
    private readonly IOffsetStorage _offsetStorage;
    private readonly ILoggerFactory _loggerFactory;
    private readonly KafkaTelemetryContext _context;
    private readonly string[] _topics;

    private CancellationTokenSource _stop = new();
    private readonly Channel<FetchResult> _consumeChannel =
        Channel.CreateBounded<FetchResult>(new BoundedChannelOptions(1)
        {
            SingleReader = true,
        });

    private readonly IGroupCoordinator _coordinator;
    private readonly ConsumePipeline<TMessage> _pipeline;
    private List<Task>? _fetchTasks;

    public int GenerationId => _coordinator.GenerationId;

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
        _offsetStorage = offsetStorage;
        _loggerFactory = loggerFactory;
        _context = new KafkaTelemetryContext(
            config.GroupId,
            config.InstanceId ?? config.ClientId,
            config.BootstrapServers.Split(",", StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries).FirstOrDefault(),
            null,
            null,
            null);
        _topics = config.Topics.Split(",", StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

        _coordinator = new GroupCoordinator(config, offsetStorage, loggerFactory);
        _pipeline = new ConsumePipeline<TMessage>(
            _consumeChannel,
            config,
            loggerFactory.CreateLogger<Consumer<TMessage>>(),
            deserializer,
            _context);
    }

    public async ValueTask JoinGroupAsync(CancellationToken cancellationToken)
    {
        _coordinator.RebalanceRequired += OnRebalanceRequired;
        await _coordinator.JoinGroupAsync(cancellationToken);
    }

    private void OnRebalanceRequired()
    {
        _stop.Cancel();
        _stop = new CancellationTokenSource();

        Task.Run(async () =>
        {
            await StopFetchingAsync();

            var rejoinToken = _stop.Token;
            try
            {
                await _coordinator.JoinGroupAsync(rejoinToken);
            }
            catch (OperationCanceledException)
            {
            }

            if (_coordinator.AssignedPartitions != null)
            {
                await StartFetchingAsync(_coordinator.AssignedPartitions, rejoinToken);
            }
        }, CancellationToken.None);
    }

    public async ValueTask<ConsumeResult<TMessage>?> ConsumeAsync(CancellationToken cancellationToken)
    {
        using var consumeActivity = KafkaTracing.Source.StartActivity(KafkaTracing.BuildSpanName(KafkaMetrics.OperationNamePoll, _context), ActivityKind.Client);
        consumeActivity?.AddMessagingAttributes(_context);

        var consumerResult = await _pipeline.ConsumeNextAsync(cancellationToken);
        if (consumerResult.HasValue)
        {
            var value = consumerResult.Value;
            var contextWithMessage = _context with
            {
                TopicName = value.Topic,
                PartitionId = value.Partition.ToString(),
            };
            KafkaMetrics.AddMessagesConsumed(contextWithMessage, 1, KafkaMetrics.OperationNamePoll);
            consumeActivity?.AddTag("nKafka.result", "message");
        }
        else
        {
            consumeActivity?.AddTag("nKafka.result", "empty");
        }

        return consumerResult;
    }

    public async ValueTask<IConsumerBatch<TMessage>> ConsumeBatchAsync(CancellationToken cancellationToken)
    {
        using var consumeBatchActivity = KafkaTracing.Source.StartActivity(KafkaTracing.BuildSpanName(KafkaMetrics.OperationNamePoll, _context), ActivityKind.Client);
        consumeBatchActivity?.AddMessagingAttributes(_context);
        using (var fetchWaitActivity = KafkaTracing.StartAsCurrent(KafkaTracing.ActivityFetchWait))
        {
            fetchWaitActivity?.AddMessagingAttributes(_context);
            await _pipeline.ConsumeNextAsync(cancellationToken);
        }
        return _pipeline.CreateBatch(cancellationToken);
    }

    public async ValueTask CommitAsync(ConsumeResult<TMessage> consumeResult, CancellationToken cancellationToken)
    {
        var coordinator = (GroupCoordinator)_coordinator;
        if (coordinator.TopicsMetadata.TryGetValue(consumeResult.Topic, out var topicMetadata) == false ||
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

        var brokerConnections = coordinator.BrokerConnections;
        if (brokerConnections.TryGetValue(partitionMetadata.LeaderId!.Value, out var connection) == false)
        {
            _logger.LogError(
                "Broker node {nodeId} not found in connections. Commit failed. {topic} {partition} {offset}",
                partitionMetadata.LeaderId,
                consumeResult.Topic,
                consumeResult.Partition,
                consumeResult.Offset);
            return;
        }
        var sw = Stopwatch.StartNew();
        var commitContext = _context with
        {
            TopicName = consumeResult.Topic,
            PartitionId = consumeResult.Partition.ToString(),
        };
        using var commitActivity = KafkaTracing.Source.StartActivity(KafkaTracing.BuildSpanName(KafkaMetrics.OperationNameCommit, commitContext), ActivityKind.Client);
        commitActivity?.AddMessagingAttributes(commitContext);
        await _offsetStorage.SetAsync(
            connection,
            _config.GroupId,
            consumeResult.Topic,
            consumeResult.Partition,
            consumeResult.Offset,
            cancellationToken);
        sw.Stop();
        KafkaMetrics.RecordClientOperation(commitContext, KafkaMetrics.OperationNameCommit, KafkaMetrics.OperationTypeSettle, sw.Elapsed.TotalMilliseconds);
    }

    private async Task StartFetchingAsync(IDictionary<string, TopicPartition> assignedPartitions, CancellationToken cancellationToken)
    {
        var coordinator = (GroupCoordinator)_coordinator;
        var topicPartitionOffsets = new List<(string Topic, int Partition, long Offset, int NodeId)>();
        var sessionManagers = new Dictionary<int, FetchSessionManager>(coordinator.BrokerConnections.Count);

        foreach (var topicAssignment in assignedPartitions)
        {
            var topicName = topicAssignment.Key;
            if (coordinator.TopicsMetadata.TryGetValue(topicName, out var topicMetadata) == false ||
                topicMetadata == null)
            {
                continue;
            }

            foreach (var partition in topicAssignment.Value.Partitions!)
            {
                var partitionMetadata = topicMetadata!.Partitions?.FirstOrDefault(x => x.PartitionIndex == partition);
                if (partitionMetadata == null)
                {
                    continue;
                }

                var nodeId = partitionMetadata.LeaderId!.Value;
                if (!coordinator.BrokerConnections.TryGetValue(nodeId, out var connection))
                {
                    _logger.LogWarning("Broker node {nodeId} not found in connections, skipping partition {topic}/{partition}.", nodeId, topicName, partition);
                    continue;
                }

                if (!sessionManagers.TryGetValue(nodeId, out var sessionManager))
                {
                    var useSessions = connection.SupportsApiKeyVersion(ApiKey.Fetch, 4);
                    sessionManager = new FetchSessionManager(0, 0, useSessions);
                    sessionManagers[nodeId] = sessionManager;
                }

                var offset = await _offsetStorage.GetAsync(
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
            var nodeId = pair.Key;
            var sessionManager = pair.Value;
            var topicMap = nodeTopics.GetValueOrDefault(nodeId, new Dictionary<string, List<(int Partition, long Offset)>>());
            var fetchTask = Task.Run(() => FetchLoopAsync(nodeId, topicMap, sessionManager, cancellationToken), cancellationToken);
            _fetchTasks.Add(fetchTask);
        }
    }

    private async Task FetchLoopAsync(int nodeId, Dictionary<string, List<(int Partition, long Offset)>> topicMap, FetchSessionManager sessionManager, CancellationToken cancellationToken)
    {
        var coordinator = (GroupCoordinator)_coordinator;
        var contextStr = new System.Text.StringBuilder();
        foreach (var topic in topicMap)
        {
            contextStr.Append(topic.Key);
            contextStr.Append("[");
            foreach (var p in topic.Value)
            {
                contextStr.Append(p.Partition);
                contextStr.Append(",");
            }
            contextStr.Append("] ");
        }
        using (_logger.BeginScope(contextStr.ToString()))
        {
            _logger.LogInformation("Fetching started.");
            var consecutiveErrors = 0;
            var firstRequest = true;
            if (!coordinator.BrokerConnections.TryGetValue(nodeId, out var connection))
            {
                _logger.LogError("Broker node {nodeId} not found in connections during fetch loop.", nodeId);
                throw new ProtocolException($"Broker node {nodeId} not found in connections during fetch loop.");
            }

            while (!cancellationToken.IsCancellationRequested)
            {
                using var fetchActivity = KafkaTracing.StartAsCurrent(KafkaTracing.ActivityFetch);
                fetchActivity?.AddMessagingAttributes(_context);
                try
                {
                    var fetchRequest = CreateFetchRequest(sessionManager, topicMap, firstRequest);
                    firstRequest = false;

                    var fetchGenerationId = _coordinator.GenerationId;
                    var response = await connection.SendAsync(fetchRequest, cancellationToken);

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
                            await coordinator.JoinGroupAsync(cancellationToken);
                            continue;
                        }

                        _logger.LogError(
                            "Error in fetch response: {@errorCode}.",
                            response.Message.ErrorCode);
                    }
                    else
                    {
                        foreach (var topicResponse in response.Message.Responses!)
                        {
                            var topicName = coordinator.ResolveTopicName(topicResponse.Topic, topicResponse.TopicId);

                            if (string.IsNullOrEmpty(topicName) || !topicMap.TryGetValue(topicName, out var topicPartitions))
                            {
                                continue;
                            }

                            for (var pi = 0; pi < topicPartitions.Count; pi++)
                            {
                                var partitionResponse = topicResponse.Partitions!.FirstOrDefault(
                                    x => x.PartitionIndex == topicPartitions[pi].Partition);
                                if (partitionResponse == null)
                                {
                                    continue;
                                }

                                var lastOffset = partitionResponse.Records?.LastOffset;
                                if (lastOffset.HasValue)
                                {
                                    topicPartitions[pi] = (topicPartitions[pi].Partition, lastOffset.Value + 1);
                                }
                            }
                        }

                    }

                    consecutiveErrors = 0;
                    if (fetchGenerationId == _coordinator.GenerationId)
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
                    fetchActivity?.SetStatus(ActivityStatusCode.Error, exception.Message);
                    fetchActivity?.AddTag("exception.message", exception.Message);
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
            var coordinator = (GroupCoordinator)_coordinator;
            var fetchTopics = new List<FetchTopic>();
            foreach (var topicEntry in topicMap)
            {
                var topicName = topicEntry.Key;
                if (coordinator.TopicsMetadata.TryGetValue(topicName, out var topicMetadata) == false)
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

    private async ValueTask StopFetchingAsync()
    {
        using var _ = _logger.BeginScope(_config.InstanceId);
        using var stopFetchingActivity = KafkaTracing.StartAsCurrent(KafkaTracing.ActivityStopFetching);
        stopFetchingActivity?.AddMessagingAttributes(_context);

        if (_stop.IsCancellationRequested)
        {
            return;
        }

        await _stop.CancelAsync();

        if (_fetchTasks != null)
        {
            var tasks = _fetchTasks.ToList();
            _fetchTasks = null;
            try
            {
                await Task.WhenAll(tasks);
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        using var _ = _logger.BeginScope(_config.InstanceId);
        using var shutdownActivity = KafkaTracing.StartAsCurrent(KafkaTracing.ActivityShutdown);
        shutdownActivity?.AddMessagingAttributes(_context);

        await StopFetchingAsync();

        _consumeChannel.Writer.TryComplete();

        _coordinator.RebalanceRequired -= OnRebalanceRequired;

        await _coordinator.DisposeAsync();

        await _pipeline.DisposeAsync();

        _stop.Dispose();
    }

    private ILogger _logger => _loggerFactory.CreateLogger<Consumer<TMessage>>();
}
