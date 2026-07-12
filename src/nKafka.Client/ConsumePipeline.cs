using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace nKafka.Client;

public sealed class ConsumePipeline<TMessage> : IConsumePipeline<TMessage>
{
    private readonly Channel<FetchResult> _channel;
    private readonly ConsumerConfig _config;
    private readonly ILogger _logger;
    private readonly IMessageDeserializer<TMessage> _deserializer;
    private readonly KafkaTelemetryContext _context;

    private FetchResult? _fetchResult;
    private IEnumerator<MessageDeserializationContext>? _enumerator;
    private bool _disposed;

    public ConsumePipeline(
        Channel<FetchResult> channel,
        ConsumerConfig config,
        ILogger logger,
        IMessageDeserializer<TMessage> deserializer,
        KafkaTelemetryContext context)
    {
        _channel = channel;
        _config = config;
        _logger = logger;
        _deserializer = deserializer;
        _context = context;
    }

    public async ValueTask<ConsumeResult<TMessage>?> ConsumeNextAsync(CancellationToken cancellationToken)
    {
        using var consumeActivity = KafkaTracing.Source.StartActivity(KafkaTracing.BuildSpanName(KafkaMetrics.OperationNamePoll, _context), ActivityKind.Client);
        consumeActivity?.AddMessagingAttributes(_context);

        var consumerResult = ConsumeFromEnumerator();
        if (consumerResult != null)
        {
            KafkaMetrics.AddMessagesConsumed(_context, 1, KafkaMetrics.OperationNamePoll);
            consumeActivity?.AddTag("nKafka.result", "cached");
            return consumerResult;
        }

        using (var fetchWaitActivity = KafkaTracing.StartAsCurrent(KafkaTracing.ActivityFetchWait))
        {
            fetchWaitActivity?.AddMessagingAttributes(_context);
            await EnsureEnumeratorAsync(cancellationToken);
        }

        ConsumeResult<TMessage>? result;
        using (var deserializeActivity = KafkaTracing.StartAsCurrent(KafkaTracing.ActivityDeserialize))
        {
            deserializeActivity?.AddMessagingAttributes(_context);
            var deserializeSw = Stopwatch.StartNew();
            _enumerator = GetMessageEnumerator();

            result = ConsumeFromEnumerator();

            if (result is { } r)
            {
                var contextWithMessage = _context with
                {
                    TopicName = r.Topic,
                    PartitionId = r.Partition.ToString(),
                };
                KafkaMetrics.AddMessagesConsumed(contextWithMessage, 1, KafkaMetrics.OperationNamePoll);
                consumeActivity?.AddTag("nKafka.result", "message");
            }
            else
            {
                consumeActivity?.AddTag("nKafka.result", "empty");
            }

            deserializeSw.Stop();
            KafkaMetrics.RecordProcessDuration(_context, KafkaMetrics.OperationNameProcess, KafkaMetrics.OperationTypeProcess, deserializeSw.Elapsed.TotalMilliseconds);
        }

        return result;
    }

    public IConsumerBatch<TMessage> CreateBatch(CancellationToken cancellationToken)
    {
        return new ConsumerBatch<TMessage>(this);
    }

    public void SetFetchResult(FetchResult? result)
    {
        _fetchResult = result;
    }

    public void SetEnumerator(IEnumerator<MessageDeserializationContext> enumerator)
    {
        _enumerator = enumerator;
    }

    public ConsumeResult<TMessage>? ConsumeFromCurrentEnumerator()
    {
        return ConsumeFromEnumerator();
    }

    public void ResetForBatch()
    {
        _enumerator?.Dispose();
        _enumerator = null;
        _fetchResult?.Dispose();
        _fetchResult = null;
    }

    public void DisposeFetchResult()
    {
        _fetchResult?.Dispose();
        _fetchResult = null;
    }

    private async ValueTask EnsureEnumeratorAsync(CancellationToken cancellationToken)
    {
        if (_enumerator != null)
        {
            return;
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter((int)_config.FetchTimeout.TotalMilliseconds);

            try
            {
                _fetchResult = await _channel.Reader.ReadAsync(timeoutCts.Token);
            }
            catch (OperationCanceledException)
            {
                continue;
            }

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

            _enumerator = GetMessageEnumerator();
            return;
        }
    }

    private ConsumeResult<TMessage>? ConsumeFromEnumerator()
    {
        if (_enumerator == null)
        {
            return null;
        }

        if (_enumerator.MoveNext())
        {
            var ctx = _enumerator.Current;
            var message = _deserializer.Deserialize(ctx);
            return new ConsumeResult<TMessage>
            {
                Topic = ctx.Topic,
                Partition = ctx.Partition,
                Offset = ctx.Offset,
                Timestamp = ctx.Timestamp,
                Message = message,
            };
        }

        _enumerator.Dispose();
        _enumerator = null;
        _fetchResult?.Dispose();
        _fetchResult = null;

        return null;
    }

    private IEnumerator<MessageDeserializationContext> GetMessageEnumerator()
    {
        if (_fetchResult == null ||
            _fetchResult.Response?.Message?.Responses == null)
        {
            yield break;
        }

        foreach (var response in _fetchResult.Response.Message.Responses)
        {
            var topic = ResolveTopicName(response.Topic, response.TopicId);
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
        if (!string.IsNullOrEmpty(name))
        {
            return name;
        }

        // Topic ID resolution requires metadata - this is handled by the consumer
        // which passes the resolved name through the fetch result
        return null;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }
        _disposed = true;

        _enumerator?.Dispose();
        _enumerator = null;
        _fetchResult?.Dispose();
        _fetchResult = null;

        _channel.Writer.TryComplete();
    }
}
