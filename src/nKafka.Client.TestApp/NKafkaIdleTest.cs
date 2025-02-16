using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using nKafka.Client.Benchmarks;
using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageDefinitions.FetchRequestNested;
using nKafka.Contracts.MessageDefinitions.ListOffsetsRequestNested;

namespace nKafka.Client.TestApp;

public static class NKafkaIdleTest
{
    public static async Task Test(FetchScenario scenario)
    {
        var loggerFactory = LoggerFactory.Create(builder => builder
            .SetMinimumLevel(LogLevel.Debug)
            .AddSimpleConsole(o => o.IncludeScopes = true));

        var consumerConfig = new ConsumerConfig(
            "PLAINTEXT://kafka-1:9192, PLAINTEXT://kafka-2:9292, PLAINTEXT://kafka-3:9392",
            scenario.TopicName,
            "test-consumer-group",
            $"testapp-{DateTime.UtcNow.Ticks}",
            "PLAINTEXT",
            "nKafka.Client.Benchmarks");

        await using var consumer = new Consumer<DummyStringMessage>(
            consumerConfig,
            new DummyStringMessageDeserializer(),
            new DummyOffsetStorage(),
            NullLoggerFactory.Instance/*loggerFactory*/);
        await consumer.JoinGroupAsync(CancellationToken.None);

        while (true)
        {
            await consumer.ConsumeAsync(CancellationToken.None);
        }
    }

    private class DummyStringMessage
    {
        public string? Value { get; set; }
    }

    private class DummyStringMessageDeserializer : IMessageDeserializer<DummyStringMessage>
    {
        public DummyStringMessage? Deserialize(MessageDeserializationContext context)
        {
            var result = new DummyStringMessage();
            if (context.Value != null &&
                context.Value.Value.Length > 0)
            {
                result.Value = Encoding.UTF8.GetString(context.Value.Value.Span);
            }

            return result;
        }
    }

    private class DummyOffsetStorage : IOffsetStorage
    {
        private ConcurrentDictionary<(string, int), long> _offsets = new();

        public async ValueTask<long> GetOffset(
            IConnection connection,
            string consumerGroup,
            string topic,
            int partition,
            CancellationToken cancellationToken)
        {
            if (!_offsets.TryGetValue((topic, partition), out var offset))
            {
                var fetchResponse = await connection.SendAsync(
                    new ListOffsetsRequest
                    {
                        ReplicaId = -1,
                        IsolationLevel = 0,
                        Topics = new List<ListOffsetsTopic>
                        {
                            new()
                            {
                                Name = topic,
                                Partitions = new List<ListOffsetsPartition>()
                                {
                                    new()
                                    {
                                        PartitionIndex = partition,
                                        CurrentLeaderEpoch = -1,
                                        Timestamp = -1,
                                        MaxNumOffsets = 1,
                                    }
                                },
                            }
                        },
                        TimeoutMs = -1,
                    },
                    cancellationToken);
                offset = fetchResponse.Message.Topics!.Single().Partitions!.Single().Offset!.Value;
                _offsets[(topic, partition)] = offset;
            }

            return offset;
        }
    }
}