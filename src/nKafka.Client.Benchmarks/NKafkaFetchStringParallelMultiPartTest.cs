using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageDefinitions.FetchRequestNested;
using nKafka.Contracts.MessageDefinitions.MetadataRequestNested;

namespace nKafka.Client.Benchmarks;

public static class NKafkaFetchStringParallelMultiPartTest
{
    public static async Task Test(FetchScenario scenario, string protocol)
    {
        using var metadata = await RequestMetadata(scenario, protocol);
        var topicMetadata = metadata.Message.Topics![scenario.TopicName];
        var partitions = topicMetadata.Partitions!
            .GroupBy(x => x.LeaderId!.Value);
        int recordCount = 0;
        var tasks = new List<Task>();
        foreach (var group in partitions)
        {
            var task = Task.Run(async () =>
            {
                var broker = metadata.Message.Brokers![group.Key];
                var config = BenchmarkHelper.CreateConnectionConfig(
                    broker.Host!, broker.Port!.Value, protocol,
                    BenchmarkHelper.ResponseBufferSize,
                    BenchmarkHelper.ResponseBufferSize);
                await using var connection = new Connection(config, NullLoggerFactory.Instance);
                await connection.OpenAsync(CancellationToken.None);

                var request = new FetchRequest
                {
                    FixedVersion = 13,
                    ClusterId = null, // ???
                    ReplicaId = -1,
                    ReplicaState = null, // ???
                    MaxWaitMs = 0, // ???
                    MinBytes = 0, // ???
                    MaxBytes = BenchmarkHelper.FetchMaxBytes,
                    IsolationLevel = 0, // !!!
                    SessionId = 0, // ???
                    SessionEpoch = -1, // ???
                    Topics =
                    [
                        new FetchTopic
                        {
                            Topic = scenario.TopicName,
                            TopicId = topicMetadata.TopicId,
                            Partitions = group
                                .Select(x =>
                                    new FetchPartition
                                    {
                                        Partition = x.PartitionIndex!.Value,
                                        CurrentLeaderEpoch = -1, // ???
                                        FetchOffset = 0, // ???
                                        LastFetchedEpoch = -1, // ???
                                        LogStartOffset = -1, // ???
                                        PartitionMaxBytes = BenchmarkHelper.PartitionMaxBytes,
                                        ReplicaDirectoryId = Guid.Empty, // ???
                                    })
                                .ToList(),
                        },
                    ],
                    ForgottenTopicsData = [], // ???
                    RackId = string.Empty, // ???
                };

                while (true)
                {
                    using var response = await connection.SendAsync(request, CancellationToken.None);
                    int responseRecordCount = 0;
                    foreach (var topicResponse in response.Message.Responses!)
                    {
                        var topicRequest = request.Topics.FirstOrDefault(x =>
                            x.Topic == topicResponse.Topic ||
                            x.TopicId == topicResponse.TopicId);
                        if (topicRequest == null)
                        {
                            continue;
                        }

                        foreach (var partitionResponse in topicResponse.Partitions!)
                        {
                            var partitionRequest = topicRequest.Partitions!
                                .FirstOrDefault(x => x.Partition == partitionResponse.PartitionIndex);
                            if (partitionRequest == null)
                            {
                                continue;
                            }

                            long? lastOffset = partitionResponse.Records?.LastOffset;
                            if (lastOffset != null)
                            {
                                partitionRequest.FetchOffset = lastOffset + 1;
                            }

                            foreach (var recordBatch in partitionResponse.Records!.RecordBatches!)
                            {
                                foreach (var record in recordBatch.Records!)
                                {
                                    if (record.Value != null &&
                                        record.Value.Value.Length > 0)
                                    {
                                        string value = Encoding.UTF8.GetString(record.Value.Value.Span);
                                        if (!string.IsNullOrEmpty(value))
                                        {
                                            responseRecordCount += 1;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if (responseRecordCount == 0)
                    {
                        break;
                    }

                    recordCount += responseRecordCount;
                }
            });
            tasks.Add(task);
        }

        await Task.WhenAll(tasks);
    }

    private static async Task<IDisposableMessage<MetadataResponse>> RequestMetadata(FetchScenario scenario, string protocol)
    {
        var config = BenchmarkHelper.CreateConnectionConfig("localhost",
            BenchmarkHelper.BootstrapPort(protocol), protocol);
        await using var connection = new Connection(config, NullLoggerFactory.Instance);

        await connection.OpenAsync(CancellationToken.None);

        var requestClient = new MetadataRequest
        {
            FixedVersion = 12,
            Topics =
            [
                new MetadataRequestTopic
                {
                    Name = scenario.TopicName,
                    TopicId = Guid.Empty,
                }
            ],
            AllowAutoTopicCreation = false,
            IncludeClusterAuthorizedOperations = true,
            IncludeTopicAuthorizedOperations = true,
        };

        var response = await connection.SendAsync(requestClient, CancellationToken.None);
        return response;
    }
}
