using Microsoft.Extensions.Logging.Abstractions;
using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageDefinitions.FetchRequestNested;
using nKafka.Contracts.MessageDefinitions.MetadataRequestNested;

namespace nKafka.Client.Benchmarks;

public static class NKafkaBytesFetchSeqMultiPartTest
{
    public static async Task Test(FetchScenario scenario)
    {
        using var metadata = await RequestMetadata(scenario);
        var topicMetadata = metadata.Message.Topics![scenario.TopicName];
        var partitions = topicMetadata.Partitions!
            .GroupBy(x => x.LeaderId!.Value);
        var recordCount = 0;
        foreach (var group in partitions)
        {
            var broker = metadata.Message.Brokers![group.Key];
            var config = new ConnectionConfig(
                "PLAINTEXT",
                broker.Host!,
                broker.Port!.Value,
                "nKafka.Client.IntegrationTests",
                10 * 512 * 1024)
            {
                RequestApiVersionsOnOpen = false,
                CheckCrcs = true,
            };
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
                MaxBytes = 0x7fffffff,
                IsolationLevel = 0, // !!!
                SessionId = 0, // ???
                SessionEpoch = -1, // ???
                Topics =
                [
                    new FetchTopic
                    {
                        Topic = "test_p12_m1M_s4B",
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
                                    PartitionMaxBytes = 512 * 1024, // !!!
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

                        var lastOffset = partitionResponse.Records?.LastOffset;
                        if (lastOffset != null)
                        {
                            partitionRequest.FetchOffset = lastOffset + 1;
                        }
                    }
                }

                var responseRecordCount = response.Message.Responses!
                    .SelectMany(x => x.Partitions!)
                    .Sum(x => x.Records!.RecordCount);
                if (responseRecordCount == 0)
                {
                    break;
                }

                recordCount += responseRecordCount;
            }
        }

        Console.WriteLine(recordCount);
    }

    private static async Task<IDisposableMessage<MetadataResponse>> RequestMetadata(FetchScenario scenario)
    {
        var config = new ConnectionConfig("PLAINTEXT", "kafka-1", 9192, "nKafka.Client.Benchmarks")
        {
            RequestApiVersionsOnOpen = false,
        };
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