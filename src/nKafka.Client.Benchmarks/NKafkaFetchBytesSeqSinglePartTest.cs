using Microsoft.Extensions.Logging.Abstractions;
using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageDefinitions.FetchRequestNested;
using nKafka.Contracts.MessageDefinitions.MetadataRequestNested;

namespace nKafka.Client.Benchmarks;

public static class NKafkaFetchBytesSeqSinglePartTest
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
            var config = new ConnectionConfig("PLAINTEXT", broker.Host!, broker.Port!.Value, "nKafka.Client.Benchmarks")
            {
                RequestApiVersionsOnOpen = false,
            };
            await using var connection = new Connection(config, NullLoggerFactory.Instance);
            await connection.OpenAsync(CancellationToken.None);

            foreach (var partition in group)
            {
                long offset = 0;
                while (true)
                {
                    var request = new FetchRequest
                    {
                        FixedVersion = 13,
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
                                Topic = topicMetadata.Name,
                                TopicId = topicMetadata.TopicId,
                                Partitions =
                                [
                                    new FetchPartition
                                    {
                                        Partition = partition.PartitionIndex!.Value,
                                        CurrentLeaderEpoch = -1, // ???
                                        FetchOffset = offset, // ???
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
                    using var response = await connection.SendAsync(request, CancellationToken.None);

                    var lastOffset = response.Message
                        .Responses?.LastOrDefault()?
                        .Partitions?.LastOrDefault()?
                        .Records?.LastOffset ?? -1;
                    offset = lastOffset + 1;

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