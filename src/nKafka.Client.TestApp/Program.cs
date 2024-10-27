﻿// See https://aka.ms/new-console-template for more information

using Microsoft.Extensions.Logging.Abstractions;
using nKafka.Client;
using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageDefinitions.FetchRequestNested;
using nKafka.Contracts.MessageDefinitions.MetadataRequestNested;
using nKafka.Contracts.RequestClients;

var metadata = await RequestMetadata();
var topicMetadata = metadata.Topics!["test"];
var partitions = topicMetadata.Partitions!
    .GroupBy(x => x.LeaderId!.Value);
var recordCount = 0;
foreach (var group in partitions)
{
    var broker = metadata.Brokers![group.Key];
    var config = new ConnectionConfig(broker.Host!, broker.Port!.Value);
    await using var connection = new Connection(NullLogger<Connection>.Instance);
    await connection.OpenAsync(config, CancellationToken.None);

    foreach (var partition in group)
    {
        long offset = 0;
        while (true)
        {
            var requestClient = new FetchRequestClient(13, new FetchRequest
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
                        Topic = "test",
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
            });
            var response = await connection.SendAsync(requestClient, CancellationToken.None);

            var lastOffset = response
                .Responses?.LastOrDefault()?
                .Partitions?.LastOrDefault()?
                .Records?.LastOffset ?? -1;
            offset = lastOffset + 1;

            var responseRecordCount = response.Responses!
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


static async Task<MetadataResponse> RequestMetadata()
{
    var config = new ConnectionConfig("kafka-1", 9192);
    var connection = new Connection(NullLogger<Connection>.Instance);

    await connection.OpenAsync(config, CancellationToken.None);

    var requestClient = new MetadataRequestClient(12, new MetadataRequest
    {
        Topics =
        [
            new MetadataRequestTopic
            {
                Name = "test",
                TopicId = Guid.Empty,
            }
        ],
        AllowAutoTopicCreation = false,
        IncludeClusterAuthorizedOperations = true,
        IncludeTopicAuthorizedOperations = true,
    });

    var response = await connection.SendAsync(requestClient, CancellationToken.None);
    return response;
}