using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using nKafka.Contracts;
using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageDefinitions.ConsumerProtocolAssignmentNested;
using nKafka.Contracts.MessageDefinitions.FetchRequestNested;
using nKafka.Contracts.MessageDefinitions.JoinGroupRequestNested;
using nKafka.Contracts.MessageDefinitions.LeaveGroupRequestNested;
using nKafka.Contracts.MessageDefinitions.MetadataRequestNested;
using nKafka.Contracts.MessageDefinitions.OffsetFetchRequestNested;
using nKafka.Contracts.MessageDefinitions.SyncGroupRequestNested;

namespace nKafka.Client.IntegrationTests;

public class ConnectionTests
{
    [SetUp]
    public void Setup()
    {
    }

    [Test]
    public async Task ConnectAsyncAndDisposeAsyncShouldNotThrow()
    {
        await using var connection = await OpenConnection();
    }

    private async Task<Connection> OpenConnection()
    {
        var config = new ConnectionConfig("PLAINTEXT", "kafka-1", 9192, "nKafka.Client.IntegrationTests")
        {
            RequestApiVersionsOnOpen = false,
        };
        var connection = new Connection(config, TestLoggerFactory.Instance);

        await connection.OpenAsync(CancellationToken.None);

        return connection;
    }

    [Test]
    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    public async Task SendAsync_ApiVersionsRequest_ShouldReturnExpectedResult(short apiVersion)
    {
        await using var connection = await OpenConnection();
        var request = new ApiVersionsRequest
        {
            FixedVersion = apiVersion,
            ClientSoftwareName = "nKafka.Client",
            ClientSoftwareVersion = "0.0.1",
        };

        using var response = await connection.SendAsync(request, CancellationToken.None);

        response.Should().NotBeNull();
        response.Message.ErrorCode.Should().Be(0);
        foreach (var apiKey in Enum.GetValues<ApiKey>())
        {
            if (apiKey is ApiKey.LeaderAndIsr or ApiKey.StopReplica or ApiKey.UpdateMetadata
                or ApiKey.ControlledShutdown)
            {
                continue;
            }

            response.Message.ApiKeys!.Should().ContainKey((short)apiKey, $"api key {apiKey} not found");
        }

        response.Message.ApiKeys.Should().AllSatisfy(x =>
        {
            x.Value.ApiKey.Should().NotBeNull();
            x.Value.ApiKey.Should().Be(x.Key);
            x.Value.MinVersion.Should().NotBeNull();
            x.Value.MaxVersion.Should().NotBeNull();
        });
    }

    [Test]
    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    [TestCase(4)]
    public async Task SendAsync_FindCoordinatorRequest_ShouldReturnExpectedResult(short apiVersion)
    {
        await using var connection = await OpenConnection();
        var consumerGroupId = Guid.NewGuid().ToString();
        var request = new FindCoordinatorRequest
        {
            FixedVersion = apiVersion,
            Key = consumerGroupId,
            KeyType = 0, // 0 = group, 1 = transaction
            CoordinatorKeys = [consumerGroupId], // for versions 4+
        };

        using var response = await connection.SendAsync(request, CancellationToken.None);

        response.Should().NotBeNull();
        if (apiVersion < 4)
        {
            response.Message.ErrorCode.Should().Be(0);
            response.Message.Coordinators.Should().BeNull();
            response.Message.Host.Should().NotBeNullOrEmpty();
            response.Message.Port.Should().NotBeNull();
            response.Message.NodeId.Should().NotBeNull();
        }
        else
        {
            response.Message.ErrorCode.Should().BeNull();
            response.Message.Coordinators.Should().AllSatisfy(x =>
            {
                x.ErrorCode.Should().Be(0);
                x.Key.Should().BeEquivalentTo(consumerGroupId);
                x.Host.Should().NotBeNullOrEmpty();
                x.Port.Should().NotBeNull();
                x.NodeId.Should().NotBeNull();
            });
        }
    }

    [Test]
    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    [TestCase(4)]
    [TestCase(5)]
    [TestCase(6)]
    [TestCase(7)]
    [TestCase(8)]
    [TestCase(9)]
    [TestCase(10)]
    [TestCase(11)]
    [TestCase(12)]
    public async Task SendAsync_MetadataRequest_ShouldReturnExpectedResult(short apiVersion)
    {
        await using var connection = await OpenConnection();
        var request = new MetadataRequest
        {
            FixedVersion = apiVersion,
            Topics =
            [
                new MetadataRequestTopic
                {
                    Name = "test_p12_m1M_s4B",
                    TopicId = Guid.Empty,
                }
            ],
            AllowAutoTopicCreation = false,
            IncludeClusterAuthorizedOperations = true,
            IncludeTopicAuthorizedOperations = true,
        };

        var response = await connection.SendAsync(request, CancellationToken.None);

        response.Should().NotBeNull();
        response.Message.Brokers.Should().NotBeNullOrEmpty();
        response.Message.Brokers.Should().AllSatisfy(x =>
        {
            x.Value.Host.Should().NotBeNullOrEmpty();
            x.Value.Port.Should().NotBeNull();
            x.Value.NodeId.Should().NotBeNull();
            x.Value.NodeId.Should().Be(x.Key);
        });
        response.Message.Topics.Should().AllSatisfy(x =>
        {
            x.Value.ErrorCode.Should().Be(0);
            x.Value.Name.Should().NotBeNullOrEmpty();
            x.Value.Partitions.Should().NotBeNullOrEmpty();

            x.Value.Partitions.Should().AllSatisfy(p =>
            {
                p.ErrorCode.Should().Be(0);
                p.PartitionIndex.Should().NotBeNull();
                p.LeaderId.Should().NotBeNull();

                response.Message.Brokers.Should().ContainKey(p.LeaderId!.Value);
            });
        });
    }

    [Test]
    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    [TestCase(4)]
    [TestCase(5)]
    [TestCase(6)]
    [TestCase(7)]
    [TestCase(8)]
    [TestCase(9)]
    public async Task SendAsync_JoinGroupRequest_ShouldReturnExpectedResult(short apiVersion)
    {
        var consumerGroupId = Guid.NewGuid().ToString();
        await using var connection = await OpenCoordinatorConnection(consumerGroupId);
        using var response = await JoinGroupAsync(connection, apiVersion, consumerGroupId);

        response.Message.ErrorCode.Should().Be(0);
        response.Message.GenerationId.Should().NotBeNull();
        response.Message.Leader.Should().NotBeNull();
        response.Message.MemberId.Should().NotBeNull();
        response.Message.Members.Should().NotBeNullOrEmpty();
        var subscriptionMember = response.Message.Members!.FirstOrDefault(x => x.MemberId == response.Message.MemberId);
        subscriptionMember.Should().NotBeNull();
        foreach (var member in response.Message.Members!)
        {
            member.Metadata.Should().NotBeNull();
            var subscription = member.Metadata;
            subscription.Should().NotBeNull();
            subscription!.GenerationId.Should().NotBeNull();
            subscription.Topics.Should().NotBeNullOrEmpty();
        }

        var subscriptionMemberMetadata = subscriptionMember!.Metadata;
        subscriptionMemberMetadata.Should().BeEquivalentTo(subscriptionMemberMetadata);
    }

    private static async Task<IDisposableMessage<JoinGroupResponse>> JoinGroupAsync(
        Connection connection, short apiVersion, string consumerGroupId)
    {
        var protocolSubscription = new ConsumerProtocolSubscription
        {
            Topics = ["test_p12_m1M_s4B"],
            UserData = null, // ???
            OwnedPartitions = null, // ???
            GenerationId = -1, // ???
            RackId = null // ???
        };
        var request = new JoinGroupRequest
        {
            FixedVersion = apiVersion,
            GroupId = consumerGroupId,
            SessionTimeoutMs = (int)TimeSpan.FromSeconds(45).TotalMilliseconds,
            RebalanceTimeoutMs = -1,
            MemberId = string.Empty, // ???
            GroupInstanceId = Guid.NewGuid().ToString(),
            ProtocolType = "consumer",
            Protocols = new Dictionary<string, JoinGroupRequestProtocol>
            {
                {
                    "nkafka-consumer", new JoinGroupRequestProtocol
                    {
                        Name = "nkafka-consumer",
                        Metadata = protocolSubscription,
                    }
                }
            },
            Reason = null
        };
        var response = await connection.SendAsync(request, CancellationToken.None);
        response.Should().NotBeNull();

        if (apiVersion == 4 && response.Message.ErrorCode == (short)ErrorCode.MemberIdRequired)
        {
            // retry with given member id
            request.MemberId = response.Message.MemberId;
            response.Dispose();

            response = await connection.SendAsync(request, CancellationToken.None);
            response.Should().NotBeNull();
        }

        return response;
    }

    [Test]
    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    [TestCase(4)]
    [TestCase(5)]
    public async Task SendAsync_LeaveGroupRequest_ShouldReturnExpectedResult(short apiVersion)
    {
        var consumerGroupId = Guid.NewGuid().ToString();
        await using var connection = await OpenCoordinatorConnection(consumerGroupId);
        using var joinGroupResponse = await JoinGroupAsync(connection, 0, consumerGroupId);
        var request = new LeaveGroupRequest
        {
            FixedVersion = apiVersion,
            GroupId = consumerGroupId,
            MemberId = joinGroupResponse.Message.MemberId,
            Members =
            [
                new MemberIdentity
                {
                    MemberId = joinGroupResponse.Message.MemberId,
                    GroupInstanceId = null,
                    Reason = "bla-bla-bla",
                }
            ],
        };
        using var response = await connection.SendAsync(request, CancellationToken.None);

        response.Should().NotBeNull();
        response.Message.ErrorCode.Should().Be(0);
    }


    [Test]
    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    [TestCase(4)]
    [TestCase(5)]
    public async Task SendAsync_SyncGroupRequest_ShouldReturnExpectedResult(short apiVersion)
    {
        var consumerGroupId = Guid.NewGuid().ToString();
        await using var connection = await OpenCoordinatorConnection(consumerGroupId);
        using var joinGroupResponse = await JoinGroupAsync(connection, 0, consumerGroupId);
        var requestedAssignment = new ConsumerProtocolAssignment
        {
            AssignedPartitions = new Dictionary<string, TopicPartition>
            {
                {
                    "test_p12_m1M_s4B", new TopicPartition
                    {
                        Topic = "test_p12_m1M_s4B",
                        Partitions = Enumerable.Range(0, 12).ToArray(),
                    }
                }
            },
            UserData = null, // ???
        };
        var request = new SyncGroupRequest
        {
            FixedVersion = apiVersion,
            GroupId = consumerGroupId,
            GenerationId = joinGroupResponse.Message.GenerationId,
            MemberId = joinGroupResponse.Message.MemberId,
            GroupInstanceId = null, // ???
            ProtocolType = "consumer",
            ProtocolName = "nkafka-consumer",
            Assignments =
            [
                new SyncGroupRequestAssignment
                {
                    MemberId = joinGroupResponse.Message.MemberId,
                    Assignment = requestedAssignment,
                }
            ],
        };
        using var response = await connection.SendAsync(request, CancellationToken.None);

        response.Should().NotBeNull();
        response.Message.ErrorCode.Should().Be(0);
        var actualAssignment = response.Message.Assignment!;
        actualAssignment.Should().BeEquivalentTo(requestedAssignment);
    }

    [Test]
    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    [TestCase(4)]
    public async Task SendAsync_HeartbeatRequest_ShouldReturnExpectedResult(short apiVersion)
    {
        var consumerGroupId = Guid.NewGuid().ToString();
        await using var connection = await OpenCoordinatorConnection(consumerGroupId);
        using var joinGroupResponse = await JoinGroupAsync(connection, 0, consumerGroupId);
        var requestClient = new HeartbeatRequest
        {
            FixedVersion = apiVersion,
            GroupId = consumerGroupId,
            GenerationId = joinGroupResponse.Message.GenerationId,
            MemberId = joinGroupResponse.Message.MemberId,
            GroupInstanceId = null, // ???
        };
        using var response = await connection.SendAsync(requestClient, CancellationToken.None);

        response.Should().NotBeNull();
        response.Message.ErrorCode.Should().Be(0);
    }

    [Test]
    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    [TestCase(4)]
    [TestCase(5)]
    [TestCase(6)]
    [TestCase(7)]
    [TestCase(8)]
    [TestCase(9)]
    [TestCase(10)]
    [TestCase(11)]
    [TestCase(12)]
    [TestCase(13)]
    public async Task SendAsync_FetchRequest_ShouldReturnExpectedResult(short apiVersion)
    {
        using var metadata = await RequestMetadata();
        var topicMetadata = metadata.Message.Topics!["test_p12_m1M_s4B"];
        var partitions = topicMetadata.Partitions!
            .GroupBy(x => x.LeaderId!.Value);
        foreach (var group in partitions)
        {
            var broker = metadata.Message.Brokers![group.Key];
            var config = new ConnectionConfig(
                "PLAINTEXT",
                broker.Host!,
                broker.Port!.Value,
                "nKafka.Client.IntegrationTests")
            {
                RequestApiVersionsOnOpen = false,
            };
            await using var connection = new Connection(config, TestLoggerFactory.Instance);
            await connection.OpenAsync(CancellationToken.None);

            foreach (var partition in group)
            {
                var request = new FetchRequest
                {
                    FixedVersion = apiVersion,
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
                            Topic = topicMetadata.Name,
                            TopicId = topicMetadata.TopicId,
                            Partitions =
                            [
                                new FetchPartition
                                {
                                    Partition = partition.PartitionIndex!.Value,
                                    CurrentLeaderEpoch = -1, // ???
                                    FetchOffset = 0, // ???
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

                response.Should().NotBeNull();
                if (apiVersion >= 7)
                {
                    response.Message.ErrorCode.Should().Be(0);
                }

                response.Message.Responses.Should().AllSatisfy(r =>
                    r.Partitions.Should().AllSatisfy(p =>
                        p.ErrorCode.Should().Be(0)));
            }
        }
    }

    [Test]
    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    [TestCase(4)]
    [TestCase(5)]
    [TestCase(6)]
    [TestCase(7)]
    [TestCase(8)]
    [TestCase(9)]
    [TestCase(10)]
    [TestCase(11)]
    [TestCase(12)]
    [TestCase(13)]
    public async Task SendAsync_FetchRequest_ShouldFetchAllRecords(short apiVersion)
    {
        using var metadata = await RequestMetadata();
        var topicMetadata = metadata.Message.Topics!["test_p12_m1M_s4B"];
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
                "nKafka.Client.IntegrationTests")
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
                        FixedVersion = apiVersion,
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

        recordCount.Should().Be(1000000);
    }

    [Test]
    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    [TestCase(4)]
    [TestCase(5)]
    [TestCase(6)]
    [TestCase(7)]
    [TestCase(8)]
    [TestCase(9)]
    [TestCase(10)]
    [TestCase(11)]
    [TestCase(12)]
    [TestCase(13)]
    public async Task SendAsync_FetchRequestWithSeveralPartitions_ShouldFetchAllRecords(short apiVersion)
    {
        using var metadata = await RequestMetadata();
        var topicMetadata = metadata.Message.Topics!["test_p12_m1M_s4B"];
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
                FixedVersion = apiVersion,
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

        recordCount.Should().Be(1000000);
    }
    
    [Test]
    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    [TestCase(4)]
    [TestCase(5)]
    [TestCase(6)]
    [TestCase(7)]
    [TestCase(8)]
    public async Task SendAsync_OffsetFetchRequest_ShouldReturnExpectedResult(short apiVersion)
    {
        var metadata = await RequestMetadata();
        await using var connection = await OpenConnection();
        var request = new OffsetFetchRequest
        {
            GroupId = Guid.NewGuid().ToString(),
            Groups = new List<OffsetFetchRequestGroup>
            {
                new()
                {
                    GroupId = Guid.NewGuid().ToString(),
                    MemberId = Guid.NewGuid().ToString(),
                    MemberEpoch = -1,
                    Topics = metadata.Message.Topics!.Values
                        .Select(topic => new OffsetFetchRequestTopics
                        {
                            Name = topic.Name,
                            PartitionIndexes = topic.Partitions!.Select(p => p.PartitionIndex!.Value).ToArray(),
                        })
                        .ToArray()
                }
            },
            FixedVersion = apiVersion,
            Topics = metadata.Message.Topics!.Values
                .Select(topic => new OffsetFetchRequestTopic
                {
                    Name = topic.Name,
                    PartitionIndexes = topic.Partitions!.Select(p => p.PartitionIndex!.Value).ToArray(),
                })
                .ToArray(),
        };

        var response = await connection.SendAsync(request, CancellationToken.None);

        response.Should().NotBeNull();
    }

    private async Task<Connection> OpenCoordinatorConnection(string groupId)
    {
        var config = new ConnectionConfig("PLAINTEXT", "kafka-1", 9192, "nKafka.Client.IntegrationTests")
        {
            RequestApiVersionsOnOpen = false,
        };
        await using var connection = new Connection(config, TestLoggerFactory.Instance);
        await connection.OpenAsync(CancellationToken.None);

        var request = new FindCoordinatorRequest
        {
            FixedVersion = 4,
            KeyType = 0, // 0 = group, 1 = transaction
            CoordinatorKeys = [groupId], // for versions 4+
        };

        using var response = await connection.SendAsync(request, CancellationToken.None);
        if (response == null)
        {
            throw new Exception("Empty response from find coordinator request.");
        }

        var coordinator = response.Message.Coordinators!.Single();
        if (coordinator.ErrorCode != 0)
        {
            throw new Exception(
                $"Non-zero error code in response from find coordinator request: {coordinator.ErrorCode}.");
        }

        var coordinatorConfig = new ConnectionConfig(
            "PLAINTEXT",
            coordinator.Host!,
            coordinator.Port!.Value,
            "nKafka.Client.IntegrationTests")
        {
            RequestApiVersionsOnOpen = false,
        };
        var coordinatorConnection = new Connection(coordinatorConfig, TestLoggerFactory.Instance);
        await coordinatorConnection.OpenAsync(CancellationToken.None);

        return coordinatorConnection;
    }

    private async Task<IDisposableMessage<MetadataResponse>> RequestMetadata()
    {
        await using var connection = await OpenConnection();
        var request = new MetadataRequest
        {
            FixedVersion = 12,
            Topics =
            [
                new MetadataRequestTopic
                {
                    Name = "test_p12_m1M_s4B",
                    TopicId = Guid.Empty,
                }
            ],
            AllowAutoTopicCreation = false,
            IncludeClusterAuthorizedOperations = true,
            IncludeTopicAuthorizedOperations = true,
        };

        var response = await connection.SendAsync(request, CancellationToken.None);
        return response;
    }
}