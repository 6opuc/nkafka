using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using nKafka.Contracts;
using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageDefinitions.ConsumerProtocolAssignmentNested;
using nKafka.Contracts.MessageDefinitions.FetchRequestNested;
using nKafka.Contracts.MessageDefinitions.JoinGroupRequestNested;
using nKafka.Contracts.MessageDefinitions.LeaveGroupRequestNested;
using nKafka.Contracts.MessageDefinitions.MetadataRequestNested;
using nKafka.Contracts.MessageDefinitions.SyncGroupRequestNested;
using nKafka.Contracts.RequestClients;

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
        var config = new ConnectionConfig("kafka-1", 9192);
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
        var requestClient = new ApiVersionsRequestClient(apiVersion, new ApiVersionsRequest
        {
            ClientSoftwareName = "nKafka.Client",
            ClientSoftwareVersion = "0.0.1",
        });
        
        var response = await connection.SendAsync(requestClient, CancellationToken.None);

        response.Should().NotBeNull();
        response.ErrorCode.Should().Be(0);
        foreach (var apiKey in Enum.GetValues<ApiKey>())
        {
            if (apiKey is ApiKey.LeaderAndIsr or ApiKey.StopReplica or ApiKey.UpdateMetadata or ApiKey.ControlledShutdown)
            {
                continue;
            }
            response.ApiKeys!.Should().ContainKey((short)apiKey, $"api key {apiKey} not found");
        }

        response.ApiKeys.Should().AllSatisfy(x =>
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
        var requestClient = new FindCoordinatorRequestClient(apiVersion, new FindCoordinatorRequest
        {
            Key = consumerGroupId,
            KeyType = 0, // 0 = group, 1 = transaction
            CoordinatorKeys = [consumerGroupId], // for versions 4+
        });
        
        var response = await connection.SendAsync(requestClient, CancellationToken.None);

        response.Should().NotBeNull();
        if (apiVersion < 4)
        {
            response.ErrorCode.Should().Be(0);
            response.Coordinators.Should().BeNull();
            response.Host.Should().NotBeNullOrEmpty();
            response.Port.Should().NotBeNull();
        }
        else
        {
            response.ErrorCode.Should().BeNull();
            response.Coordinators.Should().AllSatisfy(x =>
            {
                x.ErrorCode.Should().Be(0);
                x.Key.Should().BeEquivalentTo(consumerGroupId);
                x.Host.Should().NotBeNullOrEmpty();
                x.Port.Should().NotBeNull();
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
        var requestClient = new MetadataRequestClient(apiVersion, new MetadataRequest
        {
            Topics = [
                new MetadataRequestTopic
                {
                    Name = "test_p12_m1M_s4B",
                    TopicId = Guid.Empty,
                }
            ],
            AllowAutoTopicCreation = false,
            IncludeClusterAuthorizedOperations = true,
            IncludeTopicAuthorizedOperations = true,
        });
        
        var response = await connection.SendAsync(requestClient, CancellationToken.None);

        response.Should().NotBeNull();
        response.Brokers.Should().NotBeNullOrEmpty();
        response.Brokers.Should().AllSatisfy(x =>
        {
            x.Value.Host.Should().NotBeNullOrEmpty();
            x.Value.Port.Should().NotBeNull();
            x.Value.NodeId.Should().NotBeNull();
            x.Value.NodeId.Should().Be(x.Key);
        });
        response.Topics.Should().AllSatisfy(x =>
        {
            x.Value.ErrorCode.Should().Be(0);
            x.Value.Name.Should().NotBeNullOrEmpty();
            x.Value.Partitions.Should().NotBeNullOrEmpty();

            x.Value.Partitions.Should().AllSatisfy(p =>
            {
                p.ErrorCode.Should().Be(0);
                p.PartitionIndex.Should().NotBeNull();
                p.LeaderId.Should().NotBeNull();

                response.Brokers.Should().ContainKey(p.LeaderId!.Value);
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
        var request = new JoinGroupRequest
        {
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
                        Metadata = new ConsumerProtocolSubscription
                            {
                                Topics = ["test_p12_m1M_s4B"],
                                UserData = null, // ???
                                OwnedPartitions = null, // ???
                                GenerationId = -1, // ???
                                RackId = null // ???
                            }
                            .AsMetadata(3),
#warning metadata version vs request version
                    }
                }
            },
            Reason = null
        };
        var requestClient = new JoinGroupRequestClient(apiVersion, request);
        var response = await connection.SendAsync(requestClient, CancellationToken.None);

        response.Should().NotBeNull();
        
        if (apiVersion == 4 && response.ErrorCode == (short)ErrorCode.MemberIdRequired)
        {
            response.MemberId.Should().NotBeNullOrEmpty();
            // retry with given member id
            request.MemberId = response.MemberId;
            response = await connection.SendAsync(requestClient, CancellationToken.None);
            response.Should().NotBeNull();
        }
        
        response.ErrorCode.Should().Be(0);
        response.GenerationId.Should().NotBeNull();
        response.Leader.Should().NotBeNull();
        response.MemberId.Should().NotBeNull();
        response.Members.Should().NotBeNullOrEmpty();
        response.Members.Should().Contain(x => x.MemberId == response.MemberId);
        
#warning check members metadata
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
        var joinGroupResponse = await JoinGroup(connection, consumerGroupId);
        var requestClient = new LeaveGroupRequestClient(apiVersion, new LeaveGroupRequest
        {
            GroupId = consumerGroupId,
            MemberId = joinGroupResponse.MemberId,
            Members = [
                new MemberIdentity
                {
                    MemberId = joinGroupResponse.MemberId,
                    GroupInstanceId = null,
                    Reason = "bla-bla-bla",
                }
            ],
        });
        var response = await connection.SendAsync(requestClient, CancellationToken.None);

        response.Should().NotBeNull();
        response.ErrorCode.Should().Be(0);
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
        var joinGroupResponse = await JoinGroup(connection, consumerGroupId);
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
        var requestClient = new SyncGroupRequestClient(apiVersion, new SyncGroupRequest
        {
            GroupId = consumerGroupId,
            GenerationId = joinGroupResponse.GenerationId,
            MemberId = joinGroupResponse.MemberId,
            GroupInstanceId = null, // ???
            ProtocolType = "consumer",
            ProtocolName = "nkafka-consumer",
            Assignments = [
                new SyncGroupRequestAssignment
                {
                    MemberId = joinGroupResponse.MemberId,
                    Assignment = requestedAssignment.AsMetadata(3),
                }
            ],
        });
        var response = await connection.SendAsync(requestClient, CancellationToken.None);

        response.Should().NotBeNull();
        response.ErrorCode.Should().Be(0);
        var actualAssignment = response.Assignment!.ConsumerProtocolAssignmentFromMetadata();
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
        var joinGroupResponse = await JoinGroup(connection, consumerGroupId);
        var requestClient = new HeartbeatRequestClient(apiVersion, new HeartbeatRequest
        {
            GroupId = consumerGroupId,
            GenerationId = joinGroupResponse.GenerationId,
            MemberId = joinGroupResponse.MemberId,
            GroupInstanceId = null, // ???
        });
        var response = await connection.SendAsync(requestClient, CancellationToken.None);

        response.Should().NotBeNull();
        response.ErrorCode.Should().Be(0);
    }

    private async Task<JoinGroupResponse> JoinGroup(Connection connection, string groupId)
    {
        var request = new JoinGroupRequest
        {
            GroupId = groupId,
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
                        Metadata = new ConsumerProtocolSubscription
                            {
                                Topics = ["test_p12_m1M_s4B"],
                                UserData = null, // ???
                                OwnedPartitions = null, // ???
                                GenerationId = -1, // ???
                                RackId = null // ???
                            }
                            .AsMetadata(3),
                    }
                }
            },
            Reason = null
        };
        var requestClient = new JoinGroupRequestClient(0, request);
        var response = await connection.SendAsync(requestClient, CancellationToken.None);
        if (response == null)
        {
            throw new Exception("Empty response on join request.");
        }
        if (response.ErrorCode != 0)
        {
            throw new Exception($"Non-zero error code in response on join request: {response.ErrorCode}.");
        }
        if (response.MemberId == null)
        {
            throw new Exception("Empty member id in response on join request.");
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
        var metadata = await RequestMetadata();
        var topicMetadata = metadata.Topics!["test_p12_m1M_s4B"];
        var partitions = topicMetadata.Partitions!
            .GroupBy(x => x.LeaderId!.Value);
        foreach (var group in partitions)
        {
            var broker = metadata.Brokers![group.Key];
            var config = new ConnectionConfig(broker.Host!, broker.Port!.Value);
            await using var connection = new Connection(config, TestLoggerFactory.Instance);
            await connection.OpenAsync(CancellationToken.None);

            foreach (var partition in group)
            {
                var requestClient = new FetchRequestClient(apiVersion, new FetchRequest
                {
                    ClusterId = null, // ???
                    ReplicaId = -1,
                    ReplicaState = null, // ???
                    MaxWaitMs = 0, // ???
                    MinBytes = 0, // ???
                    MaxBytes = 0x7fffffff,
                    IsolationLevel = 0, // !!!
                    SessionId = 0, // ???
                    SessionEpoch = -1, // ???
                    Topics = [
                        new FetchTopic
                        {
                            Topic = topicMetadata.Name,
                            TopicId = topicMetadata.TopicId,
                            Partitions = [
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
                });
                var response = await connection.SendAsync(requestClient, CancellationToken.None);

                response.Should().NotBeNull();
                if (apiVersion >= 7)
                {
                    response.ErrorCode.Should().Be(0);
                }
                
                response.Responses.Should().AllSatisfy(r =>
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
        var metadata = await RequestMetadata();
        var topicMetadata = metadata.Topics!["test_p12_m1M_s4B"];
        var partitions = topicMetadata.Partitions!
            .GroupBy(x => x.LeaderId!.Value);
        var recordCount = 0;
        foreach (var group in partitions)
        {
            var broker = metadata.Brokers![group.Key];
            var config = new ConnectionConfig(broker.Host!, broker.Port!.Value);
            await using var connection = new Connection(config, NullLoggerFactory.Instance);
            await connection.OpenAsync(CancellationToken.None);

            foreach (var partition in group)
            {
                long offset = 0;
                while (true)
                {
                    var requestClient = new FetchRequestClient(apiVersion, new FetchRequest
                    {
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

        recordCount.Should().Be(1000000);
    }
    
    private async Task<Connection> OpenCoordinatorConnection(string groupId)
    {
        var config = new ConnectionConfig("kafka-1", 9192);
        await using var connection = new Connection(config, TestLoggerFactory.Instance);
        await connection.OpenAsync(CancellationToken.None);

        var requestClient = new FindCoordinatorRequestClient(4, new FindCoordinatorRequest
        {
            KeyType = 0, // 0 = group, 1 = transaction
            CoordinatorKeys = [groupId], // for versions 4+
        });
        
        var response = await connection.SendAsync(requestClient, CancellationToken.None);
        if (response == null)
        {
            throw new Exception("Empty response from find coordinator request.");
        }

        var coordinator = response.Coordinators!.Single();
        if (coordinator.ErrorCode != 0)
        {
            throw new Exception($"Non-zero error code in response from find coordinator request: {coordinator.ErrorCode}.");
        }

        var coordinatorConfig = new ConnectionConfig(coordinator.Host!, coordinator.Port!.Value);
        var coordinatorConnection = new Connection(coordinatorConfig, TestLoggerFactory.Instance);
        await coordinatorConnection.OpenAsync(CancellationToken.None);

        return coordinatorConnection;
    }

    private async Task<MetadataResponse> RequestMetadata()
    {
        await using var connection = await OpenConnection();
        var requestClient = new MetadataRequestClient(12, new MetadataRequest
        {
            Topics = [
                new MetadataRequestTopic
                {
                    Name = "test_p12_m1M_s4B",
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
}