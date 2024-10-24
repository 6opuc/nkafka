using FluentAssertions;
using nKafka.Contracts;
using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageDefinitions.ConsumerProtocolAssignmentNested;
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
        var connection = new Connection(TestLogger.Create<Connection>());
        
        await connection.OpenAsync(config, CancellationToken.None);

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
        #warning check response
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
        if (response.Coordinators == null)
        {
            response.ErrorCode.Should().Be(0);
        }
        else
        {

            response.ErrorCode.Should().BeNull();
            response.Coordinators.Should().AllSatisfy(x =>
                x.ErrorCode.Should().Be(0));
        }
        #warning check response
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
                    Name = "test",
                    TopicId = Guid.Empty,
                }
            ],
            AllowAutoTopicCreation = false,
            IncludeClusterAuthorizedOperations = true,
            IncludeTopicAuthorizedOperations = true,
        });
        
        var response = await connection.SendAsync(requestClient, CancellationToken.None);

        response.Should().NotBeNull();
        response.Topics.Should().AllSatisfy(x =>
            x.Value.ErrorCode.Should().Be(0));
#warning check response
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
                                Topics = ["test"],
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
#warning check response
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
#warning check response
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
                    "test", new TopicPartition
                    {
                        Topic = "test",
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
                                Topics = ["test"],
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
    
    private async Task<Connection> OpenCoordinatorConnection(string groupId)
    {
        var config = new ConnectionConfig("kafka-1", 9192);
        await using var connection = new Connection(TestLogger.Create<Connection>());
        await connection.OpenAsync(config, CancellationToken.None);

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
        var coordinatorConnection = new Connection(TestLogger.Create<Connection>());
        await coordinatorConnection.OpenAsync(coordinatorConfig, CancellationToken.None);

        return coordinatorConnection;
    }
}