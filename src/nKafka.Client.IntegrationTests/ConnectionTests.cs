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
using System.Collections;

namespace nKafka.Client.IntegrationTests;

public class ConnectionTests
{
    private static readonly string SslCaCertPath = Path.Combine(
        TestContext.CurrentContext.TestDirectory,
        "../../../../../infra/secrets/ca-cert.pem");

    private const string SaslMechanism = "SCRAM-SHA-512";
    private const string SaslUsername = "admin";
    private const string SaslPassword = "admin-secret";
    private const string SaslBootstrapHost = "localhost";
    private const int SaslBootstrapPort = 9192;

    [SetUp]
    public void Setup()
    {
        if (!File.Exists(SslCaCertPath))
        {
            Assert.Ignore($"SASL/SSL infrastructure not available: CA cert not found at '{SslCaCertPath}'. Run 'infra/gen-certs.sh' and 'infra/init-cluster.sh' first.");
        }
    }

    public static IEnumerable OpenableProtocols
    {
        get { yield return "PLAINTEXT"; yield return "SASL_SSL"; }
    }

    public static IEnumerable VersionedApiTests
    {
        get
        {
            foreach (var protocol in OpenableProtocols)
            {
                foreach (var version in new[] { (short)0, (short)1, (short)2, (short)3, (short)4 })
                {
                    yield return new object[] { protocol, version };
                }
            }
        }
    }

    public static IEnumerable FindCoordinatorVersions
    {
        get
        {
            foreach (var protocol in OpenableProtocols)
            {
                foreach (var version in new[] { (short)0, (short)1, (short)2, (short)3, (short)4, (short)5, (short)6 })
                {
                    yield return new object[] { protocol, version };
                }
            }
        }
    }

    public static IEnumerable MetadataVersions
    {
        get
        {
            foreach (var protocol in OpenableProtocols)
            {
                foreach (var version in new[] { (short)0, (short)1, (short)2, (short)3, (short)4, (short)5, (short)6, (short)7, (short)8, (short)9, (short)10, (short)11, (short)12, (short)13 })
                {
                    yield return new object[] { protocol, version };
                }
            }
        }
    }

    public static IEnumerable JoinGroupVersions
    {
        get
        {
            foreach (var protocol in OpenableProtocols)
            {
                foreach (var version in new[] { (short)0, (short)1, (short)2, (short)3, (short)4, (short)5, (short)6, (short)7, (short)8, (short)9 })
                {
                    yield return new object[] { protocol, version };
                }
            }
        }
    }

    public static IEnumerable LeaveGroupVersions
    {
        get
        {
            foreach (var protocol in OpenableProtocols)
            {
                foreach (var version in new[] { (short)0, (short)1, (short)2, (short)3, (short)4, (short)5 })
                {
                    yield return new object[] { protocol, version };
                }
            }
        }
    }

    public static IEnumerable SyncGroupVersions
    {
        get
        {
            foreach (var protocol in OpenableProtocols)
            {
                foreach (var version in new[] { (short)0, (short)1, (short)2, (short)3, (short)4, (short)5 })
                {
                    yield return new object[] { protocol, version };
                }
            }
        }
    }

    public static IEnumerable HeartbeatVersions
    {
        get
        {
            foreach (var protocol in OpenableProtocols)
            {
                foreach (var version in new[] { (short)0, (short)1, (short)2, (short)3, (short)4 })
                {
                    yield return new object[] { protocol, version };
                }
            }
        }
    }

    public static IEnumerable FetchVersions
    {
        get
        {
            foreach (var protocol in OpenableProtocols)
            {
                foreach (var version in new[] { (short)4, (short)5, (short)6, (short)7, (short)8, (short)9, (short)10, (short)11, (short)12, (short)13, (short)14, (short)15, (short)16, (short)17, (short)18 })
                {
                    yield return new object[] { protocol, version };
                }
            }
        }
    }

    public static IEnumerable OffsetFetchVersions
    {
        get
        {
            foreach (var protocol in OpenableProtocols)
            {
                foreach (var version in new[] { (short)1, (short)2, (short)3, (short)4, (short)5, (short)6, (short)7, (short)8, (short)9, (short)10 })
                {
                    yield return new object[] { protocol, version };
                }
            }
        }
    }

    [Test]
    [TestCaseSource(nameof(OpenableProtocols))]
    public async Task ConnectAsyncAndDisposeAsyncShouldNotThrow(string protocol)
    {
        await using var connection = await OpenConnection(protocol);
    }

    [Test]
    [TestCaseSource(nameof(VersionedApiTests))]
    public async Task SendAsync_ApiVersionsRequest_ShouldReturnExpectedResult(string protocol, short apiVersion)
    {
        await using var connection = await OpenConnection(protocol);
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
    [TestCaseSource(nameof(FindCoordinatorVersions))]
    public async Task SendAsync_FindCoordinatorRequest_ShouldReturnExpectedResult(string protocol, short apiVersion)
    {
        await using var connection = await OpenConnection(protocol);
        string consumerGroupId = Guid.NewGuid().ToString();
        var request = new FindCoordinatorRequest
        {
            FixedVersion = apiVersion,
            Key = consumerGroupId,
            KeyType = 0,
            CoordinatorKeys = [consumerGroupId],
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
    [TestCaseSource(nameof(MetadataVersions))]
    public async Task SendAsync_MetadataRequest_ShouldReturnExpectedResult(string protocol, short apiVersion)
    {
        await using var connection = await OpenConnection(protocol);
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
    [TestCaseSource(nameof(JoinGroupVersions))]
    public async Task SendAsync_JoinGroupRequest_ShouldReturnExpectedResult(string protocol, short apiVersion)
    {
        string consumerGroupId = Guid.NewGuid().ToString();
        await using var connection = await OpenCoordinatorConnection(consumerGroupId, protocol);
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
            UserData = null,
            OwnedPartitions = null,
            GenerationId = -1,
            RackId = null
        };
        var request = new JoinGroupRequest
        {
            FixedVersion = apiVersion,
            GroupId = consumerGroupId,
            SessionTimeoutMs = (int)TimeSpan.FromSeconds(45).TotalMilliseconds,
            RebalanceTimeoutMs = -1,
            MemberId = string.Empty,
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
            request.MemberId = response.Message.MemberId;
            response.Dispose();

            response = await connection.SendAsync(request, CancellationToken.None);
            response.Should().NotBeNull();
        }

        return response;
    }

    [Test]
    [TestCaseSource(nameof(LeaveGroupVersions))]
    public async Task SendAsync_LeaveGroupRequest_ShouldReturnExpectedResult(string protocol, short apiVersion)
    {
        string consumerGroupId = Guid.NewGuid().ToString();
        await using var connection = await OpenCoordinatorConnection(consumerGroupId, protocol);
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
    [TestCaseSource(nameof(SyncGroupVersions))]
    public async Task SendAsync_SyncGroupRequest_ShouldReturnExpectedResult(string protocol, short apiVersion)
    {
        string consumerGroupId = Guid.NewGuid().ToString();
        await using var connection = await OpenCoordinatorConnection(consumerGroupId, protocol);
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
            UserData = null,
        };
        var request = new SyncGroupRequest
        {
            FixedVersion = apiVersion,
            GroupId = consumerGroupId,
            GenerationId = joinGroupResponse.Message.GenerationId,
            MemberId = joinGroupResponse.Message.MemberId,
            GroupInstanceId = null,
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
    [TestCaseSource(nameof(HeartbeatVersions))]
    public async Task SendAsync_HeartbeatRequest_ShouldReturnExpectedResult(string protocol, short apiVersion)
    {
        string consumerGroupId = Guid.NewGuid().ToString();
        await using var connection = await OpenCoordinatorConnection(consumerGroupId, protocol);
        using var joinGroupResponse = await JoinGroupAsync(connection, 0, consumerGroupId);
        var requestClient = new HeartbeatRequest
        {
            FixedVersion = apiVersion,
            GroupId = consumerGroupId,
            GenerationId = joinGroupResponse.Message.GenerationId,
            MemberId = joinGroupResponse.Message.MemberId,
            GroupInstanceId = null,
        };
        using var response = await connection.SendAsync(requestClient, CancellationToken.None);

        response.Should().NotBeNull();
        response.Message.ErrorCode.Should().Be(0);
    }

    [Test]
    [TestCaseSource(nameof(FetchVersions))]
    public async Task SendAsync_FetchRequest_ShouldReturnExpectedResult(string protocol, short apiVersion)
    {
        using var metadata = await RequestMetadata(protocol);
        var topicMetadata = metadata.Message.Topics!["test_p12_m1M_s4B"];
        var partitions = topicMetadata.Partitions!
            .GroupBy(x => x.LeaderId!.Value);
        foreach (var group in partitions)
        {
            var broker = metadata.Message.Brokers![group.Key];
            var config = new ConnectionConfig(
                protocol,
                broker.Host!,
                broker.Port!.Value,
                "nKafka.Client.IntegrationTests",
                Ssl: protocol == "SASL_SSL" ? new SslConfig(
                    protocol == "SASL_SSL" ? SaslMechanism : null,
                    protocol == "SASL_SSL" ? SaslUsername : null,
                    protocol == "SASL_SSL" ? SaslPassword : null,
                    protocol == "SASL_SSL" ? SslCaCertPath : null) : null,
                CheckCrcs: protocol == "SASL_SSL",
                RequestApiVersionsOnOpen: false);
            await using var connection = new Connection(config, TestLoggerFactory.Instance);
            await connection.OpenAsync(CancellationToken.None);

            foreach (var partition in group)
            {
                var request = new FetchRequest
                {
                    FixedVersion = apiVersion,
                    ClusterId = null,
                    ReplicaId = -1,
                    ReplicaState = null,
                    MaxWaitMs = 0,
                    MinBytes = 0,
                    MaxBytes = 0x7fffffff,
                    IsolationLevel = 0,
                    SessionId = 0,
                    SessionEpoch = -1,
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
                                    CurrentLeaderEpoch = -1,
                                    FetchOffset = 0,
                                    LastFetchedEpoch = -1,
                                    LogStartOffset = -1,
                                    PartitionMaxBytes = 1 * 1024 * 1024,
                                    ReplicaDirectoryId = Guid.Empty,
                                }
                            ]
                        },
                    ],
                    ForgottenTopicsData = [],
                    RackId = string.Empty,
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
    [TestCaseSource(nameof(FetchVersions))]
    public async Task SendAsync_FetchRequest_ShouldFetchAllRecords(string protocol, short apiVersion)
    {
        using var metadata = await RequestMetadata(protocol);
        var topicMetadata = metadata.Message.Topics!["test_p12_m1M_s4B"];
        var partitions = topicMetadata.Partitions!
            .GroupBy(x => x.LeaderId!.Value);
        int recordCount = 0;
        foreach (var group in partitions)
        {
            var broker = metadata.Message.Brokers![group.Key];
            var config = new ConnectionConfig(
                protocol,
                broker.Host!,
                broker.Port!.Value,
                "nKafka.Client.IntegrationTests",
                Ssl: protocol == "SASL_SSL" ? new SslConfig(
                    protocol == "SASL_SSL" ? SaslMechanism : null,
                    protocol == "SASL_SSL" ? SaslUsername : null,
                    protocol == "SASL_SSL" ? SaslPassword : null,
                    protocol == "SASL_SSL" ? SslCaCertPath : null) : null,
                CheckCrcs: protocol == "SASL_SSL",
                RequestApiVersionsOnOpen: false);
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
                        ClusterId = null,
                        ReplicaId = -1,
                        ReplicaState = null,
                        MaxWaitMs = 0,
                        MinBytes = 0,
                        MaxBytes = 0x7fffffff,
                        IsolationLevel = 0,
                        SessionId = 0,
                        SessionEpoch = -1,
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
                                        CurrentLeaderEpoch = -1,
                                        FetchOffset = offset,
                                        LastFetchedEpoch = -1,
                                        LogStartOffset = -1,
                                        PartitionMaxBytes = 1 * 1024 * 1024,
                                        ReplicaDirectoryId = Guid.Empty,
                                    }
                                ]
                            },
                        ],
                        ForgottenTopicsData = [],
                        RackId = string.Empty,
                    };
                    using var response = await connection.SendAsync(request, CancellationToken.None);

                    long lastOffset = response.Message
                        .Responses?.LastOrDefault()?
                        .Partitions?.LastOrDefault()?
                        .Records?.LastOffset ?? -1;
                    offset = lastOffset + 1;

                    int responseRecordCount = response.Message.Responses!
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
    [TestCaseSource(nameof(FetchVersions))]
    public async Task SendAsync_FetchRequestWithSeveralPartitions_ShouldFetchAllRecords(string protocol, short apiVersion)
    {
        using var metadata = await RequestMetadata(protocol);
        var topicMetadata = metadata.Message.Topics!["test_p12_m1M_s4B"];
        var partitions = topicMetadata.Partitions!
            .GroupBy(x => x.LeaderId!.Value);
        int recordCount = 0;
        foreach (var group in partitions)
        {
            var broker = metadata.Message.Brokers![group.Key];
            var config = new ConnectionConfig(
                protocol,
                broker.Host!,
                broker.Port!.Value,
                "nKafka.Client.IntegrationTests",
                10 * 512 * 1024,
                512 * 1024,
                Ssl: protocol == "SASL_SSL" ? new SslConfig(
                    protocol == "SASL_SSL" ? SaslMechanism : null,
                    protocol == "SASL_SSL" ? SaslUsername : null,
                    protocol == "SASL_SSL" ? SaslPassword : null,
                    protocol == "SASL_SSL" ? SslCaCertPath : null) : null,
                CheckCrcs: true,
                RequestApiVersionsOnOpen: false);
            await using var connection = new Connection(config, NullLoggerFactory.Instance);
            await connection.OpenAsync(CancellationToken.None);

            var request = new FetchRequest
            {
                FixedVersion = apiVersion,
                ClusterId = null,
                ReplicaId = -1,
                ReplicaState = null,
                MaxWaitMs = 0,
                MinBytes = 0,
                MaxBytes = 0x7fffffff,
                IsolationLevel = 0,
                SessionId = 0,
                SessionEpoch = -1,
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
                                    CurrentLeaderEpoch = -1,
                                    FetchOffset = 0,
                                    LastFetchedEpoch = -1,
                                    LogStartOffset = -1,
                                    PartitionMaxBytes = 1 * 1024 * 1024,
                                    ReplicaDirectoryId = Guid.Empty,
                                })
                            .ToList(),
                    },
                ],
                ForgottenTopicsData = [],
                RackId = string.Empty,
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

                        long? lastOffset = partitionResponse.Records?.LastOffset;
                        if (lastOffset != null)
                        {
                            partitionRequest.FetchOffset = lastOffset + 1;
                        }
                    }
                }

                int responseRecordCount = response.Message.Responses!
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
    [TestCaseSource(nameof(OffsetFetchVersions))]
    public async Task SendAsync_OffsetFetchRequest_ShouldReturnExpectedResult(string protocol, short apiVersion)
    {
        var metadata = await RequestMetadata(protocol);
        await using var connection = await OpenConnection(protocol);
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
                            Name = apiVersion < 10 ? topic.Name : null,
                            TopicId = apiVersion >= 10 ? topic.TopicId : null,
                            PartitionIndexes = topic.Partitions!.Select(p => p.PartitionIndex!.Value).ToArray(),
                        })
                        .ToArray()
                }
            },
            FixedVersion = apiVersion,
            Topics = metadata.Message.Topics!.Values
                .Select(topic => new OffsetFetchRequestTopic
                {
                    Name = apiVersion < 10 ? topic.Name : null,
                    PartitionIndexes = topic.Partitions!.Select(p => p.PartitionIndex!.Value).ToArray(),
                })
                .ToArray(),
        };

        var response = await connection.SendAsync(request, CancellationToken.None);

        response.Should().NotBeNull();
    }

    private async Task<Connection> OpenConnection(string protocol)
    {
        var (host, port) = protocol == "SASL_SSL"
            ? (SaslBootstrapHost, SaslBootstrapPort)
            : ("localhost", 9193);

        var config = new ConnectionConfig(
            protocol, host, port, "nKafka.Client.IntegrationTests",
            Ssl: protocol == "SASL_SSL" ? new SslConfig(
                protocol == "SASL_SSL" ? SaslMechanism : null,
                protocol == "SASL_SSL" ? SaslUsername : null,
                protocol == "SASL_SSL" ? SaslPassword : null,
                protocol == "SASL_SSL" ? SslCaCertPath : null) : null,
            CheckCrcs: protocol == "SASL_SSL",
            RequestApiVersionsOnOpen: false);
        var connection = new Connection(config, TestLoggerFactory.Instance);

        await connection.OpenAsync(CancellationToken.None);

        return connection;
    }

    private async Task<Connection> OpenCoordinatorConnection(string groupId, string protocol)
    {
        var config = new ConnectionConfig(protocol,
            protocol == "SASL_SSL" ? SaslBootstrapHost : "localhost",
            protocol == "SASL_SSL" ? SaslBootstrapPort : 9193,
            "nKafka.Client.IntegrationTests",
            Ssl: protocol == "SASL_SSL" ? new SslConfig(
                SaslMechanism, SaslUsername, SaslPassword, SslCaCertPath) : null,
            CheckCrcs: protocol == "SASL_SSL",
            RequestApiVersionsOnOpen: false);
        await using var connection = new Connection(config, TestLoggerFactory.Instance);
        await connection.OpenAsync(CancellationToken.None);

        var request = new FindCoordinatorRequest
        {
            FixedVersion = 4,
            Key = groupId,
            KeyType = 0,
            CoordinatorKeys = [groupId],
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
             protocol,
             coordinator.Host!,
             coordinator.Port!.Value,
             "nKafka.Client.IntegrationTests",
             Ssl: protocol == "SASL_SSL" ? new SslConfig(
                 SaslMechanism, SaslUsername, SaslPassword, SslCaCertPath) : null,
             CheckCrcs: protocol == "SASL_SSL",
             RequestApiVersionsOnOpen: false);
         var coordinatorConnection = new Connection(coordinatorConfig, TestLoggerFactory.Instance);
        await coordinatorConnection.OpenAsync(CancellationToken.None);

        return coordinatorConnection;
    }

    private async Task<IDisposableMessage<MetadataResponse>> RequestMetadata(string protocol)
    {
        await using var connection = await OpenConnection(protocol);
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

    [Test]
    public async Task SaslHandshake_ShouldReturnSupportedMechanisms()
    {
var config = new ConnectionConfig(
             "SASL_SSL",
             SaslBootstrapHost,
             SaslBootstrapPort,
             "nKafka.Client.IntegrationTests",
             Ssl: new SslConfig(SaslMechanism, SaslUsername, SaslPassword, SslCaCertPath),
             RequestApiVersionsOnOpen: false);

        await using var connection = new Connection(config, TestLoggerFactory.Instance);
        await connection.OpenAsync(CancellationToken.None);

        var request = new SaslHandshakeRequest
        {
            Mechanism = SaslMechanism,
        };
        using var response = await connection.SendAsync(request, CancellationToken.None);

        response.Should().NotBeNull();
        response.Message.ErrorCode.Should().Be(0);
        response.Message.Mechanisms.Should().Contain(SaslMechanism);
    }

    [Test]
    public async Task ConnectAsync_WrongPassword_ShouldFail()
    {
var config = new ConnectionConfig(
             "SASL_SSL",
             SaslBootstrapHost,
             SaslBootstrapPort,
             "nKafka.Client.IntegrationTests",
             Ssl: new SslConfig(SaslMechanism, SaslUsername, "wrong-password", SslCaCertPath),
             RequestApiVersionsOnOpen: false);

        var connection = new Connection(config, TestLoggerFactory.Instance);

        var act = async () => await connection.OpenAsync(CancellationToken.None);
        await act.Should().ThrowAsync<Exception>()
            .WithMessage("*authentication failed*");
    }

    [Test]
    public async Task ConnectAsync_UnsupportedMechanism_ShouldFail()
    {
var config = new ConnectionConfig(
             "SASL_SSL",
             SaslBootstrapHost,
             SaslBootstrapPort,
             "nKafka.Client.IntegrationTests",
             Ssl: new SslConfig("SCRAM-SHA-1", SaslUsername, SaslPassword, SslCaCertPath),
             RequestApiVersionsOnOpen: false);

        var connection = new Connection(config, TestLoggerFactory.Instance);

        var act = async () => await connection.OpenAsync(CancellationToken.None);
        await act.Should().ThrowAsync<Exception>()
            .WithMessage("*error code 33*");
    }
}
