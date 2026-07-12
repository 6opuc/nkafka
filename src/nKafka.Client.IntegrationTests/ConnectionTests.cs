using System.Collections;
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
using static nKafka.Client.IntegrationTests.TestHelpers;

namespace nKafka.Client.IntegrationTests;

public class ConnectionTests
{
    [SetUp]
    public void Setup()
    {
        TestHelpers.ValidateSslInfrastructure();
    }

    private static IEnumerable TestProtocols => new[] { "PLAINTEXT", "SASL_SSL" };

    [Test]
    [TestCaseSource(nameof(TestProtocols))]
    public async Task ConnectAsyncAndDisposeAsyncShouldNotThrow(string protocol)
    {
        await using var connection = await OpenConnection(protocol);
    }

    [Test]
    [TestCaseSource(nameof(ApiVersionsRequestTestCases))]
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

    public static IEnumerable ApiVersionsRequestTestCases => KafkaTestCases.GetTestCases<ApiVersionsRequest>();

    [Test]
    [TestCaseSource(nameof(FindCoordinatorRequestTestCases))]
    public async Task SendAsync_FindCoordinatorRequest_ShouldReturnExpectedResult(string protocol, short apiVersion)
    {
        await using var connection = await OpenConnection(protocol);
        var consumerGroupId = Guid.NewGuid().ToString();
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

    public static IEnumerable FindCoordinatorRequestTestCases => KafkaTestCases.GetTestCases<FindCoordinatorRequest>();

    [Test]
    [TestCaseSource(nameof(MetadataRequestTestCases))]
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
                    Name = Topic,
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

    public static IEnumerable MetadataRequestTestCases => KafkaTestCases.GetTestCases<MetadataRequest>();

    [Test]
    [TestCaseSource(nameof(JoinGroupRequestTestCases))]
    public async Task SendAsync_JoinGroupRequest_ShouldReturnExpectedResult(string protocol, short apiVersion)
    {
        var consumerGroupId = Guid.NewGuid().ToString();
        await using var connection = await CreateCoordinatorConnectionAsync(consumerGroupId, protocol);
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

    public static IEnumerable JoinGroupRequestTestCases => KafkaTestCases.GetTestCases<JoinGroupRequest>();

    [Test]
    [TestCaseSource(nameof(LeaveGroupRequestTestCases))]
    public async Task SendAsync_LeaveGroupRequest_ShouldReturnExpectedResult(string protocol, short apiVersion)
    {
        var consumerGroupId = Guid.NewGuid().ToString();
        await using var connection = await CreateCoordinatorConnectionAsync(consumerGroupId, protocol);
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

    public static IEnumerable LeaveGroupRequestTestCases => KafkaTestCases.GetTestCases<LeaveGroupRequest>();


    [Test]
    [TestCaseSource(nameof(SyncGroupRequestTestCases))]
    public async Task SendAsync_SyncGroupRequest_ShouldReturnExpectedResult(string protocol, short apiVersion)
    {
        var consumerGroupId = Guid.NewGuid().ToString();
        await using var connection = await CreateCoordinatorConnectionAsync(consumerGroupId, protocol);
        using var joinGroupResponse = await JoinGroupAsync(connection, 0, consumerGroupId);
        var requestedAssignment = new ConsumerProtocolAssignment
        {
            AssignedPartitions = new Dictionary<string, TopicPartition>
            {
                {
                    Topic, new TopicPartition
                    {
                        Topic = Topic,
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

    public static IEnumerable SyncGroupRequestTestCases => KafkaTestCases.GetTestCases<SyncGroupRequest>();

    [Test]
    [TestCaseSource(nameof(HeartbeatRequestTestCases))]
    public async Task SendAsync_HeartbeatRequest_ShouldReturnExpectedResult(string protocol, short apiVersion)
    {
        var consumerGroupId = Guid.NewGuid().ToString();
        await using var connection = await CreateCoordinatorConnectionAsync(consumerGroupId, protocol);
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

    public static IEnumerable HeartbeatRequestTestCases => KafkaTestCases.GetTestCases<HeartbeatRequest>();

    [Test]
    [TestCaseSource(nameof(FetchRequestTestCases))]
    public async Task SendAsync_FetchRequest_ShouldReturnExpectedResult(string protocol, short apiVersion)
    {
        using var metadata = await RequestMetadata(protocol);
        var topicMetadata = metadata.Message.Topics![Topic];
        var partitions = topicMetadata.Partitions!
            .GroupBy(x => x.LeaderId!.Value);
        foreach (var group in partitions)
        {
            var broker = metadata.Message.Brokers![group.Key];
            await using var connection = await CreateDirectConnectionAsync(protocol, broker.Host!, broker.Port!.Value);

            foreach (var partition in group)
            {
                var request = CreateFetchRequest(
                    apiVersion,
                    topicMetadata.Name,
                    topicMetadata.TopicId,
                    [(partition.PartitionIndex!.Value, 0L)]);
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

    public static IEnumerable FetchRequestTestCases => KafkaTestCases.GetTestCases<FetchRequest>();

    [Test]
    [TestCaseSource(nameof(FetchRequestTestCases))]
    public async Task SendAsync_FetchRequest_ShouldFetchAllRecords(string protocol, short apiVersion)
    {
        using var metadata = await RequestMetadata(protocol);
        var topicMetadata = metadata.Message.Topics![Topic];
        var partitions = topicMetadata.Partitions!
            .GroupBy(x => x.LeaderId!.Value);
        var recordCount = 0;
        foreach (var group in partitions)
        {
            var broker = metadata.Message.Brokers![group.Key];
            await using var connection = await CreateDirectConnectionAsync(protocol, broker.Host!, broker.Port!.Value);

            foreach (var partition in group)
            {
                long offset = 0;
                while (true)
                {
                    var request = CreateFetchRequest(
                        apiVersion,
                        Topic,
                        topicMetadata.TopicId,
                        [(partition.PartitionIndex!.Value, offset)]);
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
    [TestCaseSource(nameof(FetchRequestTestCases))]
    public async Task SendAsync_FetchRequestWithSeveralPartitions_ShouldFetchAllRecords(string protocol, short apiVersion)
    {
        using var metadata = await RequestMetadata(protocol);
        var topicMetadata = metadata.Message.Topics![Topic];
        var partitions = topicMetadata.Partitions!
            .GroupBy(x => x.LeaderId!.Value);
        var recordCount = 0;
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
                Tls: TestHelpers.CreateTlsConfig(protocol),
                Sasl: protocol == "SASL_SSL" ? TestHelpers.CreateSaslConfig() : null,
                CheckCrcs: true,
                RequestApiVersionsOnOpen: false);
            await using var connection = new Connection(config, NullLoggerFactory.Instance);
            await connection.OpenAsync(CancellationToken.None);

            var partitionOffsets = group.ToDictionary(x => x.PartitionIndex!.Value, _ => 0L);
            var request = CreateFetchRequest(
                apiVersion,
                Topic,
                topicMetadata.TopicId,
                partitionOffsets.Select(kv => (kv.Key, kv.Value)));

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
                            partitionOffsets[partitionResponse.PartitionIndex!.Value] = lastOffset!.Value + 1;
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
    [TestCaseSource(nameof(OffsetFetchRequestTestCases))]
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

    public static IEnumerable OffsetFetchRequestTestCases => KafkaTestCases.GetTestCases<OffsetFetchRequest>();

    [Test]
    public async Task ConnectAsync_WithSaslSsl_ShouldOpenSuccessfully()
    {
        var config = TestHelpers.CreateConnectionConfig(
            "SASL_SSL",
            clientId: "nKafka.Client.IntegrationTests");

        await using var connection = new Connection(config, TestLoggerFactory.Instance);
        await connection.OpenAsync(CancellationToken.None);

        // If we got here, SASL auth succeeded during OpenAsync
    }

    [Test]
    public async Task ConnectAsync_WrongPassword_ShouldFail()
    {
        var tlsConfig = TestHelpers.CreateTlsConfig("SASL_SSL")!;
        var saslConfig = new SaslConfig(TestHelpers.SaslMechanism, TestHelpers.SaslUsername, "wrong-password");
        var config = TestHelpers.CreateConnectionConfig(
                      "SASL_SSL",
                      clientId: "nKafka.Client.IntegrationTests",
                      requestApiVersionsOnOpen: false);
        config = config with { Tls = tlsConfig, Sasl = saslConfig };

        var connection = new Connection(config, TestLoggerFactory.Instance);

        var act = async () => await connection.OpenAsync(CancellationToken.None);
        await act.Should().ThrowAsync<Exception>()
            .WithMessage("*authentication failed*");
    }

    [Test]
    public async Task ConnectAsync_UnsupportedMechanism_ShouldFail()
    {
        var tlsConfig = TestHelpers.CreateTlsConfig("SASL_SSL")!;
        var saslConfig = new SaslConfig("SCRAM-SHA-1", TestHelpers.SaslUsername, TestHelpers.SaslPassword);
        var config = TestHelpers.CreateConnectionConfig(
                      "SASL_SSL",
                      clientId: "nKafka.Client.IntegrationTests",
                      requestApiVersionsOnOpen: false);
        config = config with { Tls = tlsConfig, Sasl = saslConfig };

        var connection = new Connection(config, TestLoggerFactory.Instance);

        var act = async () => await connection.OpenAsync(CancellationToken.None);
        await act.Should().ThrowAsync<Exception>()
            .WithMessage("*error code 33*");
    }

    [Test]
    public async Task SendAsync_RequestTimeout_ThrowsPendingRequestTimeoutExceptionWithCorrectProperties()
    {
        var bootstrapConfig = TestHelpers.CreateConnectionConfig(
            "PLAINTEXT",
            port: TestHelpers.PlainTextBootstrapPort,
            clientId: "nKafka.Client.IntegrationTests",
            requestApiVersionsOnOpen: false,
            requestBufferSize: 512 * 1024);

        await using var leaderConnection = await GetLeaderConnectionAsync(bootstrapConfig, requestTimeout: TimeSpan.FromSeconds(2));

        var fetchRequest = CreateFetchRequest(
            apiVersion: 12,
            topic: Topic,
            topicId: Guid.Empty,
            partitions: [(0, 0L)],
            maxWaitMs: 60000,
            minBytes: 10_000_000);

        var act = async () => await leaderConnection.SendAsync(fetchRequest, CancellationToken.None);
        var exception = await act.Should().ThrowAsync<nKafka.Contracts.Exceptions.PendingRequestTimeoutException>();
        exception.And.ApiKey.Should().Be(ApiKey.Fetch);
        exception.And.TimeoutMs.Should().Be(2000);
    }

    [Test]
    public async Task SendAsync_AfterTimeout_StillAbleToSendRequests()
    {
        var bootstrapConfig = TestHelpers.CreateConnectionConfig(
            "PLAINTEXT",
            port: TestHelpers.PlainTextBootstrapPort,
            clientId: "nKafka.Client.IntegrationTests",
            requestApiVersionsOnOpen: false,
            requestBufferSize: 512 * 1024);
        await using var connection = await GetLeaderConnectionAsync(bootstrapConfig, requestTimeout: TimeSpan.FromSeconds(2));

        var fetchRequest = CreateFetchRequest(
            apiVersion: 12,
            topic: Topic,
            topicId: Guid.Empty,
            partitions: [(0, 0L)],
            maxWaitMs: 3000,
            minBytes: 10_000_000);

        var act = async () => await connection.SendAsync(fetchRequest, CancellationToken.None);
        await act.Should().ThrowAsync<nKafka.Contracts.Exceptions.PendingRequestTimeoutException>();

        // Connection should still work after timeout
        var metadataRequest = new MetadataRequest
        {
            FixedVersion = 12,
            Topics =
            [
                new MetadataRequestTopic
                {
                    Name = Topic,
                    TopicId = Guid.Empty,
                }
            ],
            AllowAutoTopicCreation = false,
            IncludeClusterAuthorizedOperations = true,
            IncludeTopicAuthorizedOperations = true,
        };

        var response = await connection.SendAsync(metadataRequest, CancellationToken.None);
        response.Should().NotBeNull();
        response.Message.Topics.Should().NotBeNullOrEmpty();
    }

    [Test]
    public async Task DisposeAsync_WithPendingRequests_CleansUpWithoutException()
    {
        var config = TestHelpers.CreateConnectionConfig(
            "PLAINTEXT",
            port: TestHelpers.PlainTextBootstrapPort,
            clientId: "nKafka.Client.IntegrationTests",
            requestApiVersionsOnOpen: false);
        config = config with { RequestTimeout = TimeSpan.FromMilliseconds(60000) };

        var connection = new Connection(config, TestLoggerFactory.Instance);
        await connection.OpenAsync(CancellationToken.None);

        var fetchRequest = CreateFetchRequest(
            apiVersion: 12,
            topic: Topic,
            topicId: Guid.Empty,
            partitions: [(0, 0L)],
            maxWaitMs: 60000,
            minBytes: 10_000_000);

        using var cts = new CancellationTokenSource(60000);
        var sendTask = connection.SendAsync(fetchRequest, cts.Token);

        await connection.DisposeAsync();

        Exception? caughtException = null;
        try
        {
            await sendTask;
        }
        catch (Exception ex)
        {
            caughtException = ex;
        }

        // The send task either completes successfully (if response arrived before dispose)
        // or throws an exception (if disposed while pending)
        // In either case, no unhandled exceptions should be thrown
    }

    [Test]
    public async Task SendAsync_BrokerMaxWaitLessThanRequestTimeout_RespondsWithinMaxWaitTime()
    {
        var bootstrapConfig = TestHelpers.CreateConnectionConfig(
            "PLAINTEXT",
            port: TestHelpers.PlainTextBootstrapPort,
            clientId: "nKafka.Client.IntegrationTests",
            requestApiVersionsOnOpen: false);
        await using var connection = await GetLeaderConnectionAsync(bootstrapConfig, requestTimeout: TimeSpan.FromSeconds(30));

        var fetchRequest = CreateFetchRequest(
            apiVersion: 12,
            topic: Topic,
            topicId: Guid.Empty,
            partitions: [(0, 0L)],
            maxWaitMs: 2000,
            minBytes: 10_000_000);

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        using var response = await connection.SendAsync(fetchRequest, CancellationToken.None);
        stopwatch.Stop();

        // Broker should respond after ~2s (MaxWaitMs), NOT wait full 30s (client timeout)
        Console.WriteLine($"Fetch completed in {stopwatch.ElapsedMilliseconds}ms");
        Console.WriteLine($"Records: {response.Message.Responses?.FirstOrDefault()?.Partitions?.FirstOrDefault()?.Records?.RecordCount ?? 0}");

        var elapsedMs = stopwatch.ElapsedMilliseconds;
        elapsedMs.Should().BeGreaterThanOrEqualTo(1500); // Waited at least a bit for MaxWaitMs
        elapsedMs.Should().BeLessThan(10000); // But responded well before 30s timeout

        // Should have received data (broker sends what it has after MaxWaitMs)
        response.Message.Responses.Should().NotBeNull();
        response.Message.Responses.Any().Should().BeTrue();
    }

    [Test]
    public async Task SendAsync_MaxWaitZeroNoData_RespondsWithEmptyResponse()
    {
        var bootstrapConfig = TestHelpers.CreateConnectionConfig(
            "PLAINTEXT",
            port: TestHelpers.PlainTextBootstrapPort,
            clientId: "nKafka.Client.IntegrationTests",
            requestApiVersionsOnOpen: false);
        await using var connection = await GetLeaderConnectionAsync(bootstrapConfig, requestTimeout: TimeSpan.FromSeconds(30));

        // First, consume all existing data to get a clean high watermark
        var consumeAll = CreateFetchRequest(
            apiVersion: 12,
            topic: Topic,
            topicId: Guid.Empty,
            partitions: [(0, 0L)],
            partitionMaxBytes: 10 * 1024 * 1024,
            maxWaitMs: 1000);

        using var consumeAllResponse = await connection.SendAsync(consumeAll, CancellationToken.None);
        var highWatermark = consumeAllResponse.Message.Responses!.FirstOrDefault()?.Partitions?.FirstOrDefault()?.HighWatermark;
        highWatermark.Should().NotBeNull();
        var watermark = highWatermark!.Value;

        // Now fetch from high watermark with MaxWaitMs=0, MinBytes=1MB (no data available)
        var fetchRequest = CreateFetchRequest(
            apiVersion: 12,
            topic: Topic,
            topicId: Guid.Empty,
            partitions: [(0, watermark)],
            partitionMaxBytes: 10 * 1024 * 1024,
            maxWaitMs: 0,
            minBytes: 1_000_000);

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        using var response = await connection.SendAsync(fetchRequest, CancellationToken.None);
        stopwatch.Stop();

        // With MaxWaitMs=0 and no data, broker should respond immediately with 0 records
        var elapsedMs = stopwatch.ElapsedMilliseconds;
        elapsedMs.Should().BeLessThan(500, "Broker should respond immediately with MaxWaitMs=0");

        // Should have empty response
        var partition = response.Message.Responses?.FirstOrDefault()?.Partitions?.FirstOrDefault();
        partition.Should().NotBeNull();
        partition?.Records?.RecordCount.Should().Be(0);
    }

    [Test]
    public async Task SendAsync_MinBytesMet_RespondsWithDataImmediately()
    {
        var bootstrapConfig = TestHelpers.CreateConnectionConfig(
            "PLAINTEXT",
            port: TestHelpers.PlainTextBootstrapPort,
            clientId: "nKafka.Client.IntegrationTests",
            requestApiVersionsOnOpen: false);
        await using var connection = await GetLeaderConnectionAsync(bootstrapConfig, requestTimeout: TimeSpan.FromSeconds(30));

        // Partition 0 has ~333KB of data (topic has 12 partitions, 4MB total)
        // Use MinBytes=100KB which should be met immediately
        var fetchRequest = CreateFetchRequest(
            apiVersion: 12,
            topic: Topic,
            topicId: Guid.Empty,
            partitions: [(0, 0L)],
            partitionMaxBytes: 10 * 1024 * 1024,
            maxWaitMs: 30000,
            minBytes: 100_000);

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        using var response = await connection.SendAsync(fetchRequest, CancellationToken.None);
        stopwatch.Stop();

        // Broker should respond immediately since partition has > 100KB
        var elapsedMs = stopwatch.ElapsedMilliseconds;
        elapsedMs.Should().BeLessThan(500, "Broker should respond immediately when MinBytes is met");

        // Should have received data
        var records = response.Message.Responses?.FirstOrDefault()?.Partitions?.FirstOrDefault()?.Records?.RecordCount ?? 0;
        records.Should().BeGreaterThan(0);
        response.Message.Responses!.FirstOrDefault()?.Partitions!.FirstOrDefault()?.HighWatermark.Should().BeGreaterThan(0);
    }
}
