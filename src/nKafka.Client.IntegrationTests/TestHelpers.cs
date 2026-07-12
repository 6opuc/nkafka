using FluentAssertions;
using nKafka.Client;
using nKafka.Contracts;
using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageDefinitions.ConsumerProtocolAssignmentNested;
using nKafka.Contracts.MessageDefinitions.FetchRequestNested;
using nKafka.Contracts.MessageDefinitions.JoinGroupRequestNested;
using nKafka.Contracts.MessageDefinitions.MetadataRequestNested;

namespace nKafka.Client.IntegrationTests;

public static class TestHelpers
{
    public const string SaslMechanism = "SCRAM-SHA-512";
    public const string SaslUsername = "admin";
    public const string SaslPassword = "admin-secret";
    public const string BootstrapHost = "localhost";
    public const int SaslBootstrapPort = 9192;
    public const int PlainTextBootstrapPort = 9193;
    public const string Topic = "test_p12_m1M_s4B";

    private static readonly string SslCaCertPath = Path.Combine(
        TestContext.CurrentContext.TestDirectory,
        "../../../../../infra/secrets/ca-cert.pem");

    public static TlsConfig? CreateTlsConfig(string protocol)
    {
        if (protocol != "SASL_SSL")
        {
            return null;
        }

        return new TlsConfig(SslCaCertPath);
    }

    public static SaslConfig? CreateSaslConfig()
    {
        return new SaslConfig(SaslMechanism, SaslUsername, SaslPassword);
    }

    public static void ValidateSslInfrastructure()
    {
        if (!File.Exists(SslCaCertPath))
        {
            Assert.Ignore($"SASL/SSL infrastructure not available: CA cert not found at '{SslCaCertPath}'. Run 'infra/gen-certs.sh' and 'infra/init-cluster.sh' first.");
        }
    }

    public static ConnectionConfig CreateConnectionConfig(
        string protocol,
        int? port = null,
        string clientId = "nKafka.Client.IntegrationTests",
        bool checkCrcs = false,
        int? responseBufferSize = null,
        int? requestBufferSize = null,
        bool requestApiVersionsOnOpen = false)
    {
        return new ConnectionConfig(
               protocol,
               BootstrapHost,
               port ?? (protocol == "SASL_SSL" ? SaslBootstrapPort : PlainTextBootstrapPort),
               clientId!,
               responseBufferSize ?? 512 * 1024,
               requestBufferSize ?? 512 * 1024,
               CreateTlsConfig(protocol),
               protocol == "SASL_SSL" ? CreateSaslConfig() : null,
               checkCrcs || protocol == "SASL_SSL",
               requestApiVersionsOnOpen);
    }

    public static ConsumerConfig CreateConsumerConfig(
        string clientId,
        string groupId,
        string instanceId,
        string protocol,
        string topics = Topic,
        TimeSpan? maxWaitTime = null,
        bool checkCrcs = false)
    {
        var servers = protocol == "SASL_SSL"
            ? $"SASL_SSL://{BootstrapHost}:{SaslBootstrapPort}"
            : $"PLAINTEXT://{BootstrapHost}:{PlainTextBootstrapPort}";

        var config = new ConsumerConfig(
            servers,
            topics,
            clientId,
            groupId,
            instanceId);

        config = config with
        {
            CheckCrcs = checkCrcs || protocol == "SASL_SSL",
            MaxWaitTime = maxWaitTime ?? TimeSpan.FromSeconds(1),
            Tls = CreateTlsConfig(protocol),
            Sasl = protocol == "SASL_SSL" ? CreateSaslConfig() : null,
        };

        return config;
    }

    public static async Task<Connection> CreateConnectionAsync(
        ConnectionConfig config,
        CancellationToken ct = default)
    {
        var connection = new Connection(config, TestLoggerFactory.Instance);
        await connection.OpenAsync(ct);
        return connection;
    }

    public static async Task<Connection> CreateBootstrapConnectionAsync(
        string protocol,
        CancellationToken ct = default)
    {
        var config = CreateConnectionConfig(protocol);
        return await CreateConnectionAsync(config, ct);
    }

    public static async Task<(string host, int port, Guid? topicId, IReadOnlyDictionary<int, int> partitionLeaders)>
        GetTopicMetadataAsync(Connection connection, string topicName, CancellationToken ct = default)
    {
        var metadataRequest = new MetadataRequest
        {
            FixedVersion = 12,
            Topics =
            [
                new MetadataRequestTopic
                {
                    Name = topicName,
                    TopicId = Guid.Empty,
                }
            ],
            AllowAutoTopicCreation = false,
            IncludeClusterAuthorizedOperations = true,
            IncludeTopicAuthorizedOperations = true,
        };

        using var metadataResponse = await connection.SendAsync(metadataRequest, ct);
        var topicMetadata = metadataResponse.Message.Topics![topicName];
        var partitionLeaders = new Dictionary<int, int>();
        foreach (var partition in topicMetadata.Partitions!)
        {
            partitionLeaders[partition.PartitionIndex!.Value] = partition.LeaderId!.Value;
        }

        return (
            host: metadataResponse.Message.Brokers![partitionLeaders.Values.First()].Host!,
            port: metadataResponse.Message.Brokers![partitionLeaders.Values.First()].Port!.Value,
            topicId: topicMetadata.TopicId,
            partitionLeaders: partitionLeaders.AsReadOnly());
    }

    public static async Task<Connection> CreateDirectConnectionAsync(
        string protocol,
        string host,
        int port,
        CancellationToken ct = default)
    {
        var config = new ConnectionConfig(
            protocol,
            host,
            port,
            "nKafka.Client.IntegrationTests")
        {
            RequestApiVersionsOnOpen = false,
        };
        return await CreateConnectionAsync(config, ct);
    }

    public static FetchRequest CreateFetchRequest(
        short apiVersion,
        string topic,
        Guid? topicId,
        IEnumerable<(int partition, long fetchOffset)> partitions,
        int partitionMaxBytes = 1_048_576,
        int maxWaitMs = 0,
        int minBytes = 0)
    {
        return new FetchRequest
        {
            FixedVersion = apiVersion,
            ClusterId = null,
            ReplicaId = -1,
            ReplicaState = null,
            MaxWaitMs = maxWaitMs,
            MinBytes = minBytes,
            MaxBytes = 0x7fffffff,
            IsolationLevel = 0,
            SessionId = 0,
            SessionEpoch = -1,
            Topics =
            [
                new FetchTopic
                {
                    Topic = topic,
                    TopicId = topicId,
                    Partitions =
                    [
                        ..partitions.Select(p => new FetchPartition
                        {
                            Partition = p.partition,
                            CurrentLeaderEpoch = -1,
                            FetchOffset = p.fetchOffset,
                            LastFetchedEpoch = -1,
                            LogStartOffset = -1,
                            PartitionMaxBytes = partitionMaxBytes,
                            ReplicaDirectoryId = Guid.Empty,
                        })
                    ]
                },
            ],
            ForgottenTopicsData = [],
            RackId = string.Empty,
        };
    }

    public static async Task<Connection> CreateCoordinatorConnectionAsync(
        string groupId,
        string protocol,
        CancellationToken ct = default)
    {
        var config = CreateConnectionConfig(
            protocol,
            clientId: "nKafka.Client.IntegrationTests",
            checkCrcs: protocol == "SASL_SSL",
            requestApiVersionsOnOpen: false);
        await using var connection = new Connection(config, TestLoggerFactory.Instance);
        await connection.OpenAsync(ct);

        var request = new FindCoordinatorRequest
        {
            FixedVersion = 4,
            Key = groupId,
            KeyType = 0,
            CoordinatorKeys = [groupId],
        };

        using var response = await connection.SendAsync(request, ct);
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
            Tls: CreateTlsConfig(protocol),
            Sasl: protocol == "SASL_SSL" ? CreateSaslConfig() : null,
            CheckCrcs: protocol == "SASL_SSL",
            RequestApiVersionsOnOpen: false);
        var coordinatorConnection = new Connection(coordinatorConfig, TestLoggerFactory.Instance);
        await coordinatorConnection.OpenAsync(ct);

        return coordinatorConnection;
    }

    public static async Task<IDisposableMessage<JoinGroupResponse>> JoinGroupAsync(
        Connection connection,
        short apiVersion,
        string consumerGroupId,
        CancellationToken ct = default)
    {
        var protocolSubscription = new ConsumerProtocolSubscription
        {
            Topics = [Topic],
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
        var response = await connection.SendAsync(request, ct);
        response.Should().NotBeNull();

        if (apiVersion == 4 && response.Message.ErrorCode == (short)ErrorCode.MemberIdRequired)
        {
            request.MemberId = response.Message.MemberId;
            response.Dispose();

            response = await connection.SendAsync(request, ct);
            response.Should().NotBeNull();
        }

        return response;
    }

    public static async Task<Connection> OpenConnection(string protocol, CancellationToken ct = default)
    {
        var config = CreateConnectionConfig(
            protocol,
            clientId: "nKafka.Client.IntegrationTests",
            checkCrcs: protocol == "SASL_SSL",
            requestApiVersionsOnOpen: false);
        return await CreateConnectionAsync(config, ct);
    }

    public static async Task<Connection> GetLeaderConnectionAsync(
        ConnectionConfig bootstrapConfig,
        ConnectionConfig? leaderConfig = null,
        TimeSpan? requestTimeout = null,
        CancellationToken ct = default)
    {
        await using var bootstrapConnection = new Connection(bootstrapConfig, TestLoggerFactory.Instance);
        await bootstrapConnection.OpenAsync(ct);

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

        using var metadataResponse = await bootstrapConnection.SendAsync(metadataRequest, ct);
        var topicMetadata = metadataResponse.Message.Topics![Topic];
        var partition0 = topicMetadata.Partitions!.FirstOrDefault(p => p.PartitionIndex == 0);
        if (partition0 == null)
        {
            throw new Exception("Partition 0 not found in metadata response");
        }

        var leaderId = partition0.LeaderId!.Value;
        var leader = metadataResponse.Message.Brokers![leaderId];

        ConnectionConfig effectiveLeaderConfig;
        if (leaderConfig != null)
        {
            effectiveLeaderConfig = leaderConfig with
            {
                Host = leader.Host!,
                Port = leader.Port!.Value
            };
        }
        else
        {
            effectiveLeaderConfig = new ConnectionConfig(
                bootstrapConfig.Protocol,
                leader.Host!,
                leader.Port!.Value,
                "nKafka.Client.IntegrationTests");
        }
        if (requestTimeout.HasValue)
        {
            effectiveLeaderConfig = effectiveLeaderConfig with { RequestTimeout = requestTimeout.Value };
        }
        var leaderConnection = new Connection(effectiveLeaderConfig, TestLoggerFactory.Instance);
        await leaderConnection.OpenAsync(ct);

        return leaderConnection;
    }

    public static async Task<IDisposableMessage<MetadataResponse>> RequestMetadata(
        string protocol,
        CancellationToken ct = default)
    {
        await using var connection = await CreateBootstrapConnectionAsync(protocol, ct);
        var request = new MetadataRequest
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

        var response = await connection.SendAsync(request, ct);
        return response;
    }
}
