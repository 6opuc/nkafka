namespace nKafka.Contracts;

public enum ErrorCode : short
{
    /// <summary>
    /// Unknown broker error
    /// </summary>
    Unknown = -1,

    /// <summary>
    /// Success
    /// </summary>
    NoError = 0,

    /// <summary>
    /// Offset out of range
    /// </summary>
    OffsetOutOfRange = 1,

    /// <summary>
    /// Invalid message
    /// </summary>
    InvalidMsg = 2,

    /// <summary>
    /// Unknown topic or partition
    /// </summary>
    UnknownTopicOrPart = 3,

    /// <summary>
    /// Invalid message size
    /// </summary>
    InvalidMsgSize = 4,

    /// <summary>
    /// Leader not available
    /// </summary>
    LeaderNotAvailable = 5,

    /// <summary>
    /// Not leader for partition
    /// </summary>
    NotLeaderForPartition = 6,

    /// <summary>
    /// Request timed out
    /// </summary>
    RequestTimedOut = 7,

    /// <summary>
    /// Broker not available
    /// </summary>
    BrokerNotAvailable = 8,

    /// <summary>
    /// Replica not available
    /// </summary>
    ReplicaNotAvailable = 9,

    /// <summary>
    /// Message size too large
    /// </summary>
    MsgSizeTooLarge = 10,

    /// <summary>
    /// StaleControllerEpochCode
    /// </summary>
    StaleCtrlEpoch = 11,

    /// <summary>
    /// Offset metadata string too large
    /// </summary>
    OffsetMetadataTooLarge = 12,

    /// <summary>
    /// Broker disconnected before response received
    /// </summary>
    NetworkException = 13,

    /// <summary>
    /// Group coordinator load in progress
    /// </summary>
    GroupLoadInProgress = 14,

    /// <summary>
    /// Group coordinator not available
    /// </summary>
    GroupCoordinatorNotAvailable = 15,

    /// <summary>
    /// Not coordinator for group
    /// </summary>
    NotCoordinatorForGroup = 16,

    /// <summary>
    /// Invalid topic
    /// </summary>
    TopicException = 17,

    /// <summary>
    /// Message batch larger than configured server segment size
    /// </summary>
    RecordListTooLarge = 18,

    /// <summary>
    /// Not enough in-sync replicas
    /// </summary>
    NotEnoughReplicas = 19,

    /// <summary>
    /// Message(s) written to insufficient number of in-sync replicas
    /// </summary>
    NotEnoughReplicasAfterAppend = 20,

    /// <summary>
    /// Invalid required acks value
    /// </summary>
    InvalidRequiredAcks = 21,

    /// <summary>
    /// Specified group generation id is not valid
    /// </summary>
    IllegalGeneration = 22,

    /// <summary>
    /// Inconsistent group protocol
    /// </summary>
    InconsistentGroupProtocol = 23,

    /// <summary>
    /// Invalid group.id
    /// </summary>
    InvalidGroupId = 24,

    /// <summary>
    /// Unknown member
    /// </summary>
    UnknownMemberId = 25,

    /// <summary>
    /// Invalid session timeout
    /// </summary>
    InvalidSessionTimeout = 26,

    /// <summary>
    /// Group rebalance in progress
    /// </summary>
    RebalanceInProgress = 27,

    /// <summary>
    /// Commit offset data size is not valid
    /// </summary>
    InvalidCommitOffsetSize = 28,

    /// <summary>
    /// Topic authorization failed
    /// </summary>
    TopicAuthorizationFailed = 29,

    /// <summary>
    /// Group authorization failed
    /// </summary>
    GroupAuthorizationFailed = 30,

    /// <summary>
    /// Cluster authorization failed
    /// </summary>
    ClusterAuthorizationFailed = 31,

    /// <summary>
    /// Invalid timestamp
    /// </summary>
    InvalidTimestamp = 32,

    /// <summary>
    /// Unsupported SASL mechanism
    /// </summary>
    UnsupportedSaslMechanism = 33,

    /// <summary>
    /// Illegal SASL state
    /// </summary>
    IllegalSaslState = 34,

    /// <summary>
    /// Unsupported version
    /// </summary>
    UnsupportedVersion = 35,

    /// <summary>
    /// Topic already exists
    /// </summary>
    TopicAlreadyExists = 36,

    /// <summary>
    /// Invalid number of partitions
    /// </summary>
    InvalidPartitions = 37,

    /// <summary>
    ///    Invalid replication factor
    /// </summary>
    InvalidReplicationFactor = 38,

    /// <summary>
    /// Invalid replica assignment
    /// </summary>
    InvalidReplicaAssignment = 39,

    /// <summary>
    /// Invalid config
    /// </summary>
    InvalidConfig = 40,

    /// <summary>
    /// Not controller for cluster
    /// </summary>
    NotController = 41,

    /// <summary>
    /// Invalid request
    /// </summary>
    InvalidRequest = 42,

    /// <summary>
    /// Message format on broker does not support request
    /// </summary>
    UnsupportedForMessageFormat = 43,

    /// <summary>
    /// Isolation policy violation
    /// </summary>
    PolicyViolation = 44,

    /// <summary>
    /// Broker received an out of order sequence number
    /// </summary>
    OutOfOrderSequenceNumber = 45,

    /// <summary>
    /// Broker received a duplicate sequence number
    /// </summary>
    DuplicateSequenceNumber = 46,

    /// <summary>
    /// Producer attempted an operation with an old epoch
    /// </summary>
    InvalidProducerEpoch = 47,

    /// <summary>
    /// Producer attempted a transactional operation in an invalid state
    /// </summary>
    InvalidTxnState = 48,

    /// <summary>
    /// Producer attempted to use a producer id which is not currently assigned to its transactional id
    /// </summary>
    InvalidProducerIdMapping = 49,

    /// <summary>
    /// Transaction timeout is larger than the maximum value allowed by the broker's max.transaction.timeout.ms
    /// </summary>
    InvalidTransactionTimeout = 50,

    /// <summary>
    /// Producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing
    /// </summary>
    ConcurrentTransactions = 51,

    /// <summary>
    /// Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer
    /// </summary>
    TransactionCoordinatorFenced = 52,

    /// <summary>
    /// Transactional Id authorization failed
    /// </summary>
    TransactionalIdAuthorizationFailed = 53,

    /// <summary>
    /// Security features are disabled
    /// </summary>
    SecurityDisabled = 54,

    /// <summary>
    /// Operation not attempted
    /// </summary>
    OperationNotAttempted = 55,

    /// <summary>
    /// Disk error when trying to access log file on the disk.
    /// </summary>
    KafkaStorageError = 56,

    /// <summary>
    /// The user-specified log directory is not found in the broker config.
    /// </summary>
    LogDirNotFound = 57,

    /// <summary>
    /// SASL Authentication failed.
    /// </summary>
    SaslAuthenticationFailed = 58,

    /// <summary>
    /// Unknown Producer Id.
    /// </summary>
    UnknownProducerId = 59,

    /// <summary>
    /// Partition reassignment is in progress.
    /// </summary>
    ReassignmentInProgress = 60,

    /// <summary>
    /// Delegation Token feature is not enabled.
    /// </summary>
    DelegationTokenAuthDisabled = 61,

    /// <summary>
    /// Delegation Token is not found on server.
    /// </summary>
    DelegationTokenNotFound = 62,

    /// <summary>
    /// Specified Principal is not valid Owner/Renewer.
    /// </summary>
    DelegationTokenOwnerMismatch = 63,

    /// <summary>
    /// Delegation Token requests are not allowed on this connection.
    /// </summary>
    DelegationTokenRequestNotAllowed = 64,

    /// <summary>
    /// Delegation Token authorization failed.
    /// </summary>
    DelegationTokenAuthorizationFailed = 65,

    /// <summary>
    /// Delegation Token is expired.
    /// </summary>
    DelegationTokenExpired = 66,

    /// <summary>
    /// Supplied principalType is not supported.
    /// </summary>
    InvalidPrincipalType = 67,

    /// <summary>
    /// The group is not empty.
    /// </summary>
    NonEmptyGroup = 68,

    /// <summary>
    /// The group id does not exist.
    /// </summary>
    GroupIdNotFound = 69,

    /// <summary>
    /// The fetch session ID was not found.
    /// </summary>
    FetchSessionIdNotFound = 70,

    /// <summary>
    /// The fetch session epoch is invalid.
    /// </summary>
    InvalidFetchSessionEpoch = 71,

    /// <summary>
    /// No matching listener.
    /// </summary>
    ListenerNotFound = 72,

    /// <summary>
    /// Topic deletion is disabled.
    /// </summary>
    TopicDeletionDisabled = 73,

    /// <summary>
    /// Leader epoch is older than broker epoch.
    /// </summary>
    FencedLeaderEpoch = 74,

    /// <summary>
    /// Leader epoch is newer than broker epoch.
    /// </summary>
    UnknownLeaderEpoch = 75,

    /// <summary>
    /// Unsupported compression type.
    /// </summary>
    UnsupportedCompressionType = 76,

    /// <summary>
    /// Broker epoch has changed.
    /// </summary>
    StaleBrokerEpoch = 77,

    /// <summary>
    /// Leader high watermark is not caught up.
    /// </summary>
    OffsetNotAvailable = 78,

    /// <summary>
    /// Group member needs a valid member ID.
    /// </summary>
    MemberIdRequired = 79,

    /// <summary>
    /// Preferred leader was not available.
    /// </summary>
    PreferredLeaderNotAvailable = 80,

    /// <summary>
    /// Consumer group has reached maximum size.
    /// </summary>
    GroupMaxSizeReached = 81,

    /// <summary>
    /// Static consumer fenced by other consumer with same group.instance.id.
    /// </summary>
    FencedInstanceId = 82,

    /// <summary>
    /// Eligible partition leaders are not available.
    /// </summary>
    EligibleLeadersNotAvailable = 83,

    /// <summary>
    /// Leader election not needed for topic partition.
    /// </summary>
    ElectionNotNeeded = 84,

    /// <summary>
    /// No partition reassignment is in progress.
    /// </summary>
    NoReassignmentInProgress = 85,

    /// <summary>
    /// Deleting offsets of a topic while the consumer group is subscribed to it.
    /// </summary>
    GroupSubscribedToTopic = 86,

    /// <summary>
    /// Broker failed to validate record.
    /// </summary>
    InvalidRecord = 87,

    /// <summary>
    /// There are unstable offsets that need to be cleared.
    /// </summary>
    UnstableOffsetCommit = 88,

    /// <summary>
    /// Throttling quota has been exceeded.
    /// </summary>
    ThrottlingQuotaExceeded = 89,

    /// <summary>
    /// There is a newer producer with the same transactionalId which fences the current one.
    /// </summary>
    ProducerFenced = 90,

    /// <summary>
    /// Request illegally referred to resource that does not exist.
    /// </summary>
    ResourceNotFound = 91,

    /// <summary>
    /// Request illegally referred to the same resource twice.
    /// </summary>
    DuplicateResource = 92,

    /// <summary>
    /// Requested credential would not meet criteria for acceptability.
    /// </summary>
    UnacceptableCredential = 93,

    /// <summary>
    /// Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters.
    /// </summary>
    InconsistentVoterSet = 94,

    /// <summary>
    /// Invalid update version.
    /// </summary>
    InvalidUpdateVersion = 95,

    /// <summary>
    /// Unable to update finalized features due to server error.
    /// </summary>
    FeatureUpdateFailed = 96,

    /// <summary>
    /// Request principal deserialization failed during forwarding.
    /// </summary>
    PrincipalDeserializationFailure = 97,

    /// <summary>
    /// Unknown Topic Id.
    /// </summary>
    UnknownTopicId = 100,

    /// <summary>
    /// The member epoch is fenced by the group coordinator.
    /// </summary>
    FencedMemberEpoch = 110,

    /// <summary>
    /// The instance ID is still used by another member in the
    /// consumer group.
    /// </summary>
    UnreleasedInstanceId = 111,

    /// <summary>
    /// The assignor or its version range is not supported by
    /// the consumer group.
    /// </summary>
    UnsupportedAssignor = 112,

    /// <summary>
    /// The member epoch is stale.
    /// </summary>
    StaleMemberEpoch = 113,

    /// <summary>
    /// Client sent a push telemetry request with an invalid or outdated
    /// subscription ID.
    /// </summary>
    UnknownSubscriptionId = 117,

    /// <summary>
    /// Client sent a push telemetry request larger than the maximum size
    /// the broker will accept.
    /// </summary>
    TelemetryTooLarge = 118,
}