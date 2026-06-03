using nKafka.Contracts;
using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageDefinitions.FetchRequestNested;

namespace nKafka.Client;

public class FetchSessionManager
{
    private int _sessionId;
    private int _sessionIndex;
    private bool _useSessions;
    private readonly List<FetchTopic> _currentTopics;

    public FetchSessionManager(int initialSessionId, int initialSessionIndex, bool useSessions)
    {
        _sessionId = initialSessionId;
        _sessionIndex = initialSessionIndex;
        _useSessions = useSessions;
        _currentTopics = new List<FetchTopic>();
    }

    public int SessionId => _sessionId;
    public int SessionIndex => _useSessions ? _sessionIndex : 0;
    public bool IsActive => _sessionId != 0 && _useSessions;

    public void InitializeTopics(IEnumerable<FetchTopic> topics)
    {
        _currentTopics.Clear();
        _currentTopics.AddRange(topics);
    }

    public FetchRequest CreateInitialRequest(int maxWaitMs, int minBytes, int maxBytes, IEnumerable<FetchTopic> topics)
    {
        var request = new FetchRequest
        {
            ClusterId = null,
            ReplicaId = -1,
            ReplicaState = null,
            MaxWaitMs = maxWaitMs,
            MinBytes = minBytes,
            MaxBytes = maxBytes,
            IsolationLevel = 0,
            SessionId = 0,
            SessionEpoch = -1,
            Topics = topics.ToList(),
            ForgottenTopicsData = Array.Empty<ForgottenTopic>(),
            RackId = string.Empty,
        };

        if (_useSessions)
        {
            _sessionId = 0;
            _sessionIndex = 0;
            _currentTopics.Clear();
            _currentTopics.AddRange(topics);
        }

        return request;
    }

    public FetchRequest CreateSubsequentRequest(int maxWaitMs, int minBytes, int maxBytes)
    {
        var request = new FetchRequest
        {
            ClusterId = null,
            ReplicaId = -1,
            ReplicaState = null,
            MaxWaitMs = maxWaitMs,
            MinBytes = minBytes,
            MaxBytes = maxBytes,
            IsolationLevel = 0,
            SessionId = _sessionId,
            SessionEpoch = _sessionIndex,
            Topics = Array.Empty<FetchTopic>(),
            ForgottenTopicsData = Array.Empty<ForgottenTopic>(),
            RackId = string.Empty,
        };

        if (_useSessions)
        {
            _sessionIndex++;
        }

        return request;
    }

    public void HandleResponse(FetchResponse response)
    {
        if (response.SessionId == null) return;

        if (response.SessionId == 0)
        {
            _sessionId = 0;
            _sessionIndex = 0;
            _useSessions = false;
        }
        else
        {
            _sessionId = response.SessionId.Value;
        }
    }

    public void OnError(short errorCode)
    {
        if (errorCode == (short)ErrorCode.FetchSessionIdNotFound ||
            errorCode == (short)ErrorCode.InvalidFetchSessionEpoch)
        {
            _sessionId = 0;
            _sessionIndex = 0;
            _useSessions = false;
        }
    }

    public void AddPartitions(IEnumerable<FetchTopic> topics)
    {
        if (!_useSessions) return;

        foreach (var topic in topics)
        {
            var existingTopic = _currentTopics.FirstOrDefault(t => t.Topic == topic.Topic || t.TopicId == topic.TopicId);
            if (existingTopic == null)
            {
                _currentTopics.Add(new FetchTopic
                {
                    Topic = topic.Topic,
                    TopicId = topic.TopicId,
                    Partitions = topic.Partitions?.ToList() ?? new List<FetchPartition>(),
                });
            }
            else
            {
                foreach (var partition in topic.Partitions ?? Array.Empty<FetchPartition>())
                {
                    var existingPartition = existingTopic.Partitions?.FirstOrDefault(p => p.Partition == partition.Partition);
                    if (existingPartition == null)
                    {
                        existingTopic.Partitions ??= new List<FetchPartition>();
                        existingTopic.Partitions.Add(partition);
                    }
                }
            }
        }
    }

    public void RemovePartitions(IEnumerable<FetchTopic> topics)
    {
        if (!_useSessions) return;

        foreach (var topicToRemove in topics)
        {
            var topicIndex = _currentTopics.FindIndex(t => t.Topic == topicToRemove.Topic || t.TopicId == topicToRemove.TopicId);
            if (topicIndex >= 0)
            {
                _currentTopics.RemoveAt(topicIndex);
            }
        }
    }
}
