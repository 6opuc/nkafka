using nKafka.Contracts;
using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageDefinitions.FetchRequestNested;

namespace nKafka.Client;

public class FetchSessionManager
{
    private int _sessionId;
    private int _sessionIndex;
    private bool _useSessions;

    public FetchSessionManager(int initialSessionId, int initialSessionIndex, bool useSessions)
    {
        _sessionId = initialSessionId;
        _sessionIndex = initialSessionIndex;
        _useSessions = useSessions;
    }

    public int SessionId => _sessionId;
    public int SessionIndex => _useSessions ? _sessionIndex : 0;
    public bool IsActive => _sessionId != 0 && _useSessions;

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

    public void OnResponseReceived(FetchResponse response)
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

    // TODO: Implement incremental fetch session modification for partition reassignment.
    // Currently, the consumer restarts fetch loops via StartFetchingAsync on rebalance,
    // so there's no need to dynamically add/remove topics from the session.
    // If incremental session updates are needed in the future, implement:
    //   - AddTopicsToSession(IEnumerable<FetchTopic> topics)
    //   - RemoveTopicsFromSession(IEnumerable<FetchTopic> topics)
}
