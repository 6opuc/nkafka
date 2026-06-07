using System.Collections.Concurrent;

namespace nKafka.Client;

public class ConsumerStatistics
{
    private readonly ConcurrentBag<TimeSpan> _fetchRoundTrips = new();
    private readonly ConcurrentBag<TimeSpan> _deserializeTimes = new();
    private long _fetchCount;
    private long _heartbeatCount;
    private long _totalBytesReceived;
    private long _totalMessagesConsumed;
    private long _totalFetchTimeTicks;
    private long _totalDeserializeTimeTicks;
    private long _totalCommitTimeTicks;
    private long _totalHeartbeatTimeTicks;

    public TimeSpan TotalElapsed { get; set; }

    public TimeSpan ConnectTime { get; set; }

    public TimeSpan MetadataTime { get; set; }

    public TimeSpan JoinGroupTime { get; set; }

    public TimeSpan SyncGroupTime { get; set; }

    public TimeSpan FetchTime => new(_totalFetchTimeTicks);

    public TimeSpan DeserializeTime => new(_totalDeserializeTimeTicks);

    public TimeSpan CommitTime => new(_totalCommitTimeTicks);

    public TimeSpan HeartbeatTotalTime => new(_totalHeartbeatTimeTicks);

    public int FetchCount => (int)_fetchCount;

    public long HeartbeatCount => _heartbeatCount;

    public long TotalBytesReceived => _totalBytesReceived;

    public long TotalMessagesConsumed => _totalMessagesConsumed;

    public int FetchRequestCount => (int)Interlocked.Read(ref _fetchCount);

    public void SetHeartbeatCount(long count)
    {
        Interlocked.Exchange(ref _heartbeatCount, count);
    }

    public double AverageFetchRoundTripMs
    {
        get
        {
            var times = _fetchRoundTrips.ToList();
            if (times.Count == 0) return 0;
            double total = times.Sum(t => t.TotalMilliseconds);
            return total / times.Count;
        }
    }

    public double P50FetchRoundTripMs
    {
        get { return ComputePercentile(0.50); }
    }

    public double P90FetchRoundTripMs
    {
        get { return ComputePercentile(0.90); }
    }

    public double P95FetchRoundTripMs
    {
        get { return ComputePercentile(0.95); }
    }

    public double P99FetchRoundTripMs
    {
        get { return ComputePercentile(0.99); }
    }

    public double AverageDeserializeTimeMs
    {
        get
        {
            var times = _deserializeTimes.ToList();
            if (times.Count == 0) return 0;
            double total = times.Sum(t => t.TotalMilliseconds);
            return total / times.Count;
        }
    }

    public double P50DeserializeTimeMs
    {
        get { return ComputeDeserializePercentile(0.50); }
    }

    public double P90DeserializeTimeMs
    {
        get { return ComputeDeserializePercentile(0.90); }
    }

    public double P95DeserializeTimeMs
    {
        get { return ComputeDeserializePercentile(0.95); }
    }

    public double P99DeserializeTimeMs
    {
        get { return ComputeDeserializePercentile(0.99); }
    }

    public double BytesPerSecond => TotalElapsed.TotalSeconds > 0
        ? _totalBytesReceived / TotalElapsed.TotalSeconds
        : 0;

    public double MessagesPerSecond => TotalElapsed.TotalSeconds > 0
        ? _totalMessagesConsumed / TotalElapsed.TotalSeconds
        : 0;

    public void RecordFetchRoundTrip(TimeSpan elapsed)
    {
        _fetchRoundTrips.Add(elapsed);
        Interlocked.Increment(ref _fetchCount);
    }

    public void RecordDeserializeTime(TimeSpan elapsed)
    {
        _deserializeTimes.Add(elapsed);
    }

    public void IncrementHeartbeats(int count)
    {
        Interlocked.Add(ref _heartbeatCount, count);
    }

    public void AddBytesReceived(long bytes)
    {
        Interlocked.Add(ref _totalBytesReceived, bytes);
    }

    public void AddMessagesConsumed(long count)
    {
        Interlocked.Add(ref _totalMessagesConsumed, count);
    }

    public void AddFetchTime(TimeSpan time)
    {
        Interlocked.Add(ref _totalFetchTimeTicks, time.Ticks);
    }

    public void AddDeserializeTime(TimeSpan time)
    {
        Interlocked.Add(ref _totalDeserializeTimeTicks, time.Ticks);
    }

    public void AddCommitTime(TimeSpan time)
    {
        Interlocked.Add(ref _totalCommitTimeTicks, time.Ticks);
    }

    public void AddHeartbeatTime(TimeSpan time)
    {
        Interlocked.Add(ref _totalHeartbeatTimeTicks, time.Ticks);
    }

    private double ComputePercentile(double percentile)
    {
        var times = _fetchRoundTrips.ToList();
        if (times.Count == 0) return 0;
        times.Sort((a, b) => a.CompareTo(b));
        int index = (int)Math.Ceiling(percentile * times.Count) - 1;
        index = Math.Clamp(index, 0, times.Count - 1);
        return times[index].TotalMilliseconds;
    }

    private double ComputeDeserializePercentile(double percentile)
    {
        var times = _deserializeTimes.ToList();
        if (times.Count == 0) return 0;
        times.Sort((a, b) => a.CompareTo(b));
        int index = (int)Math.Ceiling(percentile * times.Count) - 1;
        index = Math.Clamp(index, 0, times.Count - 1);
        return times[index].TotalMilliseconds;
    }
}
