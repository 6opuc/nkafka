namespace nKafka.Client.Benchmarks;

public class FetchScenario
{
    public int PartitionCount { get; set; }
    public int MessageSize { get; set; }
    public int MessageCount { get; set; }
    
    public string TopicName => $"test_p{PartitionCount}_m{CountToString(MessageCount)}_s{BytesToString(MessageSize)}";

    public override string ToString()
    {
        return $"{PartitionCount}p {CountToString(MessageCount)}x{BytesToString(MessageSize)}";
    }
    
    static String BytesToString(int byteCount)
    {
        string[] suf = { "B", "KB", "MB", "GB" };
        if (byteCount == 0)
            return "0" + suf[0];
        long bytes = Math.Abs(byteCount);
        int place = Convert.ToInt32(Math.Floor(Math.Log(bytes, 1024)));
        double num = Math.Round(bytes / Math.Pow(1024, place), 1);
        return (Math.Sign(byteCount) * num) + suf[place];
    }
    
    static String CountToString(int count)
    {
        string[] suf = { "K", "M", "G" };
        if (count == 0)
            return "0";
        long bytes = Math.Abs(count);
        int place = Convert.ToInt32(Math.Floor(Math.Log(bytes, 1000)));
        double num = Math.Round(bytes / Math.Pow(1000, place), 1);
        return (Math.Sign(count) * num) + suf[place-1];
    }
}