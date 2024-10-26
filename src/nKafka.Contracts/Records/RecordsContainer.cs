namespace nKafka.Contracts.Records;

public class RecordsContainer
{
    public int SizeInBytes { get; set; }
    public int RemainderInBytes { get; set; }
    public IList<RecordBatch>? RecordBatches { get; set; }
    public IList<Message>? Messages { get; set; }

    public long LastOffset
    {
        get
        {
            if (RecordBatches != null)
            {
                var lastBatch = RecordBatches.LastOrDefault();
                if (lastBatch == null)
                {
                    return -1;
                }
                return lastBatch.BaseOffset + lastBatch.LastOffsetDelta;
            }

            if (Messages != null)
            {
                var lastMessage = Messages.LastOrDefault();
                if (lastMessage == null)
                {
                    return -1;
                }

                return lastMessage.Offset;
            }

            return -1;
        }
    }
    
    public int RecordCount => RecordBatches?.Sum(b => b.Records?.Count ?? 0) ?? Messages?.Count ?? 0;
}