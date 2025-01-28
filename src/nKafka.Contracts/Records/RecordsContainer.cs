namespace nKafka.Contracts.Records;

public class RecordsContainer
{
    public int SizeInBytes { get; set; }
    public int RemainderInBytes { get; set; }
    public IList<RecordBatch>? RecordBatches { get; set; }
    public IList<Message>? Messages { get; set; }

    public long? LastOffset
    {
        get
        {
            if (RecordBatches != null)
            {
                var lastBatch = RecordBatches.LastOrDefault();
                if (lastBatch == null)
                {
                    return null;
                }
                return lastBatch.BaseOffset + lastBatch.LastOffsetDelta;
            }

            if (Messages != null)
            {
                var lastMessage = Messages.LastOrDefault();
                if (lastMessage == null)
                {
                    return null;
                }

                return lastMessage.Offset;
            }

            return null;
        }
    }
    
    public int RecordCount => RecordBatches?.Sum(b => b.Records?.Count ?? 0) ?? Messages?.Count ?? 0;
}