namespace nKafka.Contracts.Records;

public static class RecordsContainerSerializerV2
{
    public static void Serialize(ref BufferWriter writer, RecordsContainer? message, ISerializationContext context)
    {
        throw new NotImplementedException();
    }

    public static RecordsContainer? Deserialize(ref BufferReader reader, ISerializationContext context)
    {
        int size = reader.ReadInt32BigEndian();
        if (size < 0)
        {
            return null;
        }

        int start = reader.Position;
        int eof = start + size;
        int remainingBefore = reader.Remaining;
        if (size > remainingBefore)
        {
            throw new Exception($"Record container expected {size} bytes but got only {remainingBefore}.");
        }

        var message = new RecordsContainer
        {
            SizeInBytes = size,
            RecordBatches = new List<RecordBatch>(),
        };
        while (true)
        {
            var recordBatch = RecordBatchSerializerV2.Deserialize(ref reader, eof, context);
            if (recordBatch == null)
            {
                break;
            }
            message.RecordBatches.Add(recordBatch);
        }
        int remainder = eof - reader.Position;
        if (remainder > 0)
        {
            reader.Advance((int)remainder);
        }
        message.RemainderInBytes = (int)remainder;
        return message;
    }
}
