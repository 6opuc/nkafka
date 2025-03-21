namespace nKafka.Contracts.Records;

public static class RecordsContainerSerializerV2
{
    public static void Serialize(MemoryStream output, RecordsContainer? message, ISerializationContext context)
    {
        throw new NotImplementedException();
    }

    public static RecordsContainer? Deserialize(MemoryStream input, ISerializationContext context)
    {
        var size = PrimitiveSerializer.DeserializeInt(input);
        if (size < 0)
        {
            throw new Exception($"Negative record container size: {size}.");
        }

        var start = input.Position;
        if (start + size > input.Length)
        {
            throw new Exception($"Record container expected {size} bytes but got only {input.Length - input.Position}.");
        }
        
        var message = new RecordsContainer
        {
            SizeInBytes = size,
            RecordBatches = new List<RecordBatch>(),
        };
        var endOfLastRecordBatch = start;
        while (true)
        {
            var recordBatch = RecordBatchSerializerV2.Deserialize(input, start + size, context);
            if (recordBatch == null)
            {
                // incomplete batch
                break;
            }

            endOfLastRecordBatch = input.Position;
            message.RecordBatches.Add(recordBatch);
        }
        message.RemainderInBytes = size - (int)(endOfLastRecordBatch-start);
        if (message.RemainderInBytes > 0)
        {
            input.Position = endOfLastRecordBatch + message.RemainderInBytes;
        }
        return message;
    }
}