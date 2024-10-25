namespace nKafka.Contracts.Records;

public static class RecordBatchContainerSerializer
{
    public static void Serialize(MemoryStream output, RecordBatchContainer? message)
    {
        throw new NotImplementedException();
    }

    public static RecordBatchContainer? Deserialize(MemoryStream input)
    {
        var size = PrimitiveSerializer.DeserializeInt(input);
        if (size < 0)
        {
            return null;
        }

        var start = input.Position;
        if (start + size > input.Length)
        {
            throw new Exception($"RecordBatchContainer expected {size} bytes but got only {input.Length - input.Position}.");
        }
        
        var message = new RecordBatchContainer
        {
            SizeInBytes = size,
            #warning decide on capacity
            RecordBatches = new List<RecordBatch>(),
        };
        var endOfLastRecordBatch = start;
        while (true)
        {
            var recordBatch = RecordBatchSerializer.Deserialize(input);
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
    
    public static void SerializeFlexible(MemoryStream output, RecordBatchContainer? message)
    {
        throw new NotImplementedException();
    }
    
    public static RecordBatchContainer? DeserializeFlexible(MemoryStream input)
    {
        throw new NotImplementedException();
        /*
        var size = PrimitiveSerializer.DeserializeLength(input);
        if (size < 0)
        {
            return null;
        }

        var message = new RecordBatchContainer
        {
            SizeInBytes = size,
            Payload = new byte[size],
        };
        input.Read(message.Payload, 0, size);
        return message;*/
    }
}