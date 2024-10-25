namespace nKafka.Contracts.Records;

public static class RecordBatchSerializer
{
    public static RecordBatch? Deserialize(MemoryStream input)
    {
        var start = input.Position;
        if (start + 8 + 4 > input.Length)
        {
            // we will not be able to read batch size
            return null;
        }

        var recordBatch = new RecordBatch
        {
            BaseOffset = PrimitiveSerializer.DeserializeLong(input),
            BatchLength = PrimitiveSerializer.DeserializeInt(input),
        };
        
        var recordBatchStart = input.Position;

        if (input.Position + recordBatch.BatchLength > input.Length)
        {
            // we will not be able to read full batch
#warning try to read as much records as possible
            input.Position = start;
            return null;
        }

        recordBatch.PartitionLeaderEpoch = PrimitiveSerializer.DeserializeInt(input);
        recordBatch.Magic = PrimitiveSerializer.DeserializeByte(input);
        recordBatch.Crc = PrimitiveSerializer.DeserializeUint(input);

#warning check crc from this position

        recordBatch.Attributes = PrimitiveSerializer.DeserializeShort(input);
        recordBatch.LastOffsetDelta = PrimitiveSerializer.DeserializeInt(input);
        recordBatch.FirstTimestamp = PrimitiveSerializer.DeserializeLong(input);
        recordBatch.MaxTimestamp = PrimitiveSerializer.DeserializeLong(input);
        recordBatch.ProducerId = PrimitiveSerializer.DeserializeLong(input);
        recordBatch.ProducerEpoch = PrimitiveSerializer.DeserializeShort(input);
        recordBatch.BaseSequence = PrimitiveSerializer.DeserializeInt(input);
        var recordsCount = PrimitiveSerializer.DeserializeInt(input);
        if (recordsCount >= 0)
        {
            recordBatch.Records = new List<Record>(recordsCount);
            for (int i = 0; i < recordsCount; i++)
            {
                var record = RecordSerializer.Deserialize(input);
                if (record != null)
                {
                    recordBatch.Records.Add(record);
                }
            }
        }

#warning validate actual crc
#warning validate actual batch length
        
        input.Position = recordBatchStart + recordBatch.BatchLength;

        return recordBatch;
    }
}