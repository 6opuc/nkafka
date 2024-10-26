namespace nKafka.Contracts.Records;

public static class RecordBatchSerializerV2
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
            input.Position = start;
            return null;
        }

        recordBatch.PartitionLeaderEpoch = PrimitiveSerializer.DeserializeInt(input);
        recordBatch.Magic = PrimitiveSerializer.DeserializeByte(input);
        if (recordBatch.Magic != 2)
        {
            throw new Exception($"Version 2 was expected, but received version {recordBatch.Magic}.");
        }
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
                var record = RecordSerializerV2.Deserialize(input);
                if (record != null)
                {
                    recordBatch.Records.Add(record);
                }
            }
        }

#warning validate actual crc
#warning validate actual batch length

        if (input.Position != recordBatchStart + recordBatch.BatchLength)
        {
            input.Position = recordBatchStart + recordBatch.BatchLength;
        }

        return recordBatch;
    }
}