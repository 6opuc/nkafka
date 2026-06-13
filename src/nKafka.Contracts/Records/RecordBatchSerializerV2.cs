using nKafka.Contracts.Exceptions;

namespace nKafka.Contracts.Records;

public static class RecordBatchSerializerV2
{
    public static RecordBatch? Deserialize(ref BufferReader reader, long eof, ISerializationContext context)
    {
        if (reader.Position + 8 + 4 > eof)
        {
            return null;
        }

        var recordBatch = new RecordBatch
        {
            BaseOffset = reader.ReadInt64BigEndian(),
            BatchLength = reader.ReadInt32BigEndian(),
        };

        var recordBatchStart = reader.Position;

        if (recordBatchStart + recordBatch.BatchLength > eof)
        {
            return null;
        }

        recordBatch.PartitionLeaderEpoch = reader.ReadInt32BigEndian();
        recordBatch.Magic = reader.ReadByte();
        if (recordBatch.Magic != 2)
        {
            throw new ProtocolException($"Version 2 was expected, but received version {recordBatch.Magic}.");
        }
        recordBatch.Crc = reader.ReadUInt32BigEndian();

        var crcStart = reader.Position;
        recordBatch.Attributes = reader.ReadInt16BigEndian();
        recordBatch.LastOffsetDelta = reader.ReadInt32BigEndian();
        recordBatch.FirstTimestamp = reader.ReadInt64BigEndian();
        recordBatch.MaxTimestamp = reader.ReadInt64BigEndian();
        recordBatch.ProducerId = reader.ReadInt64BigEndian();
        recordBatch.ProducerEpoch = reader.ReadInt16BigEndian();
        recordBatch.BaseSequence = reader.ReadInt32BigEndian();
        var recordsCount = reader.ReadInt32BigEndian();
        if (recordsCount >= 0)
        {
            recordBatch.Records = new List<Record>(recordsCount);
            for (var i = 0; i < recordsCount; i++)
            {
                var record = RecordSerializerV2.Deserialize(ref reader, recordBatchStart + recordBatch.BatchLength);
                if (record == null)
                {
                    break;
                }
                recordBatch.Records.Add(record);
            }
        }

        if (context.Config.CheckCrcs)
        {
            long crcDataLength = reader.Position - crcStart;
            ChecksumValidator.ValidateCrc32c(recordBatch.Crc, reader.Buffer, (int)crcStart, (int)crcDataLength);
        }

        var actualBatchLength = reader.Position - recordBatchStart;
        if (actualBatchLength != recordBatch.BatchLength)
        {
            throw new DeserializationException($"Expected batch length was {recordBatch.BatchLength}, but got {actualBatchLength}.");
        }

        return recordBatch;
    }
}
