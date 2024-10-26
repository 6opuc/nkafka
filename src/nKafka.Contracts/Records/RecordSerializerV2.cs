namespace nKafka.Contracts.Records;

public class RecordSerializerV2
{
    public static Record? Deserialize(MemoryStream input)
    {
        var size = PrimitiveSerializer.DeserializeVarInt(input);
        if (size <= 0)
        {
            return null;
        }

        var start = input.Position;
        if (start + size > input.Length)
        {
            return null;
        }
        
        var record = new Record
        {
            Attributes = PrimitiveSerializer.DeserializeByte(input),
            TimestampDelta = PrimitiveSerializer.DeserializeVarLong(input),
            OffsetDelta = PrimitiveSerializer.DeserializeVarInt(input),
        };

        var keyLength = PrimitiveSerializer.DeserializeVarInt(input);
        record.Key = keyLength == -1
            ? null
            : keyLength == 0
                ? Array.Empty<byte>()
                : new byte[keyLength];
        if (keyLength > 0)
        {
            input.Read(record.Key!, 0, keyLength);
        }
        var valueLength = PrimitiveSerializer.DeserializeVarInt(input);
        record.Value = valueLength == -1
            ? null
            : valueLength == 0
                ? Array.Empty<byte>()
                : new byte[valueLength];
        if (valueLength > 0)
        {
            input.Read(record.Value!, 0, valueLength);
        }

        var headerCount = PrimitiveSerializer.DeserializeVarInt(input);
        if (headerCount >= 0)
        {
            var headers = new Dictionary<string, byte[]>(headerCount);
            for (var i = 0; i < headerCount; i++)
            {
                var headerKey = PrimitiveSerializer.DeserializeVarString(input);
                var headerValueLength = PrimitiveSerializer.DeserializeVarInt(input);
                var headerValue = headerValueLength == -1
                    ? null
                    : headerValueLength == 0
                        ? Array.Empty<byte>()
                        : new byte[headerValueLength];
                if (headerValueLength > 0)
                {
                    input.Read(headerValue!, 0, headerValueLength);
                }

                headers[headerKey!] = headerValue!;
            }

            record.Headers = headers;
        }

        if (start + size != input.Position)
        {
            input.Position = start + size;
        }

        return record;
    }
}