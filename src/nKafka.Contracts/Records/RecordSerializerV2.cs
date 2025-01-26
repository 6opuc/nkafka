using System.Collections.ObjectModel;

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
                ? Memory<byte>.Empty
                : input.GetBuffer().AsMemory((int)input.Position, keyLength);
        if (keyLength > 0)
        {
            input.Position += keyLength;
        }
        var valueLength = PrimitiveSerializer.DeserializeVarInt(input);
        record.Value = valueLength == -1
            ? null
            : valueLength == 0
                ? Memory<byte>.Empty
                : input.GetBuffer().AsMemory((int)input.Position, valueLength);
        if (valueLength > 0)
        {
            input.Position += valueLength;
        }

        var headerCount = PrimitiveSerializer.DeserializeVarInt(input);
        if (headerCount == 0)
        {
            record.Headers = ReadOnlyDictionary<string, Memory<byte>?>.Empty;
        }
        else if (headerCount >= 0)
        {
            var headers = new Dictionary<string, Memory<byte>?>(headerCount);
            for (var i = 0; i < headerCount; i++)
            {
                var headerKey = PrimitiveSerializer.DeserializeVarString(input);
                var headerValueLength = PrimitiveSerializer.DeserializeVarInt(input);
                var headerValue = headerValueLength == -1
                    ? null
                    : headerValueLength == 0
                        ? Memory<byte>.Empty
                        : input.GetBuffer().AsMemory((int)input.Position, headerValueLength);
                if (headerValueLength > 0)
                {
                    input.Position += headerValueLength;
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