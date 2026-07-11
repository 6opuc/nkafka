using System.Collections.ObjectModel;
using System.Runtime.CompilerServices;

namespace nKafka.Contracts.Records;

public class RecordSerializerV2
{
    public static Record? Deserialize(ref BufferReader reader, long eof)
    {
        var start = reader.Position;
        var size = reader.ReadVarInt();
        if (size <= 0)
        {
            reader.Position = start;
            return null;
        }

        if (start + size > eof)
        {
            reader.Position = start;
            return null;
        }

        var record = new Record
        {
            Attributes = reader.ReadByte(),
            TimestampDelta = reader.ReadVarLong(),
            OffsetDelta = reader.ReadVarInt(),
        };

        var keyLength = reader.ReadVarInt();
        record.Key = keyLength == -1
            ? null
            : keyLength == 0
                ? Memory<byte>.Empty
                : reader.ReadMemory(keyLength);
        var valueLength = reader.ReadVarInt();
        record.Value = valueLength == -1
            ? null
            : valueLength == 0
                ? Memory<byte>.Empty
                : reader.ReadMemory(valueLength);

        var headerCount = reader.ReadVarInt();
        if (headerCount == 0)
        {
            record.Headers = ReadOnlyDictionary<string, Memory<byte>?>.Empty;
        }
        else if (headerCount >= 0)
        {
            var headers = new Dictionary<string, Memory<byte>?>(headerCount);
            for (var i = 0; i < headerCount; i++)
            {
                var headerKey = reader.ReadVarString();
                var headerValueLength = reader.ReadVarInt();
                var headerValue = headerValueLength == -1
                    ? null
                    : headerValueLength == 0
                        ? Memory<byte>.Empty
                        : reader.ReadMemory(headerValueLength);

                headers[headerKey!] = headerValue!;
            }

            record.Headers = headers;
        }

        return record;
    }
}
