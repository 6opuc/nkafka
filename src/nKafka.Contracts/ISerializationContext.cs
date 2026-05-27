namespace nKafka.Contracts;

public interface ISerializationContext
{
    SerializationConfig Config { get; }

    BufferWriter CreateWriter(int size = 4096);
}
