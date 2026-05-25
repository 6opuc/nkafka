namespace nKafka.Contracts;

public interface ISerializationContext
{
    SerializationConfig Config { get; }
    
    PooledBuffer CreateBuffer();
}
