namespace nKafka.Contracts;

public class SerializationConfig
{
    public required string ClientId { get; init; }
    public bool CheckCrcs { get; init; }
}