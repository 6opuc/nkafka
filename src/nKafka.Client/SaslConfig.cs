namespace nKafka.Client;

public sealed record SaslConfig(
    string? Mechanism,
    string? Username,
    string? Password);
