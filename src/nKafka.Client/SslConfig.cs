namespace nKafka.Client;

public sealed record SslConfig(
    string? SaslMechanism,
    string? SaslUsername,
    string? SaslPassword,
    string? SslCaCertPath);
