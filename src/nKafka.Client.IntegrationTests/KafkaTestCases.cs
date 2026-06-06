using nKafka.Contracts;
using System.Collections;

namespace nKafka.Client.IntegrationTests;

public static class KafkaTestCases
{
    public static IEnumerable ProtocolVersions =>
        new[] { "PLAINTEXT", "SASL_SSL" };

    public static IEnumerable GetTestCases(Type messageType)
    {
        if (!typeof(IRequest).IsAssignableFrom(messageType))
            throw new ArgumentException($"Type {messageType.Name} must implement IRequest", nameof(messageType));

        var request = (IRequest)Activator.CreateInstance(messageType)!;
        var versions = request.ValidVersions;

        foreach (var protocol in ProtocolVersions)
        {
            foreach (var version in versions)
            {
                yield return new object[] { protocol, version };
            }
        }
    }

    public static IEnumerable GetTestCases<TRequest>() where TRequest : IRequest, new()
    {
        var request = new TRequest();
        var versions = request.ValidVersions;

        foreach (var protocol in ProtocolVersions)
        {
            foreach (var version in versions)
            {
                yield return new object[] { protocol, version };
            }
        }
    }
}
