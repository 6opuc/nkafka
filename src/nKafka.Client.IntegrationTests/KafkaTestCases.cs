using nKafka.Contracts;
using System.Collections;

namespace nKafka.Client.IntegrationTests;

public static class KafkaTestCases
{
    private static readonly string[] Protocols = ["PLAINTEXT", "SASL_SSL"];

    public static IEnumerable GetTestCases<TRequest>() where TRequest : IRequest, new()
    {
        var request = new TRequest();
        foreach (var protocol in Protocols)
        {
            foreach (var version in request.ValidVersions)
            {
                yield return new object[] { protocol, version };
            }
        }
    }
}
