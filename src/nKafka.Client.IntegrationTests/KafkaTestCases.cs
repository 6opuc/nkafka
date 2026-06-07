using System.Collections;
using nKafka.Contracts;

namespace nKafka.Client.IntegrationTests;

public static class KafkaTestCases
{
    private static readonly string[] Protocols = ["PLAINTEXT", "SASL_SSL"];

    public static IEnumerable GetTestCases<TRequest>() where TRequest : IRequest, new()
    {
        var request = new TRequest();
        foreach (string protocol in Protocols)
        {
            foreach (short version in request.ValidVersions)
            {
                yield return new object[] { protocol, version };
            }
        }
    }
}
