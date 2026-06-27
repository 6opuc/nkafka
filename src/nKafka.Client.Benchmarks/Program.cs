// See https://aka.ms/new-console-template for more information


using BenchmarkDotNet.Running;
using nKafka.Client.Benchmarks;
using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageDefinitions.MetadataRequestNested;

var scenarios = new FetchBenchmarks().Scenarios
    .GroupBy(x => x.TopicName, (k, g) => g.First())
    .ToList();

var missing = new List<string>();
foreach (var scenario in scenarios)
{
    var existsSsl = await TopicExists(scenario, "SASL_SSL");
    var existsPlaintext = await TopicExists(scenario, "PLAINTEXT");
    if (!existsSsl && !existsPlaintext)
    {
        missing.Add(scenario.TopicName);
    }
}

if (missing.Count > 0)
{
    var unique = missing.Distinct().OrderBy(x => x);
    Console.WriteLine($"Missing benchmark topics: {string.Join(", ", unique)}");
    Console.WriteLine("Run 'bash infra/init-benchmark-topics.sh' to create them.");
    Environment.Exit(1);
}

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);

static async Task<bool> TopicExists(FetchScenario scenario, string protocol)
{
    try
    {
        var conn = await BenchmarkHelper.OpenConnectionAsync(protocol);
        var request = new MetadataRequest
        {
            FixedVersion = 12,
            Topics = [new MetadataRequestTopic { Name = scenario.TopicName, TopicId = Guid.Empty }],
            AllowAutoTopicCreation = false,
        };
        using var response = await conn.SendAsync(request, CancellationToken.None);
        return response.Message.Topics?.ContainsKey(scenario.TopicName) == true;
    }
    catch
    {
        return false;
    }
}
