// See https://aka.ms/new-console-template for more information


using BenchmarkDotNet.Running;
using nKafka.Client.Benchmarks;

var scenarios = new FetchBenchmarks().Scenarios
    .GroupBy(x => x.TopicName, (k, g) => g.First())
    .ToList();

var missing = new List<string>();
foreach (var scenario in scenarios)
{
    var exists = await TopicExists(scenario, "SASL_SSL");
    if (!exists) missing.Add(scenario.TopicName);
    exists = await TopicExists(scenario, "PLAINTEXT");
    if (!exists) missing.Add(scenario.TopicName);
}

if (missing.Count > 0)
{
    var unique = missing.Distinct().OrderBy(x => x);
    Console.WriteLine($"Missing benchmark topics: {string.Join(", ", unique)}");
    Console.WriteLine("Run 'bash infra/init-benchmark-topics.sh' to create them.");
    return 1;
}

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);

static async Task<bool> TopicExists(FetchScenario scenario, string protocol)
{
    try
    {
        await using var conn = await BenchmarkHelper.OpenConnectionAsync(protocol);
        var request = new Contracts.MessageDefinitions.MetadataRequest
        {
            FixedVersion = 12,
            Topics = [new Contracts.MessageDefinitions.MetadataRequestTopic { Name = scenario.TopicName, TopicId = Guid.Empty }],
            AllowAutoTopicCreation = false,
        };
        await using var response = await conn.SendAsync(request, CancellationToken.None);
        return response.Message.Topics?.ContainsKey(scenario.TopicName) == true;
    }
    catch
    {
        return false;
    }
}
