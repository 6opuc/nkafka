using Microsoft.Extensions.Logging.Abstractions;

namespace nKafka.Client.Benchmarks;

public static class NKafkaConsumeBytesTest
{
    public static async Task Test(FetchScenario scenario, string protocol)
    {
        var consumerConfig = new ConsumerConfig(
            BenchmarkHelper.BootstrapServers(protocol),
            scenario.TopicName,
            $"testapp-{DateTime.UtcNow.Date:yyyyMMdd}-{Guid.NewGuid():N}",
            $"test-consumer-group-{Guid.NewGuid():N}",
            $"test-instance-{Guid.NewGuid():N}",
            protocol)
        {
            MaxWaitTime = TimeSpan.FromMilliseconds(100),
            SslCaCertPath = protocol == "SASL_SSL" ? BenchmarkHelper.GetCACertPath() : null,
            SaslMechanism = protocol == "SASL_SSL" ? "SCRAM-SHA-512" : null,
            SaslUsername = protocol == "SASL_SSL" ? "admin" : null,
            SaslPassword = protocol == "SASL_SSL" ? "admin-secret" : null,
        };

        Consumer<Memory<byte>?>? consumer = null;
        try
        {
            consumer = new Consumer<Memory<byte>?>(
                consumerConfig,
                new DummyBytesMessageDeserializer(),
                new DummyOffsetStorage(),
                NullLoggerFactory.Instance);
            await consumer.JoinGroupAsync(CancellationToken.None);

            int counter = 0;
            while (counter < scenario.MessageCount)
            {
                var consumeResult = await consumer.ConsumeAsync(CancellationToken.None);
                if (consumeResult?.Message == null)
                {
                    continue;
                }

                counter += 1;
            }

            await consumer.DisposeAsync();
            PrintStatistics(scenario, protocol, consumer.Statistics);
        }
        finally
        {
            if (consumer != null)
            {
                await consumer.DisposeAsync();
            }
        }
    }

    private static void PrintStatistics(FetchScenario scenario, string protocol, ConsumerStatistics stats)
    {
        Console.WriteLine();
        Console.WriteLine("=== Consumer Statistics ===");
        Console.WriteLine($"Scenario: {scenario.MessageCount} messages x {scenario.MessageSize} bytes x {scenario.PartitionCount} partitions");
        Console.WriteLine($"Protocol: {protocol}");
        Console.WriteLine();
        Console.WriteLine("--- Overall ---");
        Console.WriteLine($"Total elapsed:       {stats.TotalElapsed.TotalMilliseconds,10:F2} ms");
        Console.WriteLine($"Messages consumed:   {stats.TotalMessagesConsumed,10:N0}");
        Console.WriteLine($"Bytes received:      {stats.TotalBytesReceived,10:N0}");
        Console.WriteLine($"Messages/sec:        {stats.MessagesPerSecond,10:N0}");
        Console.WriteLine($"Bytes/sec:           {stats.BytesPerSecond / 1024 / 1024,10:F2} MB/s");
        Console.WriteLine();
        Console.WriteLine("--- Phase Breakdown ---");
        Console.WriteLine($"Connect:             {stats.ConnectTime.TotalMilliseconds,10:F2} ms");
        Console.WriteLine($"Metadata:            {stats.MetadataTime.TotalMilliseconds,10:F2} ms");
        Console.WriteLine($"Join Group:          {stats.JoinGroupTime.TotalMilliseconds,10:F2} ms");
        Console.WriteLine($"Sync Group:          {stats.SyncGroupTime.TotalMilliseconds,10:F2} ms");
        Console.WriteLine($"Fetch:               {stats.FetchTime.TotalMilliseconds,10:F2} ms");
        Console.WriteLine($"Deserialize:         {stats.DeserializeTime.TotalMilliseconds,10:F2} ms");
        Console.WriteLine($"Heartbeat:           {stats.HeartbeatTotalTime.TotalMilliseconds,10:F2} ms ({stats.HeartbeatCount} heartbeats)");
        Console.WriteLine();
        Console.WriteLine("--- Fetch Stats ---");
        Console.WriteLine($"Fetch count:         {stats.FetchCount,10:N0}");
        Console.WriteLine($"Avg fetch RTT:       {stats.AverageFetchRoundTripMs,10:F2} ms");
        Console.WriteLine($"P50 fetch RTT:       {stats.P50FetchRoundTripMs,10:F2} ms");
        Console.WriteLine($"P90 fetch RTT:       {stats.P90FetchRoundTripMs,10:F2} ms");
        Console.WriteLine($"P95 fetch RTT:       {stats.P95FetchRoundTripMs,10:F2} ms");
        Console.WriteLine($"P99 fetch RTT:       {stats.P99FetchRoundTripMs,10:F2} ms");
        Console.WriteLine();
        Console.WriteLine("--- Deserialization ---");
        Console.WriteLine($"Avg deserialize:     {stats.AverageDeserializeTimeMs,10:F4} ms/batch");
        Console.WriteLine($"P50 deserialize:     {stats.P50DeserializeTimeMs,10:F4} ms/batch");
        Console.WriteLine($"P90 deserialize:     {stats.P90DeserializeTimeMs,10:F4} ms/batch");
        Console.WriteLine($"P95 deserialize:     {stats.P95DeserializeTimeMs,10:F4} ms/batch");
        Console.WriteLine($"P99 deserialize:     {stats.P99DeserializeTimeMs,10:F4} ms/batch");
        Console.WriteLine("=========================");
        Console.WriteLine();
    }

    private class DummyBytesMessageDeserializer : IMessageDeserializer<Memory<byte>?>
    {
        public Memory<byte>? Deserialize(MessageDeserializationContext context)
        {
            if (context.Value != null &&
                context.Value.Value.Length > 0)
            {
                return context.Value;
            }

            return null;
        }
    }

    private class DummyOffsetStorage : IOffsetStorage
    {
        public ValueTask<long> GetAsync(
            IConnection connection,
            string consumerGroup,
            string topic,
            int partition,
            CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(0L);
        }

        public ValueTask SetAsync(
            IConnection connection,
            string consumerGroup,
            string topic,
            int partition,
            long offset,
            CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
        }
    }
}
