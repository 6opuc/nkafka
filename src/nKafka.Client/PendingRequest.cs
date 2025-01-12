using System.Diagnostics;
using nKafka.Contracts;
using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageSerializers;

namespace nKafka.Client;

public class PendingRequest
{
    public IRequest Payload { get; init; }
    public TaskCompletionSource<MessageWithPooledPayload> Response { get; init; }
    public CancellationToken CancellationToken { get; init; }
    public int CorrelationId { get; init; }
    public short ApiVersion { get; init; }
    
    // TODO: remove
    private Stopwatch _queueStopwatch;
    private Stopwatch _sendStopwatch;
    private Stopwatch _responseStopwatch;
    private static TimeSpan _totalElapsedOnQueue = TimeSpan.Zero; 
    private static TimeSpan _totalElapsedSerialization = TimeSpan.Zero;
    private static TimeSpan _totalElapsedDeserialization = TimeSpan.Zero;
    private static TimeSpan _totalElapsedSending = TimeSpan.Zero;
    private static TimeSpan _totalElapsedResponse = TimeSpan.Zero;
        

    public PendingRequest(
        IRequest payload, 
        TaskCompletionSource<MessageWithPooledPayload> response,
        CancellationToken cancellationToken,
        int correlationId,
        short apiVersion)
    {
        Payload = payload;
        Response = response;
        CancellationToken = cancellationToken;
        CorrelationId = correlationId;
        ApiVersion = payload.FixedVersion ?? apiVersion;
        _queueStopwatch = Stopwatch.StartNew();
        _sendStopwatch = Stopwatch.StartNew();
    }

    // TODO: remove
    public void OnDequeued()
    {
        _queueStopwatch.Stop();
        _totalElapsedOnQueue += _queueStopwatch.Elapsed;
    }

    public void OnWrittenToSocket()
    {
        _sendStopwatch.Stop();
        _totalElapsedSending += _sendStopwatch.Elapsed;
        _responseStopwatch = Stopwatch.StartNew();
    }

    public static void PrintTotalElapsedTimeAndReset()
    {
        Console.WriteLine("Total send queue time: {0}ms", _totalElapsedOnQueue.TotalMilliseconds);
        Console.WriteLine("Total serialization time: {0}ms", _totalElapsedSerialization.TotalMilliseconds);
        Console.WriteLine("Total deserialization time: {0}ms", _totalElapsedDeserialization.TotalMilliseconds);
        Console.WriteLine("Total sending time: {0}ms", _totalElapsedSending.TotalMilliseconds);
        Console.WriteLine("Total response time: {0}ms", _totalElapsedResponse.TotalMilliseconds);
        _totalElapsedOnQueue = TimeSpan.Zero;
        _totalElapsedSerialization = TimeSpan.Zero;
        _totalElapsedDeserialization = TimeSpan.Zero;
        _totalElapsedSending = TimeSpan.Zero;
        _totalElapsedResponse = TimeSpan.Zero;
    }
    
    public void SerializeRequest(MemoryStream output, ISerializationContext context)
    {
        var stopwatch = Stopwatch.StartNew();
        var header = new RequestHeader
        {
            RequestApiKey = (short)Payload.ApiKey,
            RequestApiVersion = ApiVersion,
            CorrelationId = CorrelationId,
            ClientId = context.Config.ClientId,
        };
        PrimitiveSerializer.SerializeInt(output, 0); // placeholder for header + payload
        var start = output.Position;
        var requestHeaderVersion = Payload.ApiKey == ApiKey.ControlledShutdown && ApiVersion == 0
            ? (short)0
            : (short)(Payload.FlexibleVersions.Includes(ApiVersion) ? 2 : 1);
        RequestHeaderSerializer.Serialize(output, header, requestHeaderVersion, context);
        Payload.SerializeRequest(output, ApiVersion, context);
        var end = output.Position;
        var size = (int)(end - start);
        output.Position = start - 4;
        PrimitiveSerializer.SerializeInt(output, size);
        output.Position = end;
        stopwatch.Stop();
        _totalElapsedSerialization += stopwatch.Elapsed;
    }
    
    public object DeserializeResponse(MemoryStream input, ISerializationContext context)
    {
        #warning why _responseStopwatch is null????
        if (_responseStopwatch != null)
        {
            _responseStopwatch.Stop();
            _totalElapsedResponse += _responseStopwatch.Elapsed;
        }

        var stopwatch = Stopwatch.StartNew();
        var responseHeaderVersion = Payload.ApiKey == ApiKey.ApiVersions
            ? (short)0
            : (short)(Payload.FlexibleVersions.Includes(ApiVersion) ? 1 : 0);
        var header = ResponseHeaderSerializer.Deserialize(input, responseHeaderVersion, context);
        if (header.CorrelationId == null)
        {
            throw new InvalidOperationException("Received response with empty correlation id.");
        }

        if (header.CorrelationId != CorrelationId)
        {
            throw new InvalidOperationException(
                $"Received response with incorrect correlation id. Expected {CorrelationId}, but got {header.CorrelationId}.");
        }

        var result = Payload.DeserializeResponse(input, ApiVersion, context);
        stopwatch.Stop();
        _totalElapsedDeserialization += stopwatch.Elapsed;

        return result;
    }
}