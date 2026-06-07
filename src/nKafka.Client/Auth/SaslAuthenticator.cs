using System.Buffers;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using Microsoft.Extensions.Logging;
using nKafka.Client.Auth;
using nKafka.Contracts;
using nKafka.Contracts.MessageDefinitions;
using nKafka.Contracts.MessageSerializers;

namespace nKafka.Client;

public sealed class SaslAuthenticator : IAuthenticator
{
    private readonly ArrayPool<byte> _arrayPool = ArrayPool<byte>.Shared;
    private readonly ILogger _logger;
    private readonly ConnectionConfig _config;
    private readonly IStreamProvider _streamProvider;

    public SaslAuthenticator(ConnectionConfig config, IStreamProvider streamProvider, ILoggerFactory loggerFactory)
    {
        _config = config;
        _streamProvider = streamProvider;
        _logger = loggerFactory.CreateLogger<SaslAuthenticator>();
    }

    public async ValueTask AuthenticateAsync(IConnection connection, CancellationToken ct)
    {
        string? mechanism = _config.Sasl?.Mechanism;
        if (string.IsNullOrEmpty(mechanism))
        {
            return;
        }

        _logger.LogInformation("Starting SASL authentication with {Mechanism}.", mechanism);

        // 1. SaslHandshake (raw stream — receive loop not started yet)
        _logger.LogDebug("Sending SaslHandshakeRequest.");
        SaslHandshakeResponse handshakeResponse = await SendSaslHandshakeAsync(mechanism, ct);

        if (handshakeResponse.ErrorCode != 0)
        {
            throw new InvalidOperationException(
                $"SASL handshake failed with error code {handshakeResponse.ErrorCode}");
        }

        var supportedMechanisms = handshakeResponse.Mechanisms;
        if (supportedMechanisms == null ||
            !supportedMechanisms.Any(m => m == mechanism))
        {
            string supported = supportedMechanisms != null
                ? string.Join(", ", supportedMechanisms)
                : "none";
            throw new InvalidOperationException(
                $"Server does not support {mechanism}. Supported: {supported}");
        }

        // 2. SASL Authenticate (SCRAM-SHA exchange)
        HashAlgorithmName hashAlgorithm;
        if (mechanism == "SCRAM-SHA-512")
            hashAlgorithm = HashAlgorithmName.SHA512;
        else if (mechanism == "SCRAM-SHA-256")
            hashAlgorithm = HashAlgorithmName.SHA256;
        else
            throw new NotSupportedException($"SASL mechanism {mechanism} is not supported");

        string? username = _config.Sasl!.Username;
        string? password = _config.Sasl!.Password;
        if (string.IsNullOrEmpty(username) || string.IsNullOrEmpty(password))
        {
            throw new InvalidOperationException("SASL credentials (username, password) are required when SaslMechanism is configured.");
        }

        var scramClient = new ScramClient(username, password, hashAlgorithm);

        // Round 1: Client-First → Server-First
        byte[] clientFirstBytes = scramClient.GetClientFirstMessage();
        _logger.LogDebug("Sending SASL authenticate (client-first).");
        byte[] serverFirstBytes = await SendSaslAuthenticateAsync(clientFirstBytes, ct);

        // Round 2: Client-Final → Server-Final
        byte[] clientFinalBytes = scramClient.GetClientFinalMessage(serverFirstBytes);
        _logger.LogDebug("Sending SASL authenticate (client-final).");
        byte[] serverFinalBytes = await SendSaslAuthenticateAsync(clientFinalBytes, ct);

        scramClient.VerifyServerFinalMessage(serverFinalBytes);
        _logger.LogInformation("SASL authentication successful.");
    }

    private async Task<SaslHandshakeResponse> SendSaslHandshakeAsync(string mechanism, CancellationToken ct)
    {
        int correlationId = IdGenerator.Next();
        short apiVersion = 1;
        bool isFlexible = apiVersion >= 2;
        short requestHeaderVersion = isFlexible ? (short)2 : (short)1;
        short responseHeaderVersion = isFlexible ? (short)1 : (short)0;
        var requestHeader = new RequestHeader
        {
            RequestApiKey = 17,
            RequestApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = _config.ClientId,
        };

        var writer = new BufferWriter(_arrayPool, _config.RequestBufferSize);
        try
        {
            int start = writer.Position;
            writer.WriteInt(0);

            var serializationContext = new SaslSerializationContext(_arrayPool, _config.RequestBufferSize)
            {
                Config = new SerializationConfig
                {
                    ClientId = _config.ClientId,
                }
            };

            RequestHeaderSerializer.Serialize(ref writer, requestHeader, requestHeaderVersion, serializationContext);
            SaslHandshakeRequestSerializer.Serialize(ref writer, new SaslHandshakeRequest { Mechanism = mechanism }, 0, serializationContext);

            int end = writer.Position;
            int size = end - start - 4;
            writer.Position = start;
            writer.WriteInt(size);
            writer.Position = end;

            await _streamProvider.ReadStream.WriteAsync(writer.Memory.Slice(0, writer.Position), ct);
            await _streamProvider.ReadStream.FlushAsync(ct);
            _logger.LogDebug("SASL handshake request written ({PayloadSize} bytes).", writer.Position);

            byte[] sizeBuf = new byte[4];
            await ReadAtLeastAsync(sizeBuf, 4, ct);
            int responseSize = (sizeBuf[0] << 24) | (sizeBuf[1] << 16) | (sizeBuf[2] << 8) | sizeBuf[3];

            byte[] responseBuffer = _arrayPool.Rent(responseSize);
            try
            {
                await ReadAtLeastAsync(responseBuffer, responseSize, ct);

                var buffer = new Memory<byte>(responseBuffer, 0, responseSize);
                var reader = new BufferReader(buffer);
                var responseHeader = ResponseHeaderSerializer.Deserialize(ref reader, responseHeaderVersion, serializationContext);

                if (responseHeader.CorrelationId != correlationId)
                {
                    throw new InvalidOperationException(
                        $"Correlation ID mismatch: expected {correlationId}, got {responseHeader.CorrelationId}");
                }

                return SaslHandshakeResponseSerializer.Deserialize(ref reader, apiVersion, serializationContext);
            }
            finally
            {
                _arrayPool.Return(responseBuffer);
            }
        }
        finally
        {
            writer.Dispose();
        }
    }

    private async ValueTask<byte[]> SendSaslAuthenticateAsync(
        byte[] authBytes, CancellationToken ct)
    {
        _logger.LogDebug("SASL authenticate payload ({Len} bytes).", authBytes.Length);
        var request = new SaslAuthenticateRequest
        {
            AuthBytes = authBytes,
        };

        int correlationId = IdGenerator.Next();

        short effectiveApiVersion = 0;
        bool isFlexible = effectiveApiVersion >= 2;
        short requestHeaderVersion = isFlexible ? (short)2 : (short)1;
        short responseHeaderVersion = isFlexible ? (short)1 : (short)0;
        var requestHeader = new RequestHeader
        {
            RequestApiKey = 36,
            RequestApiVersion = effectiveApiVersion,
            CorrelationId = correlationId,
            ClientId = _config.ClientId,
        };

        var writer = new BufferWriter(_arrayPool, _config.RequestBufferSize);
        try
        {
            int start = writer.Position;
            writer.WriteInt(0);

            var serializationContext = new SaslSerializationContext(_arrayPool, _config.RequestBufferSize)
            {
                Config = new SerializationConfig
                {
                    ClientId = _config.ClientId,
                }
            };

            RequestHeaderSerializer.Serialize(ref writer, requestHeader, requestHeaderVersion, serializationContext);
            SaslAuthenticateRequestSerializer.Serialize(ref writer, request, effectiveApiVersion, serializationContext);

            int end = writer.Position;
            int size = end - start - 4;
            writer.Position = start;
            writer.WriteInt(size);
            writer.Position = end;

            await _streamProvider.ReadStream.WriteAsync(writer.Memory.Slice(0, writer.Position), ct);
            await _streamProvider.ReadStream.FlushAsync(ct);
            _logger.LogDebug("SASL auth request written ({PayloadSize} bytes).", writer.Position);

            byte[] sizeBuf = new byte[4];
            await ReadAtLeastAsync(sizeBuf, 4, ct);
            int responseSize = (sizeBuf[0] << 24) | (sizeBuf[1] << 16) | (sizeBuf[2] << 8) | sizeBuf[3];

            byte[] responseBuffer = _arrayPool.Rent(responseSize);
            try
            {
                await ReadAtLeastAsync(responseBuffer, responseSize, ct);

                var buffer = new Memory<byte>(responseBuffer, 0, responseSize);
                var reader = new BufferReader(buffer);
                var responseHeader = ResponseHeaderSerializer.Deserialize(ref reader, responseHeaderVersion, serializationContext);

                if (responseHeader.CorrelationId != correlationId)
                {
                    throw new InvalidOperationException(
                        $"Correlation ID mismatch: expected {correlationId}, got {responseHeader.CorrelationId}");
                }

                var response = SaslAuthenticateResponseSerializer.Deserialize(ref reader, effectiveApiVersion, serializationContext);

                if (response.ErrorCode != 0)
                {
                    string errorMsg = response.ErrorMessage ?? $"Error code {response.ErrorCode}";
                    throw new InvalidOperationException($"SASL authentication failed: {errorMsg}");
                }

                return response.AuthBytes?.ToArray() ?? [];
            }
            finally
            {
                _arrayPool.Return(responseBuffer);
            }
        }
        finally
        {
            writer.Dispose();
        }
    }

    private async Task ReadAtLeastAsync(byte[] buffer, int minimumBytes, CancellationToken ct)
    {
        await _streamProvider.ReadStream.ReadAtLeastAsync(buffer, minimumBytes, true, ct);
    }

    private class SaslSerializationContext(ArrayPool<byte> arrayPool, int bufferSize) : ISerializationContext
    {
        public required SerializationConfig Config { get; init; }

        public BufferWriter CreateWriter()
        {
            return new BufferWriter(arrayPool, bufferSize);
        }
    }
}
