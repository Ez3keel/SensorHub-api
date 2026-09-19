using System.Text.Json;
using MQTTnet;
using MQTTnet.Formatter;
using MQTTnet.Protocol;

namespace SensorHub.Simulator.Load;

/// <summary>
/// Publica os lotes como um dispositivo MQTT de verdade: usuário = id do dispositivo, senha = a chave de API (a mesma da ingestão HTTP), QoS 1
/// (só considera enviado depois do PUBACK do broker) e um array JSON por mensagem em <c>sensorhub/devices/{id}/readings</c>.
/// Se a conexão cai, reconecta e reenvia o mesmo lote (a persistência é idempotente por sensor_id + ts).
/// </summary>
public sealed class MqttReadingSink : IReadingSink, IAsyncDisposable
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    private readonly string _host;
    private readonly int _port;
    private readonly Guid _deviceId;
    private readonly string _apiKey;
    private readonly IMqttClient _client = new MqttClientFactory().CreateMqttClient();
    private readonly SemaphoreSlim _gate = new(1, 1); // um cliente MQTT, uma publicação por vez: o PUBACK é a confirmação
    private long _retries;

    public MqttReadingSink(string host, int port, Guid deviceId, string apiKey)
    {
        _host = host;
        _port = port;
        _deviceId = deviceId;
        _apiKey = apiKey;
    }

    public long Retries => Interlocked.Read(ref _retries);

    public string Topic => $"sensorhub/devices/{_deviceId:D}/readings";

    public async Task SendAsync(IReadOnlyList<SimulatedReading> batch, CancellationToken cancellationToken)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(
            batch.Select(r => new { sensorId = r.SensorId, timestamp = r.Timestamp, value = r.Value, unit = r.Unit }), Json);

        await _gate.WaitAsync(cancellationToken);
        try
        {
            for (var attempt = 0; ; attempt++)
            {
                try
                {
                    if (!_client.IsConnected) await ConnectAsync(cancellationToken);

                    var result = await _client.PublishAsync(new MqttApplicationMessageBuilder()
                        .WithTopic(Topic).WithPayload(payload).WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce).Build(), cancellationToken);

                    // NotAuthorized/QuotaExceeded etc.: reenviar o mesmo lote não muda o veredito (chave errada, dispositivo desativado)
                    if (result.ReasonCode != MqttClientPublishReasonCode.Success && result.ReasonCode != MqttClientPublishReasonCode.NoMatchingSubscribers)
                        throw new InvalidOperationException($"O broker recusou a publicação: {result.ReasonCode}.");
                    return;
                }
                catch (InvalidOperationException) { throw; }
                catch (Exception) when (attempt < 3 && !cancellationToken.IsCancellationRequested)
                {
                    Interlocked.Increment(ref _retries);
                    await Task.Delay(TimeSpan.FromMilliseconds(200 * Math.Pow(2, attempt)), cancellationToken);
                }
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task ConnectAsync(CancellationToken ct)
    {
        var options = new MqttClientOptionsBuilder()
            .WithTcpServer(_host, _port)
            .WithClientId($"sim-{_deviceId:N}-{Guid.NewGuid():N}"[..40])
            .WithCredentials(_deviceId.ToString("D"), _apiKey)
            .WithProtocolVersion(MqttProtocolVersion.V500)
            .WithCleanSession()
            .Build();

        var result = await _client.ConnectAsync(options, ct);
        if (result.ResultCode != MqttClientConnectResultCode.Success)
            throw new InvalidOperationException($"O broker recusou a conexão: {result.ResultCode} (chave de API do dispositivo correta?).");
    }

    public async ValueTask DisposeAsync()
    {
        if (_client.IsConnected) await _client.DisconnectAsync();
        _client.Dispose();
        _gate.Dispose();
    }
}
