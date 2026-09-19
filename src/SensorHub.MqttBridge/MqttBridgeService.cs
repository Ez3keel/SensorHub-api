using System.Diagnostics;
using Microsoft.Extensions.Options;
using MQTTnet;
using MQTTnet.Protocol;
using SensorHub.Application.Ingestion;
using SensorHub.Application.Observability;
using SensorHub.Application.Security;

namespace SensorHub.MqttBridge;

/// <summary>Como uma mensagem foi resolvida: <see cref="Ack"/> confirma ao broker; <see cref="Redeliver"/> NÃO confirma e força a reentrega.</summary>
public enum MessageDisposition { Ack, Redeliver }

/// <summary>
/// MQTT → Kafka. Assina (assinatura compartilhada, QoS 1, sessão persistente) os tópicos de leituras de TODOS os dispositivos e entrega cada
/// mensagem ao MESMO <see cref="IngestionService"/> da API HTTP: validação, posse do sensor, idempotência e publicação no Kafka são um
/// caminho só. A confirmação (PUBACK) ao broker é MANUAL e só acontece depois que o Kafka confirmou: se o bridge cai no meio, o broker
/// reentrega e a persistência idempotente (sensor_id, ts) absorve a duplicata. Ou seja, at-least-once de ponta a ponta.
/// </summary>
public sealed class MqttBridgeService(
    IngestionService ingestion,
    IOptions<MqttBridgeOptions> options,
    ILogger<MqttBridgeService> logger) : BackgroundService
{
    private readonly MqttBridgeOptions _options = options.Value;
    private readonly IMqttClient _client = new MqttClientFactory().CreateMqttClient();
    private CancellationToken _stopping;
    private bool _everConnected;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _stopping = stoppingToken;
        _client.ApplicationMessageReceivedAsync += OnMessageAsync;
        _client.DisconnectedAsync += _ => { MqttMetrics.SetConnected(false); return Task.CompletedTask; };

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (!_client.IsConnected) await ConnectAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Sem conexão com o broker MQTT ({Host}:{Port}); nova tentativa em instantes.", _options.Host, _options.Port);
            }

            try { await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken); }
            catch (OperationCanceledException) { break; }
        }

        if (_client.IsConnected) await _client.DisconnectAsync();
        _client.Dispose();
        MqttMetrics.SetConnected(false);
    }

    private async Task ConnectAsync(CancellationToken ct)
    {
        // Um clientId FIXO por réplica + sessão persistente: é o que faz o broker guardar as mensagens QoS 1 enquanto o bridge está fora.
        var clientId = $"sensorhub-bridge-{Environment.MachineName}";
        var connectOptions = new MqttClientOptionsBuilder()
            .WithTcpServer(_options.Host, _options.Port)
            .WithClientId(clientId)
            .WithCredentials(_options.BridgeUsername, _options.BridgePassword)
            .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
            .WithCleanSession(false)
            .WithSessionExpiryInterval((uint)_options.SessionExpirySeconds)
            .WithKeepAlivePeriod(TimeSpan.FromSeconds(15))
            .Build();

        var result = await _client.ConnectAsync(connectOptions, ct);
        if (result.ResultCode != MqttClientConnectResultCode.Success)
            throw new InvalidOperationException($"O broker recusou o bridge: {result.ResultCode}.");

        var subscribe = new MqttClientSubscribeOptionsBuilder()
            .WithTopicFilter(f => f.WithTopic(MqttTopics.SharedSubscription(_options.SharedGroup)).WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
            .Build();
        var subscription = await _client.SubscribeAsync(subscribe, ct);
        if (subscription.Items.Any(i => i.ResultCode > MqttClientSubscribeResultCode.GrantedQoS2))
            throw new InvalidOperationException("O broker recusou a assinatura dos tópicos de leituras (ACL do bridge?).");

        if (_everConnected) MqttMetrics.Reconnects.Add(1);
        _everConnected = true;
        MqttMetrics.SetConnected(true);
        logger.LogInformation("Bridge MQTT conectado a {Host}:{Port}; assinatura {Filter}.", _options.Host, _options.Port, MqttTopics.SharedSubscription(_options.SharedGroup));
    }

    private async Task OnMessageAsync(MqttApplicationMessageReceivedEventArgs e)
    {
        e.AutoAcknowledge = false; // o PUBACK só sai depois que o Kafka confirmar

        MessageDisposition disposition;
        try
        {
            disposition = await ProcessAsync(e.ApplicationMessage.Topic, System.Buffers.BuffersExtensions.ToArray(e.ApplicationMessage.Payload), _stopping);
        }
        catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
        {
            return; // encerrando: não confirma, o broker reentrega na próxima sessão
        }

        if (disposition == MessageDisposition.Ack)
        {
            await e.AcknowledgeAsync(_stopping);
        }
        else
        {
            // Sem PUBACK e com a conexão viva o broker só reenviaria na próxima reconexão. Derrubar a conexão a antecipa
            // (o laço de ExecuteAsync reconecta e a sessão persistente reentrega o que ficou pendente).
            logger.LogWarning("Kafka indisponível para o bridge: derrubando a conexão para o broker reentregar as mensagens pendentes.");
            _ = Task.Run(() => _client.DisconnectAsync(), CancellationToken.None);
        }
    }

    /// <summary>Núcleo testável (sem MQTT): decide o destino de UMA mensagem e a resolve.</summary>
    public async Task<MessageDisposition> ProcessAsync(string topic, byte[] payload, CancellationToken ct)
    {
        using var activity = SensorHubTelemetry.Source.StartActivity("mqtt process", ActivityKind.Consumer);

        if (!MqttTopics.TryGetDeviceId(topic, out var deviceId))
        {
            // não deveria ocorrer (o filtro só casa o padrão certo), mas nunca confie no que chega pela rede
            return Malformed("tópico fora do esquema", topic);
        }

        if (payload.Length > _options.MaxPayloadBytes)
            return Malformed($"payload de {payload.Length} bytes acima do limite", topic);

        var parsed = ReadingPayload.Parse(payload, maxBatchSize: 1000);
        if (!parsed.IsValid) return Malformed(parsed.Error!, topic);

        // A identidade vem do TÓPICO (a ACL do broker garante que só o dono publica nele), nunca do conteúdo da mensagem.
        var device = new DeviceIdentity(deviceId, $"mqtt:{deviceId:N}");

        for (var attempt = 0; ; attempt++)
        {
            try
            {
                var result = await ingestion.IngestAsync(parsed.Readings!, device, ct);
                var outcome = !result.HasRejections ? "accepted" : result.Accepted == 0 ? "rejected" : "partial";
                MqttMetrics.Messages.Add(1, new KeyValuePair<string, object?>("outcome", outcome));
                if (result.HasRejections)
                    // Reenviar não muda o veredito (sensor alheio, valor absurdo...): confirma e registra. Sem NACK em MQTT 3.1.1 que sirva.
                    logger.LogWarning("Dispositivo {DeviceId}: {Rejected} de {Total} leituras rejeitadas ({First}).",
                        deviceId, result.Rejected.Count, parsed.Readings!.Count, string.Join("; ", result.Rejected[0].Errors));
                return MessageDisposition.Ack;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                if (attempt >= _options.PublishRetries)
                {
                    MqttMetrics.Messages.Add(1, new KeyValuePair<string, object?>("outcome", "retry"));
                    logger.LogError(ex, "Falha persistente ao publicar no Kafka após {Attempts} tentativas.", attempt + 1);
                    return MessageDisposition.Redeliver;
                }

                var delay = TimeSpan.FromMilliseconds(200 * Math.Pow(2, attempt));
                logger.LogWarning(ex, "Falha ao publicar no Kafka (tentativa {Attempt}); repetindo em {Delay} ms.", attempt + 1, delay.TotalMilliseconds);
                await Task.Delay(delay, ct);
            }
        }
    }

    private MessageDisposition Malformed(string reason, string topic)
    {
        MqttMetrics.Messages.Add(1, new KeyValuePair<string, object?>("outcome", "malformed"));
        // Mensagem malformada nunca vira válida: reentregá-la travaria a fila do dispositivo (poison message). Confirma e descarta com log.
        logger.LogWarning("Mensagem MQTT descartada em {Topic}: {Reason}.", topic, reason);
        return MessageDisposition.Ack;
    }
}
