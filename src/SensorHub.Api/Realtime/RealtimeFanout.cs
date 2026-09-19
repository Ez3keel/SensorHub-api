using System.Diagnostics;
using Confluent.Kafka;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Options;
using SensorHub.Application.Alerting;
using SensorHub.Application.Observability;
using SensorHub.Application.Processing;
using SensorHub.Application.Realtime;
using SensorHub.Infrastructure.Kafka;

namespace SensorHub.Api.Realtime;

/// <summary>
/// Ponte entre o Kafka e os dashboards. Handler do consumer group de tempo real: NÃO empurra nada por leitura, só deposita
/// no <see cref="ReadingCoalescer"/> (uma escrita em memória por leitura). Quem fala com os clientes é o
/// <see cref="RealtimeFlushService"/>, num ritmo fixo. Assim a taxa de mensagens para o navegador independe da taxa de
/// ingestão: 150 mil leituras/s de entrada continuam sendo, no máximo, 4 atualizações/s por sensor na saída.
/// </summary>
public sealed class RealtimeFanout : IReadingBatchHandler
{
    public ReadingCoalescer Coalescer { get; } = new();

    public Task HandleAsync(IReadOnlyList<ConsumedReading> batch, CancellationToken cancellationToken)
    {
        foreach (var item in batch) Coalescer.Add(item.Reading);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Hospeda o consumer de leituras do tempo real. TODAS as instâncias da API entram no MESMO consumer group: o Kafka reparte
/// as partições entre elas, cada leitura é tratada por UMA instância, e o backplane Redis do SignalR entrega o push a
/// clientes conectados em QUALQUER instância. Começa do fim do log (<c>Latest</c>): um dashboard novo não deve reprocessar
/// o histórico, só mostrar o "agora".
/// </summary>
public sealed class RealtimeReadingsConsumerHost(
    RealtimeFanout fanout,
    IOptions<RealtimeOptions> options,
    IOptions<KafkaOptions> kafka,
    IDeadLetterSink deadLetters,
    ILoggerFactory loggers) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!options.Value.Enabled) return;

        using var consumer = new KafkaBatchConsumer(
            "realtime", kafka.Value,
            new BatchConsumerOptions { GroupId = options.Value.ReadingsGroupId, StartFromEarliest = false, MaxBatchSize = 5000, MaxWaitMs = 100 },
            fanout, deadLetters, loggers.CreateLogger("Consumer.realtime"));

        await consumer.StartAsync(stoppingToken);
        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // shutdown
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
        }
    }
}

/// <summary>Drena o coalescer a cada <c>FlushMs</c> e empurra o resultado para os grupos do SignalR.</summary>
public sealed class RealtimeFlushService(
    RealtimeFanout fanout,
    IHubContext<TelemetryHub> hub,
    ISensorDirectory directory,
    ISubscriptionRegistry registry,
    IOptions<RealtimeOptions> options,
    TimeProvider clock,
    ILogger<RealtimeFlushService> logger) : BackgroundService
{
    private IReadOnlySet<Guid> _subscribedSensors = new HashSet<Guid>();
    private DateTimeOffset _subscribedLoadedAt;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!options.Value.Enabled) return;

        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(options.Value.FlushMs));
        while (await WaitAsync(timer, stoppingToken))
        {
            try
            {
                await FlushAsync(stoppingToken);
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                // Tempo real é uma visão viva: perder um flush é aceitável (o próximo traz o estado mais novo). Nunca derruba o serviço.
                logger.LogWarning(ex, "Falha no flush de tempo real; seguindo para o próximo ciclo.");
            }
        }
    }

    public async Task FlushAsync(CancellationToken cancellationToken)
    {
        var readings = fanout.Coalescer.Drain();
        if (readings.Count == 0) return;

        // 1) visão geral: uma mensagem por GRUPO de sensores (a rede vê dezenas de mensagens, não milhares)
        var byGroup = new Dictionary<string, List<ReadingPush>>();
        foreach (var reading in readings)
        {
            var group = await directory.GetGroupAsync(reading.SensorId, cancellationToken);
            if (group is null) continue; // sensor não cadastrado: não pertence a nenhum painel

            if (!byGroup.TryGetValue(group, out var list)) byGroup[group] = list = [];
            list.Add(new ReadingPush(reading.SensorId, reading.Timestamp, reading.Value));
        }

        foreach (var (group, list) in byGroup)
        {
            await hub.Clients.Group(TelemetryHub.ReadingGroup(group)).SendAsync("readings", list, cancellationToken);
            SensorHubTelemetry.RealtimePushes.Add(1, new KeyValuePair<string, object?>("kind", "readings"));
        }

        // 2) detalhe: só para sensores que alguém, em qualquer instância, está assistindo
        var subscribed = await GetSubscribedSensorsAsync(cancellationToken);
        if (subscribed.Count == 0) return;

        foreach (var reading in readings.Where(r => subscribed.Contains(r.SensorId)))
            await hub.Clients.Group(TelemetryHub.SensorGroup(reading.SensorId))
                .SendAsync("reading", new ReadingPush(reading.SensorId, reading.Timestamp, reading.Value), cancellationToken);

        SensorHubTelemetry.RealtimePushes.Add(readings.Count(r => subscribed.Contains(r.SensorId)), new KeyValuePair<string, object?>("kind", "reading"));
    }

    /// <summary>Lista de sensores assistidos, cacheada por 1 s: consultar o Redis a cada flush de 250 ms seria desperdício.</summary>
    private async Task<IReadOnlySet<Guid>> GetSubscribedSensorsAsync(CancellationToken cancellationToken)
    {
        if (clock.GetUtcNow() - _subscribedLoadedAt >= TimeSpan.FromSeconds(1))
        {
            _subscribedSensors = await registry.GetSubscribedSensorsAsync(cancellationToken);
            _subscribedLoadedAt = clock.GetUtcNow();
        }

        return _subscribedSensors;
    }

    private static async Task<bool> WaitAsync(PeriodicTimer timer, CancellationToken cancellationToken)
    {
        try { return await timer.WaitForNextTickAsync(cancellationToken); }
        catch (OperationCanceledException) { return false; }
    }
}

/// <summary>
/// Escuta o tópico de alertas e empurra cada disparo/resolução para o dashboard. Eventos de alerta são poucos: sem
/// coalescing, imediatos. Best-effort por desenho (auto-commit): se uma instância cai, o dashboard recarrega os alertas
/// abertos pela API REST; o que importa é o estado, e o Postgres é quem guarda a verdade dos alertas.
/// </summary>
public sealed class AlertEventListener(
    IHubContext<TelemetryHub> hub,
    IOptions<RealtimeOptions> options,
    IOptions<KafkaOptions> kafka,
    ILogger<AlertEventListener> logger) : BackgroundService
{
    private readonly RecentEventFilter _seen = new();

    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        !options.Value.Enabled
            ? Task.CompletedTask
            : Task.Factory.StartNew(() => RunAsync(stoppingToken), stoppingToken, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();

    private async Task RunAsync(CancellationToken ct)
    {
        using var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
        {
            BootstrapServers = kafka.Value.BootstrapServers,
            GroupId = options.Value.AlertsGroupId,
            AutoOffsetReset = AutoOffsetReset.Latest, // só o que acontece a partir de agora
            EnableAutoCommit = true
        }).Build();
        consumer.Subscribe(kafka.Value.AlertsTopic);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                ConsumeResult<string, byte[]>? result;
                try { result = consumer.Consume(TimeSpan.FromMilliseconds(500)); }
                catch (ConsumeException ex) when (!ex.Error.IsFatal) { logger.LogWarning("Erro ao consumir alertas: {Reason}", ex.Error.Reason); continue; }

                if (result is null) continue;

                var alertEvent = AlertEventSerializer.Deserialize(result.Message.Value);
                if (alertEvent is null || !_seen.TryAdd(alertEvent)) continue; // ilegível ou duplicata (at-least-once do motor)

                using var activity = SensorHubTelemetry.Source.StartActivity(
                    "sensorhub.alerts process", ActivityKind.Consumer, KafkaTraceContext.Extract(result.Message.Headers) ?? default);
                activity?.SetTag("alert.kind", alertEvent.Kind.ToString());

                var push = new AlertPush(alertEvent.AlertId, alertEvent.RuleId, alertEvent.SensorId, alertEvent.Kind.ToString(),
                    alertEvent.Severity.ToString(), alertEvent.At, alertEvent.Value, alertEvent.Message);
                try
                {
                    await hub.Clients.Group(TelemetryHub.AlertsGroup).SendAsync("alert", push, ct);
                    await hub.Clients.Group(TelemetryHub.SensorGroup(alertEvent.SensorId)).SendAsync("alert", push, ct);
                    SensorHubTelemetry.RealtimePushes.Add(1, new KeyValuePair<string, object?>("kind", "alert"));
                }
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    logger.LogWarning(ex, "Falha ao empurrar alerta {AlertId}.", alertEvent.AlertId);
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // shutdown
        }
        finally
        {
            consumer.Close();
        }
    }
}
