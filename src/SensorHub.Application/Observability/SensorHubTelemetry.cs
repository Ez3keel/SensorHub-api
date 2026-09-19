using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace SensorHub.Application.Observability;

/// <summary>
/// Instrumentos de métricas e fonte de traces do SensorHub. Usa só a BCL (<c>System.Diagnostics</c>): a Application não
/// depende de OpenTelemetry. O host (API/Worker) é quem liga o OpenTelemetry a estes nomes (<see cref="Name"/>).
/// </summary>
public static class SensorHubTelemetry
{
    public const string Name = "SensorHub";

    public static readonly ActivitySource Source = new(Name);
    public static readonly Meter Meter = new(Name);

    // ------------------------------------------------------------------ ingestão (API)
    public static readonly Counter<long> IngestedReadings = Meter.CreateCounter<long>(
        "sensorhub.ingest.readings", "{reading}", "Leituras recebidas pela API, por resultado (accepted/rejected).");

    public static readonly Histogram<double> PublishDuration = Meter.CreateHistogram<double>(
        "sensorhub.ingest.publish.duration", "ms", "Tempo até o broker confirmar (acks=all) um lote publicado.");

    // ------------------------------------------------------------------ consumers (todos os grupos)
    public static readonly Counter<long> ConsumerMessages = Meter.CreateCounter<long>(
        "sensorhub.consumer.messages", "{message}", "Mensagens consumidas, por consumer group.");

    public static readonly Counter<long> ConsumerBatches = Meter.CreateCounter<long>(
        "sensorhub.consumer.batches", "{batch}", "Lotes processados, por consumer group.");

    public static readonly Histogram<int> ConsumerBatchSize = Meter.CreateHistogram<int>(
        "sensorhub.consumer.batch.size", "{message}", "Tamanho dos lotes processados.");

    public static readonly Histogram<double> HandlerDuration = Meter.CreateHistogram<double>(
        "sensorhub.consumer.handler.duration", "ms", "Tempo do handler por lote (gravar no banco, avaliar regras...).");

    public static readonly Histogram<double> ProcessingDelay = Meter.CreateHistogram<double>(
        "sensorhub.consumer.processing.delay", "s", "Latência ponta a ponta: da ingestão pela API até o lote ser tratado.");

    public static readonly Counter<long> ConsumerRetries = Meter.CreateCounter<long>(
        "sensorhub.consumer.retries", "{retry}", "Falhas transitórias que forçaram nova tentativa do lote.");

    public static readonly Counter<long> DeadLetters = Meter.CreateCounter<long>(
        "sensorhub.consumer.dead_letters", "{message}", "Mensagens desviadas para a DLQ, por motivo.");

    public static readonly Counter<long> Rebalances = Meter.CreateCounter<long>(
        "sensorhub.consumer.rebalances", "{event}", "Eventos de atribuição/revogação de partições.");

    // Lag: a métrica mais importante de um sistema Kafka. Gauge observável alimentado pelo registro abaixo.
    public static readonly ObservableGauge<long> ConsumerLag = Meter.CreateObservableGauge(
        "sensorhub.consumer.lag", ConsumerLagRegistry.Measure, "{message}",
        "Mensagens no log que o grupo ainda não commitou, por partição (medido dentro do consumer).");

    // ------------------------------------------------------------------ persistência e alertas
    public static readonly Counter<long> PersistedRows = Meter.CreateCounter<long>(
        "sensorhub.persistence.rows", "{row}", "Linhas do lote gravadas, por resultado (inserted/duplicate).");

    public static readonly Counter<long> AlertTransitions = Meter.CreateCounter<long>(
        "sensorhub.alerts.transitions", "{transition}", "Alertas disparados/resolvidos, por severidade e tipo de regra.");

    // ------------------------------------------------------------------ tempo real
    public static readonly Counter<long> RealtimePushes = Meter.CreateCounter<long>(
        "sensorhub.realtime.pushes", "{message}", "Mensagens empurradas aos dashboards, por tipo.");

    public static readonly UpDownCounter<long> RealtimeConnections = Meter.CreateUpDownCounter<long>(
        "sensorhub.realtime.connections", "{connection}", "Conexões SignalR abertas nesta instância.");
}

/// <summary>
/// Lag por (grupo, tópico, partição) atualizado pelos consumers a cada relatório de estatísticas do librdkafka.
/// Cada consumer substitui atomicamente o SEU instantâneo; partições que ele deixou de ter somem do gauge (senão o lag de
/// uma partição já cedida a outra instância ficaria congelado e daria falso alarme).
/// </summary>
public static class ConsumerLagRegistry
{
    private static readonly ConcurrentDictionary<string, (string Group, IReadOnlyList<(string Topic, int Partition, long Lag)> Partitions)> Owners = new();

    public static void Update(string ownerId, string group, IReadOnlyList<(string Topic, int Partition, long Lag)> partitions) =>
        Owners[ownerId] = (group, partitions);

    public static void Remove(string ownerId) => Owners.TryRemove(ownerId, out _);

    public static IEnumerable<Measurement<long>> Measure()
    {
        foreach (var (_, snapshot) in Owners)
        {
            foreach (var (topic, partition, lag) in snapshot.Partitions)
            {
                yield return new Measurement<long>(lag,
                    new KeyValuePair<string, object?>("group", snapshot.Group),
                    new KeyValuePair<string, object?>("topic", topic),
                    new KeyValuePair<string, object?>("partition", partition));
            }
        }
    }

    /// <summary>Soma do lag de um grupo (para alertas e testes).</summary>
    public static long TotalFor(string group) =>
        Owners.Values.Where(o => o.Group == group).SelectMany(o => o.Partitions).Sum(p => p.Lag);
}
