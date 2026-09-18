using System.Text.Json;
using System.Text.Json.Serialization;
using SensorHub.Domain.Alerts;

namespace SensorHub.Application.Alerting;

public enum AlertEventKind
{
    Fired = 1,
    Resolved = 2
}

/// <summary>
/// Evento publicado no tópico de alertas sempre que um alerta dispara ou resolve. Contrato público para os
/// consumidores (dashboard em tempo real, notificações). Entrega "pelo menos uma vez": o par
/// (<see cref="AlertId"/>, <see cref="Kind"/>) identifica o evento, e quem consome deve deduplicar por ele.
/// </summary>
public sealed record AlertEvent(
    int SchemaVersion,
    Guid AlertId,
    Guid RuleId,
    Guid SensorId,
    AlertEventKind Kind,
    Severity Severity,
    DateTimeOffset At,
    double? Value,
    string Message)
{
    public const int CurrentSchemaVersion = 1;
}

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase, DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    UseStringEnumConverter = true)]
[JsonSerializable(typeof(AlertEvent))]
public sealed partial class AlertEventJsonContext : JsonSerializerContext;

public static class AlertEventSerializer
{
    public static byte[] Serialize(AlertEvent alertEvent) =>
        JsonSerializer.SerializeToUtf8Bytes(alertEvent, AlertEventJsonContext.Default.AlertEvent);

    public static AlertEvent? Deserialize(ReadOnlySpan<byte> utf8Json) =>
        JsonSerializer.Deserialize(utf8Json, AlertEventJsonContext.Default.AlertEvent);
}

/// <summary>Regras habilitadas de cada sensor (com cache em memória: a consulta ao banco não pode ficar no caminho de cada lote).</summary>
public interface IRuleCatalog
{
    Task<IReadOnlyList<AlertRule>> GetEnabledRulesForSensorAsync(Guid sensorId, CancellationToken cancellationToken);

    /// <summary>Todas as regras habilitadas do tipo "sem dados" (varridas periodicamente pelo relógio, não por leitura).</summary>
    Task<IReadOnlyList<AlertRule>> GetEnabledNoDataRulesAsync(CancellationToken cancellationToken);
}

/// <summary>Estado de avaliação de cada regra (a máquina de estado + janelas), guardado fora do processo.</summary>
public interface IRuleStateStore
{
    Task<IReadOnlyDictionary<Guid, RuleState>> GetManyAsync(IReadOnlyCollection<Guid> ruleIds, CancellationToken cancellationToken);

    Task SaveManyAsync(IReadOnlyDictionary<Guid, RuleState> states, CancellationToken cancellationToken);

    Task DeleteAsync(Guid ruleId, CancellationToken cancellationToken);
}

public interface IAlertStore
{
    /// <summary>Insere o alerta disparado. Idempotente: o Id é determinístico, então reinserir é no-op. Retorna true se foi criado agora.</summary>
    Task<bool> InsertFiredAsync(Alert alert, CancellationToken cancellationToken);

    /// <summary>
    /// Resolve o alerta ABERTO (Firing/Acknowledged) da regra, se houver. Idempotente. Retorna o alerta resolvido
    /// (para o evento) ou null se não havia alerta aberto.
    /// </summary>
    Task<Alert?> ResolveOpenAsync(Guid ruleId, DateTimeOffset at, double? value, CancellationToken cancellationToken);
}

public interface IAlertEventPublisher
{
    /// <summary>Publica os eventos e só retorna quando todos foram confirmados pelo broker.</summary>
    Task PublishAsync(IReadOnlyList<AlertEvent> events, CancellationToken cancellationToken);
}

/// <summary>Exclusão mútua entre instâncias (usada para que só UMA varra as regras de "sem dados" por vez).</summary>
public interface IDistributedLease
{
    /// <summary>Tenta adquirir o lease por <paramref name="duration"/>. Retorna um handle a descartar, ou null se outro detém.</summary>
    Task<IAsyncDisposable?> TryAcquireAsync(string name, TimeSpan duration, CancellationToken cancellationToken);
}
