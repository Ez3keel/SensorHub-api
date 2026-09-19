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

public enum InsertOutcome
{
    /// <summary>Alerta novo, criado agora.</summary>
    Created = 1,

    /// <summary>Já existia com o MESMO Id: é a reprocessamento do mesmo disparo (o evento ainda deve ser publicado).</summary>
    AlreadyExists = 2,

    /// <summary>A regra já tem outro alerta ABERTO (Id diferente): disparo duplicado, ignorado. No máximo 1 alerta aberto por regra.</summary>
    OpenAlertExists = 3
}

public interface IAlertStore
{
    /// <summary>
    /// Insere o alerta disparado. Idempotente por dois mecanismos: o Id determinístico (reprocessamento do mesmo
    /// disparo) e um índice único parcial "1 alerta aberto por regra" (disparo duplicado com Id diferente).
    /// </summary>
    Task<InsertOutcome> InsertFiredAsync(Alert alert, CancellationToken cancellationToken);

    /// <summary>
    /// Resolve o alerta ABERTO (Firing/Acknowledged) da regra. Idempotente: se o alerta já foi resolvido NO MESMO
    /// instante <paramref name="at"/> (reprocessamento), devolve-o de novo para que o evento ainda seja publicado.
    /// Retorna null se não havia alerta aberto nem resolvido neste instante.
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
