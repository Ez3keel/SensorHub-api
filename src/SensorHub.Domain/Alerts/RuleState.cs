namespace SensorHub.Domain.Alerts;

/// <summary>
/// Máquina de estado de uma regra: Normal → Pending (violando, ainda dentro da duração exigida)
/// → Firing (alerta disparado) → Normal (resolvido). Pending só existe em regras com duração.
/// </summary>
public enum RuleStatus
{
    Normal = 0,
    Pending = 1,
    Firing = 2
}

/// <summary>
/// Estado mutável de UMA regra avaliada sobre UM sensor. Vive no Redis (JSON) entre leituras.
/// Como o Kafka garante ordem por sensor (chave de partição) e uma partição tem um único consumer
/// por grupo, este estado tem um único escritor por vez, sem necessidade de lock distribuído.
/// </summary>
public sealed class RuleState
{
    public RuleStatus Status { get; set; }
    public DateTimeOffset? ViolationStartedAt { get; set; }
    public DateTimeOffset? FiredAt { get; set; }

    /// <summary>
    /// Timestamp (tempo de evento) da última leitura já avaliada. Leituras com timestamp menor ou
    /// igual são ignoradas: torna a avaliação idempotente diante de reentrega e imune a dados fora de ordem.
    /// </summary>
    public DateTimeOffset? LastEventTime { get; set; }

    public SlidingWindow? Window { get; set; }
}
