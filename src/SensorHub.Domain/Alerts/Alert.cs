using System.Security.Cryptography;
using SensorHub.Domain.Common;

namespace SensorHub.Domain.Alerts;

public enum AlertStatus
{
    Firing = 1,
    Acknowledged = 2,
    Resolved = 3
}

/// <summary>
/// Um alerta disparado por uma regra. Máquina de estado: Firing → Acknowledged → Resolved
/// (ou Firing → Resolved direto quando o sinal normaliza antes de alguém reconhecer).
/// </summary>
public sealed class Alert
{
    public Guid Id { get; private set; }
    public Guid RuleId { get; private set; }
    public Guid SensorId { get; private set; }
    public Severity Severity { get; private set; }
    public AlertStatus Status { get; private set; }
    public string Message { get; private set; } = null!;
    public double? TriggerValue { get; private set; }
    public DateTimeOffset FiredAt { get; private set; }
    public DateTimeOffset? AcknowledgedAt { get; private set; }
    public string? AcknowledgedBy { get; private set; }
    public DateTimeOffset? ResolvedAt { get; private set; }
    public double? ResolvedValue { get; private set; }

    private Alert() { } // EF

    public static Alert Fire(AlertRule rule, DateTimeOffset firedAt, double? triggerValue) => new()
    {
        Id = DeterministicId(rule.Id, firedAt),
        RuleId = rule.Id,
        SensorId = rule.SensorId,
        Severity = rule.Severity,
        Status = AlertStatus.Firing,
        Message = Describe(rule, triggerValue),
        TriggerValue = triggerValue,
        FiredAt = firedAt
    };

    /// <summary>
    /// O Id é derivado de (regra, instante do disparo). O Kafka entrega "pelo menos uma vez": se o
    /// consumer cair depois de disparar e antes de commitar o offset, o mesmo disparo é reprocessado.
    /// Com Id determinístico, o INSERT repetido colide na chave primária e vira no-op: alerta
    /// duplicado no banco é impossível por construção.
    /// </summary>
    public static Guid DeterministicId(Guid ruleId, DateTimeOffset firedAt)
    {
        Span<byte> input = stackalloc byte[24];
        ruleId.TryWriteBytes(input[..16]);
        BitConverter.TryWriteBytes(input[16..], firedAt.UtcTicks);
        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(input, hash);
        return new Guid(hash[..16]);
    }

    public void Acknowledge(string user, DateTimeOffset now)
    {
        if (string.IsNullOrWhiteSpace(user)) throw new DomainException("Usuário do reconhecimento é obrigatório.");
        if (Status != AlertStatus.Firing)
            throw new DomainException($"Só é possível reconhecer um alerta em Firing (estado atual: {Status}).");

        Status = AlertStatus.Acknowledged;
        AcknowledgedAt = now;
        AcknowledgedBy = user.Trim();
    }

    /// <summary>Resolve o alerta. Idempotente: resolver um alerta já resolvido devolve false e não altera nada.</summary>
    public bool Resolve(DateTimeOffset at, double? value)
    {
        if (Status == AlertStatus.Resolved) return false;

        Status = AlertStatus.Resolved;
        ResolvedAt = at;
        ResolvedValue = value;
        return true;
    }

    private static string Describe(AlertRule rule, double? value)
    {
        var op = rule.Comparison == Comparison.GreaterThan ? ">" : "<";
        return rule.Type switch
        {
            RuleType.Threshold => $"{rule.Name}: valor {Format(value)} {op} {rule.Threshold:0.##} por {rule.Duration}",
            RuleType.NoData => $"{rule.Name}: sem leituras há {rule.Duration}",
            RuleType.WindowAverage => $"{rule.Name}: média {Format(value)} {op} {rule.Threshold:0.##} em {rule.Duration}",
            RuleType.RateOfChange => $"{rule.Name}: variação {Format(value)} {op} {rule.Threshold:0.##} em {rule.Duration}",
            _ => rule.Name
        };
    }

    private static string Format(double? v) => v is null ? "n/d" : v.Value.ToString("0.##", System.Globalization.CultureInfo.InvariantCulture);
}
