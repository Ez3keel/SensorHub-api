using SensorHub.Domain.Common;
using SensorHub.Domain.Readings;

namespace SensorHub.Domain.Alerts;

public enum RuleType
{
    /// <summary>Valor cruza o limite e permanece assim por <see cref="AlertRule.Duration"/> (0 = imediato).</summary>
    Threshold = 1,

    /// <summary>Sensor sem reportar por <see cref="AlertRule.Duration"/> (sensor offline).</summary>
    NoData = 2,

    /// <summary>Média dos últimos <see cref="AlertRule.Duration"/> cruza o limite.</summary>
    WindowAverage = 3,

    /// <summary>Variação (último − mais antigo) dentro dos últimos <see cref="AlertRule.Duration"/> cruza o limite.</summary>
    RateOfChange = 4
}

public enum Comparison
{
    GreaterThan = 1,
    LessThan = 2
}

public enum Severity
{
    Info = 1,
    Warning = 2,
    Critical = 3
}

public enum Transition
{
    None = 0,
    Fired = 1,
    Resolved = 2
}

public readonly record struct RuleEvaluation(Transition Transition, double? Value, DateTimeOffset At)
{
    public static RuleEvaluation None { get; } = new(Transition.None, null, default);
}

/// <summary>
/// Regra de alerta sobre um sensor. A avaliação é PURA e determinística: recebe o estado e uma
/// observação, muta o estado e devolve a transição. Não conhece Redis, Kafka nem relógio;
/// quem chama passa o "agora". Isso permite testar todas as regras com tempo simulado.
/// </summary>
public sealed class AlertRule
{
    public static readonly TimeSpan MaxDuration = TimeSpan.FromHours(24);

    public Guid Id { get; private set; }
    public Guid SensorId { get; private set; }
    public string Name { get; private set; } = null!;
    public RuleType Type { get; private set; }
    public Comparison Comparison { get; private set; }
    public double Threshold { get; private set; }
    public TimeSpan Duration { get; private set; }

    /// <summary>
    /// Histerese: para RESOLVER, o valor precisa voltar além do limite por esta margem. Evita o
    /// "flapping" (dispara/resolve/dispara) quando o sinal oscila em torno do limite.
    /// </summary>
    public double Hysteresis { get; private set; }

    public Severity Severity { get; private set; }
    public bool Enabled { get; private set; }
    public DateTimeOffset CreatedAt { get; private set; }

    private AlertRule() { } // EF

    public static AlertRule Create(
        Guid sensorId, string name, RuleType type, Comparison comparison, double threshold,
        TimeSpan duration, double hysteresis, Severity severity, DateTimeOffset now)
    {
        if (sensorId == Guid.Empty) throw new DomainException("SensorId não pode ser vazio.");
        if (string.IsNullOrWhiteSpace(name)) throw new DomainException("Nome da regra é obrigatório.");
        if (name.Trim().Length > 120) throw new DomainException("Nome da regra excede 120 caracteres.");
        if (!Enum.IsDefined(type)) throw new DomainException("Tipo de regra inválido.");
        if (!Enum.IsDefined(comparison)) throw new DomainException("Comparação inválida.");
        if (!Enum.IsDefined(severity)) throw new DomainException("Severidade inválida.");
        if (!double.IsFinite(hysteresis) || hysteresis < 0) throw new DomainException("Histerese deve ser um número finito >= 0.");
        if (duration < TimeSpan.Zero || duration > MaxDuration) throw new DomainException("Duração deve estar entre 0 e 24h.");

        if (type != RuleType.NoData && !double.IsFinite(threshold))
            throw new DomainException("O limite deve ser um número finito.");

        if (type != RuleType.Threshold && duration == TimeSpan.Zero)
            throw new DomainException($"Regras do tipo {type} exigem duração maior que zero.");

        return new AlertRule
        {
            Id = Guid.NewGuid(),
            SensorId = sensorId,
            Name = name.Trim(),
            Type = type,
            Comparison = comparison,
            Threshold = type == RuleType.NoData ? 0 : threshold,
            Duration = duration,
            Hysteresis = type == RuleType.NoData ? 0 : hysteresis,
            Severity = severity,
            Enabled = true,
            CreatedAt = now
        };
    }

    public void Disable() => Enabled = false;

    public void Enable() => Enabled = true;

    /// <summary>Cria o estado inicial adequado ao tipo da regra (com janela quando necessário).</summary>
    public RuleState NewState()
    {
        var state = new RuleState();
        if (Type is RuleType.WindowAverage or RuleType.RateOfChange)
            state.Window = new SlidingWindow(Duration, BucketWidth());
        return state;
    }

    /// <summary>
    /// Avalia uma leitura. <paramref name="now"/> é o relógio do consumer (só usado por NoData).
    /// Leituras não mais novas que a última avaliada são ignoradas (idempotência/ordem).
    /// </summary>
    public RuleEvaluation Evaluate(RuleState state, Reading reading, DateTimeOffset now)
    {
        if (state.LastEventTime is { } last && reading.Timestamp <= last)
            return RuleEvaluation.None;

        state.LastEventTime = reading.Timestamp;

        switch (Type)
        {
            case RuleType.Threshold:
                return Step(state, reading.Value, reading.Timestamp, sustain: Duration);

            case RuleType.NoData:
                // Uma leitura só pode RESOLVER um "sem dados" (o sensor voltou), nunca dispará-lo. Se pudesse,
                // reprocessar um backlog antigo (leituras com horas de idade em relação ao relógio) faria um
                // sensor saudável parecer mudo e dispararia um alerta falso. Quem dispara é a varredura por relógio.
                return state.Status == RuleStatus.Firing
                    ? EvaluateSilence(state, reading.Timestamp, now)
                    : RuleEvaluation.None;

            case RuleType.WindowAverage:
            {
                var stats = Track(state, reading);
                if (stats is null) return RuleEvaluation.None;
                // Aquecimento: sem cobrir (quase) a janela inteira a "média da janela" ainda não
                // existe. Evita disparar no primeiro ponto após o start do consumer.
                var warmed = stats.Value.Coverage >= Duration - TimeSpan.FromTicks(state.Window!.BucketTicks);
                return warmed
                    ? Step(state, stats.Value.Average, reading.Timestamp, sustain: TimeSpan.Zero)
                    : RuleEvaluation.None;
            }

            case RuleType.RateOfChange:
            {
                var stats = Track(state, reading);
                if (stats is null) return RuleEvaluation.None;
                var delta = stats.Value.LatestValue - stats.Value.OldestValue;
                return Step(state, delta, reading.Timestamp, sustain: TimeSpan.Zero);
            }

            default:
                return RuleEvaluation.None;
        }
    }

    /// <summary>
    /// Varredura por tempo de relógio, só para NoData: a ausência de leituras não gera evento,
    /// então alguém precisa perguntar periodicamente "há quanto tempo este sensor está mudo?".
    /// </summary>
    public RuleEvaluation EvaluateSilence(RuleState state, DateTimeOffset? lastSeen, DateTimeOffset now)
    {
        if (Type != RuleType.NoData || lastSeen is null) return RuleEvaluation.None;

        var silent = now - lastSeen.Value >= Duration;

        // Os instantes das transições NÃO usam o relógio ("agora"): usam fatos observáveis (quando o silêncio
        // cruzou o limite; quando o dado voltou). Assim uma reavaliação após queda produz exatamente o mesmo
        // instante, e o Id determinístico do Alert continua evitando duplicatas. Com "now" cada replay seria outro alerta.
        if (state.Status == RuleStatus.Normal && silent)
        {
            var firedAt = lastSeen.Value + Duration;
            state.Status = RuleStatus.Firing;
            state.FiredAt = firedAt;
            return new RuleEvaluation(Transition.Fired, null, firedAt);
        }

        if (state.Status == RuleStatus.Firing && !silent)
        {
            state.Status = RuleStatus.Normal;
            state.FiredAt = null;
            return new RuleEvaluation(Transition.Resolved, null, lastSeen.Value);
        }

        return RuleEvaluation.None;
    }

    /// <summary>Máquina de estado compartilhada: mede uma grandeza, aplica limite, duração e histerese.</summary>
    private RuleEvaluation Step(RuleState state, double measure, DateTimeOffset at, TimeSpan sustain)
    {
        switch (state.Status)
        {
            case RuleStatus.Normal:
            case RuleStatus.Pending:
                if (!Violates(measure))
                {
                    state.Status = RuleStatus.Normal;
                    state.ViolationStartedAt = null;
                    return RuleEvaluation.None;
                }

                state.ViolationStartedAt ??= at;
                if (at - state.ViolationStartedAt.Value >= sustain)
                {
                    state.Status = RuleStatus.Firing;
                    state.FiredAt = at;
                    return new RuleEvaluation(Transition.Fired, measure, at);
                }

                state.Status = RuleStatus.Pending;
                return RuleEvaluation.None;

            case RuleStatus.Firing:
                if (Cleared(measure))
                {
                    state.Status = RuleStatus.Normal;
                    state.ViolationStartedAt = null;
                    state.FiredAt = null;
                    return new RuleEvaluation(Transition.Resolved, measure, at);
                }

                return RuleEvaluation.None; // ainda disparado (ou na banda de histerese): NÃO re-dispara

            default:
                return RuleEvaluation.None;
        }
    }

    private WindowStats? Track(RuleState state, Reading reading)
    {
        state.Window ??= new SlidingWindow(Duration, BucketWidth());
        state.Window.Add(reading.Timestamp, reading.Value);
        return state.Window.Stats();
    }

    private bool Violates(double x) => Comparison == Comparison.GreaterThan ? x > Threshold : x < Threshold;

    private bool Cleared(double x) => Comparison == Comparison.GreaterThan
        ? x <= Threshold - Hysteresis
        : x >= Threshold + Hysteresis;

    /// <summary>~20 baldes por janela, no mínimo 1s: equilíbrio entre precisão de borda e tamanho do estado.</summary>
    private TimeSpan BucketWidth()
    {
        var width = TimeSpan.FromTicks(Duration.Ticks / 20);
        return width < TimeSpan.FromSeconds(1) ? TimeSpan.FromSeconds(1) : width;
    }
}
