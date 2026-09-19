using Microsoft.Extensions.Logging;
using SensorHub.Application.Processing;
using SensorHub.Domain.Alerts;
using SensorHub.Domain.Readings;

namespace SensorHub.Application.Alerting;

/// <summary>
/// Motor de alertas em stream. Aplica as regras (funções puras do domínio) ao fluxo de leituras e a uma
/// varredura periódica, e transforma transições em alertas persistidos e eventos publicados.
///
/// <b>Ordem das escritas e por que ela importa</b> (at-least-once, sem alerta duplicado):
/// <list type="number">
/// <item>Persiste o alerta (INSERT idempotente: Id determinístico + índice "1 aberto por regra").</item>
/// <item>Publica o evento no Kafka (confirmado pelo broker).</item>
/// <item>SÓ ENTÃO salva o novo estado das regras no Redis.</item>
/// </list>
/// Uma queda em qualquer ponto reentrega o lote, com o estado ANTIGO ainda no Redis. A reavaliação produz as mesmas
/// transições nos mesmos instantes; o INSERT vira no-op (mesmo Id) e o evento é republicado (duplicata inofensiva:
/// quem consome deduplica por AlertId + Kind). Se o estado fosse salvo antes, uma queda perderia o alerta.
/// </summary>
public sealed class AlertEngine(
    IRuleCatalog catalog,
    IRuleStateStore states,
    IAlertStore alerts,
    IAlertEventPublisher events,
    TimeProvider clock,
    ILogger<AlertEngine> logger)
{
    private long _lastBatchTicks;   // relógio do fim do último lote
    private long _lastDelayTicks;   // atraso de processamento medido nesse lote

    /// <summary>
    /// Atraso de processamento do último lote (agora − quando a API ingeriu a leitura mais nova). Se o motor está
    /// atrasado no fluxo, "o sensor está calado" pode ser só efeito do atraso, e a varredura de "sem dados" se abstém.
    /// Sem lote recente, o fluxo está ocioso: não há atraso.
    /// </summary>
    public TimeSpan ProcessingDelay(TimeSpan idleAfter)
    {
        var ageOfLastBatch = clock.GetUtcNow().UtcTicks - Interlocked.Read(ref _lastBatchTicks);
        return ageOfLastBatch > idleAfter.Ticks ? TimeSpan.Zero : TimeSpan.FromTicks(Interlocked.Read(ref _lastDelayTicks));
    }

    /// <summary>Avalia um lote de leituras contra as regras dos sensores envolvidos. Retorna quantas transições ocorreram.</summary>
    public async Task<int> EvaluateAsync(IReadOnlyList<ConsumedReading> batch, CancellationToken cancellationToken)
    {
        var now = clock.GetUtcNow();
        RecordDelay(batch, now);

        var work = new List<(AlertRule Rule, List<Reading> Readings)>();
        foreach (var bySensor in batch.GroupBy(b => b.Reading.SensorId))
        {
            var rules = await catalog.GetEnabledRulesForSensorAsync(bySensor.Key, cancellationToken);
            if (rules.Count == 0) continue; // a maioria dos sensores não tem regra: custo ~zero

            var readings = bySensor.Select(b => b.Reading).OrderBy(r => r.Timestamp).ToList();
            work.AddRange(rules.Select(rule => (rule, readings)));
        }

        if (work.Count == 0) return 0;

        var rulesById = work.Select(w => w.Rule).DistinctBy(r => r.Id).ToDictionary(r => r.Id);
        var loaded = await states.GetManyAsync(rulesById.Keys.ToList(), cancellationToken);
        var current = rulesById.ToDictionary(kv => kv.Key, kv => loaded.TryGetValue(kv.Key, out var s) ? s : kv.Value.NewState());

        var transitions = new List<(AlertRule Rule, RuleEvaluation Evaluation)>();
        foreach (var (rule, readings) in work)
        {
            var state = current[rule.Id];
            foreach (var reading in readings)
            {
                var evaluation = rule.Evaluate(state, reading, now);
                if (evaluation.Transition != Transition.None) transitions.Add((rule, evaluation));
            }
        }

        await ApplyTransitionsAsync(transitions, cancellationToken);
        await states.SaveManyAsync(current, cancellationToken); // por ÚLTIMO (ver a documentação da classe)
        return transitions.Count;
    }

    /// <summary>
    /// Varredura por relógio das regras "sem dados". Ausência de leituras não gera evento, então precisa de
    /// alguém perguntando. Usa a última leitura vista registrada NO ESTADO da própria regra (não depende do
    /// consumer do último valor). Retorna quantas transições ocorreram.
    /// </summary>
    public async Task<int> SweepNoDataAsync(TimeSpan maxAcceptableDelay, CancellationToken cancellationToken)
    {
        var delay = ProcessingDelay(idleAfter: TimeSpan.FromMinutes(1));
        if (delay > maxAcceptableDelay)
        {
            logger.LogWarning("Varredura 'sem dados' adiada: o motor está {Delay:F0}s atrasado no fluxo (silêncio aparente pode ser só atraso).", delay.TotalSeconds);
            return 0;
        }

        var rules = await catalog.GetEnabledNoDataRulesAsync(cancellationToken);
        if (rules.Count == 0) return 0;

        var now = clock.GetUtcNow();
        var loaded = await states.GetManyAsync(rules.Select(r => r.Id).ToList(), cancellationToken);

        var transitions = new List<(AlertRule Rule, RuleEvaluation Evaluation)>();
        var changed = new Dictionary<Guid, RuleState>();
        foreach (var rule in rules)
        {
            // Sem estado = nenhuma leitura deste sensor jamais chegou ao motor: não há "última vez" para medir.
            if (!loaded.TryGetValue(rule.Id, out var state)) continue;

            var evaluation = rule.EvaluateSilence(state, state.LastEventTime, now);
            if (evaluation.Transition == Transition.None) continue;

            transitions.Add((rule, evaluation));
            changed[rule.Id] = state;
        }

        if (transitions.Count == 0) return 0;

        await ApplyTransitionsAsync(transitions, cancellationToken);
        await states.SaveManyAsync(changed, cancellationToken);
        return transitions.Count;
    }

    private async Task ApplyTransitionsAsync(
        IReadOnlyList<(AlertRule Rule, RuleEvaluation Evaluation)> transitions, CancellationToken cancellationToken)
    {
        if (transitions.Count == 0) return;

        var toPublish = new List<AlertEvent>(transitions.Count);
        foreach (var (rule, evaluation) in transitions)
        {
            if (evaluation.Transition == Transition.Fired)
            {
                var alert = Alert.Fire(rule, evaluation.At, evaluation.Value);
                var outcome = await alerts.InsertFiredAsync(alert, cancellationToken);
                if (outcome == InsertOutcome.OpenAlertExists)
                {
                    logger.LogWarning("Regra {Rule} tentou disparar com um alerta ainda aberto; disparo duplicado ignorado.", rule.Id);
                    continue;
                }

                toPublish.Add(ToEvent(alert.Id, rule, AlertEventKind.Fired, evaluation, alert.Message));
            }
            else
            {
                var resolved = await alerts.ResolveOpenAsync(rule.Id, evaluation.At, evaluation.Value, cancellationToken);
                if (resolved is null) continue;

                toPublish.Add(ToEvent(resolved.Id, rule, AlertEventKind.Resolved, evaluation, resolved.Message));
            }
        }

        if (toPublish.Count > 0)
        {
            await events.PublishAsync(toPublish, cancellationToken);
            logger.LogInformation("{Count} evento(s) de alerta publicados ({Fired} disparos, {Resolved} resoluções).",
                toPublish.Count, toPublish.Count(e => e.Kind == AlertEventKind.Fired), toPublish.Count(e => e.Kind == AlertEventKind.Resolved));
        }
    }

    private static AlertEvent ToEvent(Guid alertId, AlertRule rule, AlertEventKind kind, RuleEvaluation evaluation, string message) =>
        new(AlertEvent.CurrentSchemaVersion, alertId, rule.Id, rule.SensorId, kind, rule.Severity, evaluation.At, evaluation.Value, message);

    private void RecordDelay(IReadOnlyList<ConsumedReading> batch, DateTimeOffset now)
    {
        if (batch.Count == 0) return;

        var newestIngest = batch.Max(b => b.IngestedAt);
        Interlocked.Exchange(ref _lastDelayTicks, Math.Max(0, (now - newestIngest).Ticks));
        Interlocked.Exchange(ref _lastBatchTicks, now.UtcTicks);
    }
}

/// <summary>Handler do grupo <c>sensorhub.alerts</c>: entrega cada lote de leituras ao motor.</summary>
public sealed class EvaluateAlertsHandler(AlertEngine engine) : IReadingBatchHandler
{
    public async Task HandleAsync(IReadOnlyList<ConsumedReading> batch, CancellationToken cancellationToken) =>
        await engine.EvaluateAsync(batch, cancellationToken);
}
