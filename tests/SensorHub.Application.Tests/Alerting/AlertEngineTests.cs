using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using SensorHub.Application.Alerting;
using SensorHub.Application.Processing;
using SensorHub.Domain.Alerts;
using SensorHub.Domain.Readings;

namespace SensorHub.Application.Tests.Alerting;

[Collection("telemetry")]
public class AlertEngineTests
{
    private static readonly DateTimeOffset T0 = new(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);
    private static readonly Guid Sensor = Guid.NewGuid();

    private readonly FakeTimeProvider _clock = new(T0);
    private readonly FakeCatalog _catalog = new();
    private readonly InMemoryStateStore _states = new();
    private readonly FakeAlertStore _alerts = new();
    private readonly RecordingPublisher _publisher = new();
    private readonly List<string> _callLog = [];
    private readonly AlertEngine _engine;

    public AlertEngineTests()
    {
        _states.Log = _alerts.Log = _publisher.Log = _callLog;
        _engine = new AlertEngine(_catalog, _states, _alerts, _publisher, _clock, NullLogger<AlertEngine>.Instance);
    }

    private static AlertRule Threshold(double threshold = 80, TimeSpan? duration = null, double hysteresis = 0, Guid? sensor = null) =>
        AlertRule.Create(sensor ?? Sensor, "Temp alta", RuleType.Threshold, Comparison.GreaterThan, threshold,
            duration ?? TimeSpan.Zero, hysteresis, Severity.Critical, T0);

    private static ConsumedReading C(int seconds, double value, Guid? sensor = null) =>
        new(Reading.Create(sensor ?? Sensor, T0.AddSeconds(seconds), value), T0);

    // ------------------------------------------------------------------ dedupe

    [Fact]
    public async Task A_thousand_violating_readings_produce_exactly_one_alert_and_one_event()
    {
        _catalog.Add(Threshold());

        await _engine.EvaluateAsync(Enumerable.Range(0, 1000).Select(i => C(i, 90)).ToList(), default);

        Assert.Single(_alerts.All);
        var published = Assert.Single(_publisher.Events);
        Assert.Equal(AlertEventKind.Fired, published.Kind);
        Assert.Equal(_alerts.All.Single().Id, published.AlertId);
        Assert.Equal(Severity.Critical, published.Severity);
    }

    [Fact]
    public async Task No_duplicate_alert_across_many_batches_while_the_signal_stays_high()
    {
        _catalog.Add(Threshold());

        for (var batch = 0; batch < 20; batch++)
            await _engine.EvaluateAsync(Enumerable.Range(batch * 50, 50).Select(i => C(i, 95)).ToList(), default);

        Assert.Single(_alerts.All);
        Assert.Single(_publisher.Events);
    }

    // ------------------------------------------------------------------ máquina de estado através do motor

    [Fact]
    public async Task Duration_is_measured_in_event_time_across_batches()
    {
        _catalog.Add(Threshold(duration: TimeSpan.FromMinutes(5)));

        await _engine.EvaluateAsync([C(0, 90), C(120, 90)], default);
        Assert.Empty(_alerts.All); // 2 min de violação: ainda Pending

        await _engine.EvaluateAsync([C(240, 90), C(300, 90)], default); // completa 5 min (estado veio do "Redis")
        var alert = Assert.Single(_alerts.All);
        Assert.Equal(T0.AddSeconds(300), alert.FiredAt); // no instante da leitura, não do relógio da máquina
    }

    [Fact]
    public async Task Recovery_resolves_the_same_alert_and_publishes_a_resolved_event()
    {
        _catalog.Add(Threshold());
        await _engine.EvaluateAsync([C(0, 90)], default);

        await _engine.EvaluateAsync([C(10, 70)], default);

        var alert = Assert.Single(_alerts.All);
        Assert.Equal(AlertStatus.Resolved, alert.Status);
        Assert.Equal(T0.AddSeconds(10), alert.ResolvedAt);
        Assert.Equal([AlertEventKind.Fired, AlertEventKind.Resolved], _publisher.Events.Select(e => e.Kind));
        Assert.Equal(alert.Id, _publisher.Events[1].AlertId);
    }

    [Fact]
    public async Task A_new_episode_after_resolution_creates_a_new_alert()
    {
        _catalog.Add(Threshold());
        await _engine.EvaluateAsync([C(0, 90), C(10, 70), C(20, 91)], default);

        Assert.Equal(2, _alerts.All.Count);
        Assert.Equal([AlertStatus.Resolved, AlertStatus.Firing], _alerts.All.OrderBy(a => a.FiredAt).Select(a => a.Status));
    }

    [Fact]
    public async Task Multiple_rules_on_the_same_sensor_are_evaluated_independently()
    {
        var critical = Threshold(threshold: 90);
        var warning = AlertRule.Create(Sensor, "Aquecendo", RuleType.Threshold, Comparison.GreaterThan, 70, TimeSpan.Zero, 0, Severity.Warning, T0);
        _catalog.Add(critical);
        _catalog.Add(warning);

        await _engine.EvaluateAsync([C(0, 75)], default);            // só o de 70 dispara
        await _engine.EvaluateAsync([C(10, 95)], default);           // agora o de 90

        Assert.Equal(2, _alerts.All.Count);
        Assert.Equal([Severity.Warning, Severity.Critical], _alerts.All.OrderBy(a => a.FiredAt).Select(a => a.Severity));
    }

    [Fact]
    public async Task Window_average_rule_keeps_its_window_between_batches_through_serialization()
    {
        var rule = AlertRule.Create(Sensor, "Média alta", RuleType.WindowAverage, Comparison.GreaterThan, 50, TimeSpan.FromMinutes(1), 0, Severity.Warning, T0);
        _catalog.Add(rule);

        // 130 s de leituras a 70, em lotes de 10 s. A janela (estado) atravessa o "Redis" como JSON a cada lote.
        for (var start = 0; start <= 120; start += 10)
            await _engine.EvaluateAsync(Enumerable.Range(start, 10).Select(i => C(i, 70)).ToList(), default);

        Assert.Single(_alerts.All);
    }

    // ------------------------------------------------------------------ eficiência

    [Fact]
    public async Task Sensors_without_rules_cost_nothing_no_state_read_or_write()
    {
        _catalog.Add(Threshold(sensor: Guid.NewGuid())); // regra de OUTRO sensor

        var transitions = await _engine.EvaluateAsync(Enumerable.Range(0, 5000).Select(i => C(i, 999)).ToList(), default);

        Assert.Equal(0, transitions);
        Assert.Equal(0, _states.Reads);
        Assert.Equal(0, _states.Writes);
    }

    [Fact]
    public async Task State_is_saved_even_when_nothing_fired_so_the_last_seen_reading_is_remembered()
    {
        var rule = Threshold();
        _catalog.Add(rule);

        await _engine.EvaluateAsync([C(5, 10)], default);

        Assert.Equal(T0.AddSeconds(5), _states.Stored[rule.Id].LastEventTime);
    }

    // ------------------------------------------------------------------ at-least-once

    [Fact]
    public async Task Writes_happen_in_the_safe_order_persist_then_publish_then_save_state()
    {
        _catalog.Add(Threshold());

        await _engine.EvaluateAsync([C(0, 90)], default);

        Assert.Equal(["state:get", "alert:insert", "publish", "state:save"], _callLog);
    }

    [Fact]
    public async Task Crash_after_persisting_and_publishing_but_before_saving_state_replays_without_a_second_alert()
    {
        _catalog.Add(Threshold());
        _states.FailNextSave = true; // "cai" depois de persistir e publicar, antes de salvar o estado
        var batch = new List<ConsumedReading> { C(0, 90) };

        await Assert.ThrowsAsync<InvalidOperationException>(() => _engine.EvaluateAsync(batch, default));
        Assert.Single(_alerts.All);
        Assert.Single(_publisher.Events);

        // o Kafka reentrega o MESMO lote; o estado no Redis ainda é o antigo (vazio)
        await _engine.EvaluateAsync(batch, default);

        Assert.Single(_alerts.All);                                    // continua UM alerta (Id determinístico)
        Assert.Equal(2, _publisher.Events.Count);                     // o evento é republicado: at-least-once
        Assert.Single(_publisher.Events.Select(e => (e.AlertId, e.Kind)).Distinct()); // mesma identidade: consumidor deduplica
    }

    [Fact]
    public async Task Crash_after_persisting_but_before_publishing_still_publishes_the_event_on_replay()
    {
        _catalog.Add(Threshold());
        _publisher.FailNext = true;
        var batch = new List<ConsumedReading> { C(0, 90) };

        await Assert.ThrowsAsync<InvalidOperationException>(() => _engine.EvaluateAsync(batch, default));
        Assert.Single(_alerts.All);
        Assert.Empty(_publisher.Events);

        await _engine.EvaluateAsync(batch, default);

        Assert.Single(_alerts.All);
        Assert.Single(_publisher.Events); // o alerta NÃO foi perdido: o evento saiu na reentrega
    }

    [Fact]
    public async Task A_redelivered_batch_after_a_successful_save_produces_nothing_new()
    {
        _catalog.Add(Threshold());
        var batch = new List<ConsumedReading> { C(0, 90), C(1, 91) };
        await _engine.EvaluateAsync(batch, default);

        var transitions = await _engine.EvaluateAsync(batch, default);

        Assert.Equal(0, transitions);
        Assert.Single(_publisher.Events);
    }

    [Fact]
    public async Task Losing_the_redis_state_cannot_create_a_second_open_alert()
    {
        _catalog.Add(Threshold());
        await _engine.EvaluateAsync([C(0, 90)], default);
        _states.Stored.Clear(); // Redis perdeu tudo (flush/failover sem persistência)

        await _engine.EvaluateAsync([C(10, 92)], default); // a regra "dispara de novo" com outro instante

        Assert.Single(_alerts.All);                  // o índice "1 alerta aberto por regra" barrou
        Assert.Single(_publisher.Events);            // e nenhum evento duplicado saiu
    }

    [Fact]
    public async Task State_store_failure_propagates_so_the_consumer_retries_the_batch()
    {
        _catalog.Add(Threshold());
        _states.FailNextRead = true;

        await Assert.ThrowsAsync<InvalidOperationException>(() => _engine.EvaluateAsync([C(0, 90)], default));
        Assert.Empty(_alerts.All);
    }

    [Fact]
    public async Task Handler_delegates_the_batch_to_the_engine()
    {
        _catalog.Add(Threshold());
        var handler = new EvaluateAlertsHandler(_engine);

        await handler.HandleAsync([C(0, 90)], default);

        Assert.Single(_alerts.All);
    }

    // ------------------------------------------------------------------ "sem dados"

    private AlertRule NoData(TimeSpan? duration = null) =>
        AlertRule.Create(Sensor, "Sensor offline", RuleType.NoData, Comparison.GreaterThan, 0, duration ?? TimeSpan.FromMinutes(2), 0, Severity.Warning, T0);

    [Fact]
    public async Task No_data_rule_fires_from_the_sweep_when_silence_exceeds_the_duration()
    {
        var rule = NoData();
        _catalog.Add(rule);
        await _engine.EvaluateAsync([C(0, 50)], default); // última leitura vista: T0

        _clock.Advance(TimeSpan.FromMinutes(1));
        Assert.Equal(0, await _engine.SweepNoDataAsync(TimeSpan.FromSeconds(30), default)); // 1 min de silêncio: ainda não

        _clock.Advance(TimeSpan.FromMinutes(2));
        Assert.Equal(1, await _engine.SweepNoDataAsync(TimeSpan.FromSeconds(30), default));

        var alert = Assert.Single(_alerts.All);
        Assert.Equal(T0.AddMinutes(2), alert.FiredAt); // quando o silêncio cruzou o limite, não o "agora" da varredura
        Assert.Equal(RuleStatus.Firing, _states.Stored[rule.Id].Status);
    }

    [Fact]
    public async Task Consecutive_sweeps_do_not_fire_the_no_data_alert_twice()
    {
        _catalog.Add(NoData());
        await _engine.EvaluateAsync([C(0, 50)], default);
        _clock.Advance(TimeSpan.FromMinutes(10));

        await _engine.SweepNoDataAsync(TimeSpan.FromSeconds(30), default);
        _clock.Advance(TimeSpan.FromMinutes(1));
        await _engine.SweepNoDataAsync(TimeSpan.FromSeconds(30), default);
        await _engine.SweepNoDataAsync(TimeSpan.FromSeconds(30), default);

        Assert.Single(_alerts.All);
        Assert.Single(_publisher.Events);
    }

    [Fact]
    public async Task A_fresh_reading_resolves_the_no_data_alert()
    {
        _catalog.Add(NoData());
        await _engine.EvaluateAsync([C(0, 50)], default);
        _clock.Advance(TimeSpan.FromMinutes(5));
        await _engine.SweepNoDataAsync(TimeSpan.FromSeconds(30), default);

        // o sensor volta: leitura com timestamp atual
        await _engine.EvaluateAsync([new ConsumedReading(Reading.Create(Sensor, _clock.GetUtcNow(), 51), _clock.GetUtcNow())], default);

        Assert.Equal(AlertStatus.Resolved, _alerts.All.Single().Status);
        Assert.Equal([AlertEventKind.Fired, AlertEventKind.Resolved], _publisher.Events.Select(e => e.Kind));
    }

    [Fact]
    public async Task Replaying_an_old_backlog_does_not_fire_no_data_for_a_healthy_sensor()
    {
        _catalog.Add(NoData());
        _clock.Advance(TimeSpan.FromHours(2)); // o consumer ficou parado 2 h; o backlog tem leituras de 2 h atrás

        await _engine.EvaluateAsync(Enumerable.Range(0, 100).Select(i => C(i, 50)).ToList(), default);

        Assert.Empty(_alerts.All); // só a varredura, com dado real, pode declarar silêncio
    }

    [Fact]
    public async Task Sweep_ignores_sensors_that_never_reported_to_the_engine()
    {
        _catalog.Add(NoData());
        _clock.Advance(TimeSpan.FromDays(1));

        Assert.Equal(0, await _engine.SweepNoDataAsync(TimeSpan.FromSeconds(30), default));
        Assert.Empty(_alerts.All);
    }

    [Fact]
    public async Task Sweep_holds_back_while_the_engine_is_behind_on_the_stream()
    {
        _catalog.Add(NoData());
        // O motor acaba de processar um lote cujo dado tem 10 minutos: está 10 min ATRASADO no fluxo.
        var old = T0.AddMinutes(-10);
        await _engine.EvaluateAsync([new ConsumedReading(Reading.Create(Sensor, old, 50), old)], default);

        var whileBehind = await _engine.SweepNoDataAsync(TimeSpan.FromSeconds(30), default);

        Assert.Equal(0, whileBehind);
        Assert.Empty(_alerts.All); // "silêncio" de 10 min é só efeito do atraso: NÃO vira alerta falso

        // Passado tempo sem lotes novos, o fluxo está ocioso (sem atraso a esconder) e o silêncio é real.
        _clock.Advance(TimeSpan.FromMinutes(2));
        Assert.Equal(1, await _engine.SweepNoDataAsync(TimeSpan.FromSeconds(30), default));
    }

    // ============================================================ test doubles

    private sealed class FakeCatalog : IRuleCatalog
    {
        private readonly List<AlertRule> _rules = [];
        public void Add(AlertRule rule) => _rules.Add(rule);

        public Task<IReadOnlyList<AlertRule>> GetEnabledRulesForSensorAsync(Guid sensorId, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<AlertRule>>(_rules.Where(r => r.SensorId == sensorId && r.Enabled).ToList());

        public Task<IReadOnlyList<AlertRule>> GetEnabledNoDataRulesAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<AlertRule>>(_rules.Where(r => r.Enabled && r.Type == RuleType.NoData).ToList());
    }

    /// <summary>Guarda o estado como JSON, como o Redis: garante que o estado sobrevive à serialização.</summary>
    private sealed class InMemoryStateStore : IRuleStateStore
    {
        public Dictionary<Guid, RuleState> Stored { get; } = [];
        public List<string> Log { get; set; } = [];
        public int Reads { get; private set; }
        public int Writes { get; private set; }
        public bool FailNextSave { get; set; }
        public bool FailNextRead { get; set; }

        public Task<IReadOnlyDictionary<Guid, RuleState>> GetManyAsync(IReadOnlyCollection<Guid> ruleIds, CancellationToken cancellationToken)
        {
            Reads++;
            Log.Add("state:get");
            if (FailNextRead) { FailNextRead = false; throw new InvalidOperationException("redis fora"); }

            return Task.FromResult<IReadOnlyDictionary<Guid, RuleState>>(ruleIds
                .Where(Stored.ContainsKey)
                .ToDictionary(id => id, id => JsonSerializer.Deserialize<RuleState>(JsonSerializer.Serialize(Stored[id]))!));
        }

        public Task SaveManyAsync(IReadOnlyDictionary<Guid, RuleState> states, CancellationToken cancellationToken)
        {
            Writes++;
            Log.Add("state:save");
            if (FailNextSave) { FailNextSave = false; throw new InvalidOperationException("caiu antes de salvar o estado"); }

            foreach (var (id, state) in states) Stored[id] = JsonSerializer.Deserialize<RuleState>(JsonSerializer.Serialize(state))!;
            return Task.CompletedTask;
        }

        public Task DeleteAsync(Guid ruleId, CancellationToken cancellationToken)
        {
            Stored.Remove(ruleId);
            return Task.CompletedTask;
        }
    }

    /// <summary>Reproduz a semântica do banco: Id único e no máximo 1 alerta aberto por regra.</summary>
    private sealed class FakeAlertStore : IAlertStore
    {
        public List<Alert> All { get; } = [];
        public List<string> Log { get; set; } = [];

        public Task<InsertOutcome> InsertFiredAsync(Alert alert, CancellationToken cancellationToken)
        {
            Log.Add("alert:insert");
            if (All.Any(a => a.Id == alert.Id)) return Task.FromResult(InsertOutcome.AlreadyExists);
            if (All.Any(a => a.RuleId == alert.RuleId && a.Status != AlertStatus.Resolved)) return Task.FromResult(InsertOutcome.OpenAlertExists);

            All.Add(alert);
            return Task.FromResult(InsertOutcome.Created);
        }

        public Task<Alert?> ResolveOpenAsync(Guid ruleId, DateTimeOffset at, double? value, CancellationToken cancellationToken)
        {
            Log.Add("alert:resolve");
            var alert = All.Where(a => a.RuleId == ruleId && (a.Status != AlertStatus.Resolved || a.ResolvedAt == at))
                .OrderByDescending(a => a.FiredAt).FirstOrDefault();
            alert?.Resolve(at, value);
            return Task.FromResult(alert);
        }
    }

    private sealed class RecordingPublisher : IAlertEventPublisher
    {
        public List<AlertEvent> Events { get; } = [];
        public List<string> Log { get; set; } = [];
        public bool FailNext { get; set; }

        public Task PublishAsync(IReadOnlyList<AlertEvent> events, CancellationToken cancellationToken)
        {
            Log.Add("publish");
            if (FailNext) { FailNext = false; throw new InvalidOperationException("kafka fora"); }

            Events.AddRange(events);
            return Task.CompletedTask;
        }
    }
}
