using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using SensorHub.Application.Alerting;
using SensorHub.Application.Ingestion;
using SensorHub.Application.Observability;
using SensorHub.Domain.Alerts;

namespace SensorHub.Application.Tests.Observability;

/// <summary>Registra o que os instrumentos do SensorHub emitem, sem OpenTelemetry: só a BCL (MeterListener).</summary>
internal sealed class MetricRecorder : IDisposable
{
    private readonly MeterListener _listener = new();
    private readonly List<(string Name, double Value, Dictionary<string, object?> Tags)> _measurements = [];

    public MetricRecorder()
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == SensorHubTelemetry.Name) listener.EnableMeasurementEvents(instrument);
        };
        _listener.SetMeasurementEventCallback<long>(Record);
        _listener.SetMeasurementEventCallback<int>((i, m, t, s) => Record(i, m, t, s));
        _listener.SetMeasurementEventCallback<double>(Record);
        _listener.Start();
    }

    private void Record<T>(Instrument instrument, T measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state) where T : struct
    {
        lock (_measurements)
            _measurements.Add((instrument.Name, Convert.ToDouble(measurement), tags.ToArray().ToDictionary(t => t.Key, t => t.Value)));
    }

    public double Sum(string name, params (string Key, string Value)[] tags)
    {
        lock (_measurements)
            return _measurements
                .Where(m => m.Name == name && tags.All(t => m.Tags.TryGetValue(t.Key, out var v) && (v?.ToString() ?? "") == t.Value))
                .Sum(m => m.Value);
    }

    public int Count(string name)
    {
        lock (_measurements) return _measurements.Count(m => m.Name == name);
    }

    public void Dispose() => _listener.Dispose();
}

public class ConsumerLagRegistryTests
{
    [Fact]
    public void Lag_is_reported_per_partition_with_group_topic_and_partition_tags()
    {
        var owner = Guid.NewGuid().ToString();
        var group = $"g-{Guid.NewGuid():N}";
        ConsumerLagRegistry.Update(owner, group, [("readings", 0, 120), ("readings", 1, 30)]);

        var lag = ConsumerLagRegistry.Measure()
            .Where(m => m.Tags.ToArray().Any(t => t.Key == "group" && (string?)t.Value == group)).ToList();

        Assert.Equal(2, lag.Count);
        Assert.Equal(150, ConsumerLagRegistry.TotalFor(group));
        Assert.Contains(lag, m => m.Value == 120 && m.Tags.ToArray().Any(t => t.Key == "partition" && (int?)t.Value == 0));
        ConsumerLagRegistry.Remove(owner);
    }

    [Fact]
    public void A_new_snapshot_replaces_the_old_one_so_partitions_given_away_disappear()
    {
        var owner = Guid.NewGuid().ToString();
        var group = $"g-{Guid.NewGuid():N}";
        ConsumerLagRegistry.Update(owner, group, [("readings", 0, 100), ("readings", 1, 100)]);

        ConsumerLagRegistry.Update(owner, group, [("readings", 1, 40)]); // a partição 0 foi cedida a outra instância

        Assert.Equal(40, ConsumerLagRegistry.TotalFor(group)); // o lag congelado da 0 não pode dar falso alarme
        ConsumerLagRegistry.Remove(owner);
    }

    [Fact]
    public void A_consumer_that_leaves_stops_being_reported()
    {
        var owner = Guid.NewGuid().ToString();
        var group = $"g-{Guid.NewGuid():N}";
        ConsumerLagRegistry.Update(owner, group, [("readings", 0, 999)]);

        ConsumerLagRegistry.Remove(owner);

        Assert.Equal(0, ConsumerLagRegistry.TotalFor(group));
    }

    [Fact]
    public void Several_consumers_of_the_same_group_are_summed()
    {
        var group = $"g-{Guid.NewGuid():N}";
        var a = Guid.NewGuid().ToString();
        var b = Guid.NewGuid().ToString();
        ConsumerLagRegistry.Update(a, group, [("readings", 0, 10), ("readings", 1, 20)]);
        ConsumerLagRegistry.Update(b, group, [("readings", 2, 5)]);

        Assert.Equal(35, ConsumerLagRegistry.TotalFor(group));
        ConsumerLagRegistry.Remove(a);
        ConsumerLagRegistry.Remove(b);
    }
}

[Collection("telemetry")]
public class IngestionMetricsTests
{
    private static readonly DateTimeOffset Now = new(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Accepted_and_rejected_readings_are_counted_separately()
    {
        var clock = new FakeTimeProvider(Now);
        var service = new IngestionService(
            new ReadingRequestValidator(Options.Create(new IngestionOptions()), clock), new NoopPublisher(), clock);
        using var metrics = new MetricRecorder();

        await service.IngestAsync(
        [
            new(Guid.NewGuid(), Now, 1, null),
            new(Guid.NewGuid(), Now, 2, null),
            new(Guid.Empty, Now, 3, null),     // rejeitada
            new(Guid.NewGuid(), Now, double.NaN, null) // rejeitada
        ], default);

        Assert.Equal(2, metrics.Sum("sensorhub.ingest.readings", ("result", "accepted")));
        Assert.Equal(2, metrics.Sum("sensorhub.ingest.readings", ("result", "rejected")));
    }

    private sealed class NoopPublisher : IReadingPublisher
    {
        public Task PublishAsync(IReadOnlyList<SensorHub.Application.Contracts.ReadingMessage> messages, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }
}

[Collection("telemetry")]
public class AlertMetricsTests
{
    private static readonly DateTimeOffset T0 = new(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Fired_and_resolved_alerts_are_counted_with_severity_and_rule_type()
    {
        var sensor = Guid.NewGuid();
        var rule = AlertRule.Create(sensor, "Temp", RuleType.Threshold, Comparison.GreaterThan, 80, TimeSpan.Zero, 0, Severity.Critical, T0);
        var engine = new AlertEngine(new Catalog(rule), new States(), new Alerts(), new Publisher(), new FakeTimeProvider(T0), NullLogger<AlertEngine>.Instance);
        using var metrics = new MetricRecorder();

        await engine.EvaluateAsync(
        [
            new(SensorHub.Domain.Readings.Reading.Create(sensor, T0, 90), T0),                    // dispara
            new(SensorHub.Domain.Readings.Reading.Create(sensor, T0.AddSeconds(1), 70), T0)       // resolve
        ], default);

        Assert.Equal(1, metrics.Sum("sensorhub.alerts.transitions", ("kind", "fired"), ("severity", "Critical"), ("rule_type", "Threshold")));
        Assert.Equal(1, metrics.Sum("sensorhub.alerts.transitions", ("kind", "resolved"), ("severity", "Critical")));
    }

    private sealed class Catalog(AlertRule rule) : IRuleCatalog
    {
        public Task<IReadOnlyList<AlertRule>> GetEnabledRulesForSensorAsync(Guid sensorId, CancellationToken ct) =>
            Task.FromResult<IReadOnlyList<AlertRule>>(sensorId == rule.SensorId ? [rule] : []);
        public Task<IReadOnlyList<AlertRule>> GetEnabledNoDataRulesAsync(CancellationToken ct) => Task.FromResult<IReadOnlyList<AlertRule>>([]);
    }

    private sealed class States : IRuleStateStore
    {
        private readonly Dictionary<Guid, RuleState> _s = [];
        public Task<IReadOnlyDictionary<Guid, RuleState>> GetManyAsync(IReadOnlyCollection<Guid> ids, CancellationToken ct) =>
            Task.FromResult<IReadOnlyDictionary<Guid, RuleState>>(ids.Where(_s.ContainsKey).ToDictionary(i => i, i => _s[i]));
        public Task SaveManyAsync(IReadOnlyDictionary<Guid, RuleState> states, CancellationToken ct)
        {
            foreach (var (k, v) in states) _s[k] = v;
            return Task.CompletedTask;
        }
        public Task DeleteAsync(Guid ruleId, CancellationToken ct) => Task.CompletedTask;
    }

    private sealed class Alerts : IAlertStore
    {
        private readonly List<Alert> _all = [];
        public Task<InsertOutcome> InsertFiredAsync(Alert alert, CancellationToken ct)
        {
            _all.Add(alert);
            return Task.FromResult(InsertOutcome.Created);
        }
        public Task<Alert?> ResolveOpenAsync(Guid ruleId, DateTimeOffset at, double? value, CancellationToken ct)
        {
            var a = _all.FirstOrDefault(x => x.RuleId == ruleId && x.Status != AlertStatus.Resolved);
            a?.Resolve(at, value);
            return Task.FromResult(a);
        }
    }

    private sealed class Publisher : IAlertEventPublisher
    {
        public Task PublishAsync(IReadOnlyList<AlertEvent> events, CancellationToken ct) => Task.CompletedTask;
    }
}
