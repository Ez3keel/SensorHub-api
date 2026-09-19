using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using SensorHub.Application.Alerting;
using SensorHub.Domain.Alerts;
using SensorHub.Domain.Sensors;
using SensorHub.Infrastructure.Persistence;
using SensorHub.Infrastructure.Redis;
using SensorHub.IntegrationTests.Infrastructure;

namespace SensorHub.IntegrationTests.Alerting;

internal static class AlertTestData
{
    public static readonly DateTimeOffset T0 = new(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);

    public static AlertRule Rule(Guid? sensor = null, RuleType type = RuleType.Threshold, TimeSpan? duration = null) =>
        AlertRule.Create(sensor ?? Guid.NewGuid(), "Temp alta", type, Comparison.GreaterThan, 80,
            duration ?? (type == RuleType.Threshold ? TimeSpan.Zero : TimeSpan.FromMinutes(1)), 0, Severity.Critical, T0);

    public static async Task<Sensor> SaveSensorAsync(PlatformFixture platform, string group = "g1")
    {
        await using var db = platform.ContextFactory.CreateDbContext();
        var sensor = Sensor.Create(platform.SharedDeviceId, $"sensor-{Guid.NewGuid():N}"[..20], MetricType.Temperature, "°C", group, T0);
        db.Set<Sensor>().Add(sensor);
        await db.SaveChangesAsync();
        return sensor;
    }

    public static async Task<AlertRule> SaveRuleAsync(PlatformFixture platform, Guid sensorId, RuleType type = RuleType.Threshold, bool enabled = true, TimeSpan? duration = null)
    {
        await using var db = platform.ContextFactory.CreateDbContext();
        var rule = Rule(sensorId, type, duration);
        if (!enabled) rule.Disable();
        db.Set<AlertRule>().Add(rule);
        await db.SaveChangesAsync();
        return rule;
    }
}

[Collection(PlatformCollection.Name)]
public class AlertStoreTests(PlatformFixture platform)
{
    private PostgresAlertStore Store() => new(platform.ContextFactory);

    [Fact]
    public async Task The_migration_created_the_tables_and_the_one_open_alert_per_rule_index()
    {
        await using var command = platform.DataSource.CreateCommand(
            "SELECT indexdef FROM pg_indexes WHERE indexname = 'ux_alerts_one_open_per_rule'");
        var definition = (string)(await command.ExecuteScalarAsync())!;

        Assert.Contains("UNIQUE", definition);
        Assert.Contains("Resolved", definition); // índice PARCIAL: só alertas abertos competem
    }

    [Fact]
    public async Task Insert_is_created_the_first_time_and_already_exists_on_the_same_id()
    {
        var rule = AlertTestData.Rule();
        var alert = Alert.Fire(rule, AlertTestData.T0, 90);
        var store = Store();

        Assert.Equal(InsertOutcome.Created, await store.InsertFiredAsync(alert, default));
        Assert.Equal(InsertOutcome.AlreadyExists, await store.InsertFiredAsync(Alert.Fire(rule, AlertTestData.T0, 90), default));
    }

    [Fact]
    public async Task A_second_open_alert_for_the_same_rule_is_rejected_by_the_database()
    {
        var rule = AlertTestData.Rule();
        var store = Store();
        await store.InsertFiredAsync(Alert.Fire(rule, AlertTestData.T0, 90), default);

        var outcome = await store.InsertFiredAsync(Alert.Fire(rule, AlertTestData.T0.AddMinutes(5), 95), default); // Id diferente

        Assert.Equal(InsertOutcome.OpenAlertExists, outcome);
    }

    [Fact]
    public async Task After_resolving_the_rule_can_open_a_new_alert()
    {
        var rule = AlertTestData.Rule();
        var store = Store();
        await store.InsertFiredAsync(Alert.Fire(rule, AlertTestData.T0, 90), default);
        await store.ResolveOpenAsync(rule.Id, AlertTestData.T0.AddMinutes(1), 70, default);

        var outcome = await store.InsertFiredAsync(Alert.Fire(rule, AlertTestData.T0.AddMinutes(10), 91), default);

        Assert.Equal(InsertOutcome.Created, outcome);
    }

    [Fact]
    public async Task Concurrent_fires_for_the_same_rule_create_exactly_one_alert()
    {
        var rule = AlertTestData.Rule();
        var store = Store();

        // 12 instâncias disparam a MESMA regra ao mesmo tempo, cada uma com um instante (Id) diferente
        var outcomes = await Task.WhenAll(Enumerable.Range(0, 12).Select(i =>
            store.InsertFiredAsync(Alert.Fire(rule, AlertTestData.T0.AddSeconds(i), 90 + i), default)));

        Assert.Equal(1, outcomes.Count(o => o == InsertOutcome.Created));
        Assert.Equal(11, outcomes.Count(o => o == InsertOutcome.OpenAlertExists));
    }

    [Fact]
    public async Task Resolve_closes_the_open_alert_and_is_idempotent_for_the_same_instant()
    {
        var rule = AlertTestData.Rule();
        var store = Store();
        var fired = Alert.Fire(rule, AlertTestData.T0, 90);
        await store.InsertFiredAsync(fired, default);
        var at = AlertTestData.T0.AddMinutes(1);

        var first = await store.ResolveOpenAsync(rule.Id, at, 70, default);
        var replay = await store.ResolveOpenAsync(rule.Id, at, 70, default);          // reprocessamento: mesmo instante
        var other = await store.ResolveOpenAsync(rule.Id, at.AddMinutes(5), 60, default); // instante diferente: nada aberto

        Assert.Equal(fired.Id, first!.Id);
        Assert.Equal(AlertStatus.Resolved, first.Status);
        Assert.Equal(fired.Id, replay!.Id); // devolvido de novo para o evento poder ser republicado
        Assert.Null(other);
    }

    [Fact]
    public async Task Resolve_also_closes_an_acknowledged_alert()
    {
        var rule = AlertTestData.Rule();
        var store = Store();
        var fired = Alert.Fire(rule, AlertTestData.T0, 90);
        await store.InsertFiredAsync(fired, default);
        await using (var db = platform.ContextFactory.CreateDbContext())
        {
            var loaded = await db.Set<Alert>().SingleAsync(a => a.Id == fired.Id);
            loaded.Acknowledge("maria", AlertTestData.T0.AddSeconds(30));
            await db.SaveChangesAsync();
        }

        var resolved = await store.ResolveOpenAsync(rule.Id, AlertTestData.T0.AddMinutes(1), 70, default);

        Assert.Equal(AlertStatus.Resolved, resolved!.Status);
        Assert.Equal("maria", resolved.AcknowledgedBy); // o reconhecimento é preservado no histórico
    }

    [Fact]
    public async Task Resolve_without_any_alert_returns_null()
    {
        Assert.Null(await Store().ResolveOpenAsync(Guid.NewGuid(), AlertTestData.T0, 1, default));
    }
}

[Collection(PlatformCollection.Name)]
public class RuleStateAndLeaseTests(PlatformFixture platform)
{
    private RedisRuleStateStore States() => new(platform.Redis, Options.Create(new RedisOptions { KeyPrefix = "test:" }));
    private RedisDistributedLease Lease() => new(platform.Redis, Options.Create(new RedisOptions { KeyPrefix = "test:" }));

    [Fact]
    public async Task Rule_state_with_a_sliding_window_survives_the_roundtrip()
    {
        var rule = AlertRule.Create(Guid.NewGuid(), "Média", RuleType.WindowAverage, Comparison.GreaterThan, 50, TimeSpan.FromMinutes(1), 0, Severity.Info, AlertTestData.T0);
        var state = rule.NewState();
        for (var i = 0; i < 90; i++)
            rule.Evaluate(state, SensorHub.Domain.Readings.Reading.Create(rule.SensorId, AlertTestData.T0.AddSeconds(i), 70), AlertTestData.T0);
        var store = States();

        await store.SaveManyAsync(new Dictionary<Guid, RuleState> { [rule.Id] = state }, default);
        var loaded = (await store.GetManyAsync([rule.Id], default))[rule.Id];

        Assert.Equal(state.Status, loaded.Status);
        Assert.Equal(state.LastEventTime, loaded.LastEventTime);
        Assert.Equal(state.Window!.Stats(), loaded.Window!.Stats()); // a janela deslizante inteira
    }

    [Fact]
    public async Task Missing_states_are_simply_absent_and_many_are_read_in_one_call()
    {
        var store = States();
        var a = Guid.NewGuid();
        var b = Guid.NewGuid();
        await store.SaveManyAsync(new Dictionary<Guid, RuleState> { [a] = new() { Status = RuleStatus.Firing }, [b] = new() }, default);

        var result = await store.GetManyAsync([a, b, Guid.NewGuid()], default);

        Assert.Equal(2, result.Count);
        Assert.Equal(RuleStatus.Firing, result[a].Status);
        Assert.Empty(await store.GetManyAsync([], default));
    }

    [Fact]
    public async Task Delete_removes_the_state()
    {
        var store = States();
        var id = Guid.NewGuid();
        await store.SaveManyAsync(new Dictionary<Guid, RuleState> { [id] = new() }, default);

        await store.DeleteAsync(id, default);

        Assert.Empty(await store.GetManyAsync([id], default));
    }

    [Fact]
    public async Task Only_one_holder_can_have_the_lease_at_a_time_and_it_can_be_reacquired_after_release()
    {
        var lease = Lease();
        var name = $"sweep-{Guid.NewGuid():N}";

        var first = await lease.TryAcquireAsync(name, TimeSpan.FromSeconds(30), default);
        var second = await lease.TryAcquireAsync(name, TimeSpan.FromSeconds(30), default);

        Assert.NotNull(first);
        Assert.Null(second); // outra instância não entra

        await first.DisposeAsync();
        var third = await lease.TryAcquireAsync(name, TimeSpan.FromSeconds(30), default);
        Assert.NotNull(third);
        await third.DisposeAsync();
    }

    [Fact]
    public async Task An_expired_lease_is_taken_over_and_the_old_holder_cannot_release_the_new_owners_lock()
    {
        var lease = Lease();
        var name = $"sweep-{Guid.NewGuid():N}";
        var oldHolder = await lease.TryAcquireAsync(name, TimeSpan.FromMilliseconds(300), default);
        await Task.Delay(500); // o lease do primeiro expirou (ele "travou" ou demorou)
        var newHolder = await lease.TryAcquireAsync(name, TimeSpan.FromSeconds(30), default);
        Assert.NotNull(newHolder);

        await oldHolder!.DisposeAsync(); // o antigo "acorda" e tenta liberar

        // o lock do NOVO dono continua de pé: a liberação compara o token
        Assert.Null(await lease.TryAcquireAsync(name, TimeSpan.FromSeconds(30), default));
        await newHolder.DisposeAsync();
    }
}

[Collection(PlatformCollection.Name)]
public class RuleCatalogTests(PlatformFixture platform)
{
    [Fact]
    public async Task Returns_only_enabled_rules_of_the_sensor_and_the_no_data_subset()
    {
        var sensor = await AlertTestData.SaveSensorAsync(platform);
        var threshold = await AlertTestData.SaveRuleAsync(platform, sensor.Id);
        var noData = await AlertTestData.SaveRuleAsync(platform, sensor.Id, RuleType.NoData);
        await AlertTestData.SaveRuleAsync(platform, sensor.Id, enabled: false);
        var catalog = new CachedRuleCatalog(platform.ContextFactory, Options.Create(new AlertingOptions { RuleCacheSeconds = 0 }), TimeProvider.System);

        var rules = await catalog.GetEnabledRulesForSensorAsync(sensor.Id, default);
        var noDataRules = await catalog.GetEnabledNoDataRulesAsync(default);

        Assert.Equal(new[] { noData.Id, threshold.Id }.Order(), rules.Select(r => r.Id).Order());
        Assert.Contains(noDataRules, r => r.Id == noData.Id);
        Assert.DoesNotContain(noDataRules, r => r.Id == threshold.Id);
    }

    [Fact]
    public async Task Rules_are_cached_until_the_ttl_expires_then_refreshed()
    {
        var sensor = await AlertTestData.SaveSensorAsync(platform);
        var clock = new FakeTimeProvider(AlertTestData.T0);
        var catalog = new CachedRuleCatalog(platform.ContextFactory, Options.Create(new AlertingOptions { RuleCacheSeconds = 10 }), clock);
        Assert.Empty(await catalog.GetEnabledRulesForSensorAsync(sensor.Id, default)); // carrega o cache (vazio)

        var added = await AlertTestData.SaveRuleAsync(platform, sensor.Id);

        clock.Advance(TimeSpan.FromSeconds(5));
        Assert.Empty(await catalog.GetEnabledRulesForSensorAsync(sensor.Id, default));       // ainda dentro do TTL: não viu
        clock.Advance(TimeSpan.FromSeconds(6));
        Assert.Equal(added.Id, Assert.Single(await catalog.GetEnabledRulesForSensorAsync(sensor.Id, default)).Id); // TTL vencido: viu
    }

    [Fact]
    public async Task Deleting_a_sensor_cascades_to_its_rules_but_keeps_alert_history()
    {
        var sensor = await AlertTestData.SaveSensorAsync(platform);
        var rule = await AlertTestData.SaveRuleAsync(platform, sensor.Id);
        await new PostgresAlertStore(platform.ContextFactory).InsertFiredAsync(Alert.Fire(rule, AlertTestData.T0, 90), default);

        await using (var db = platform.ContextFactory.CreateDbContext())
        {
            db.Set<Sensor>().Remove(await db.Set<Sensor>().SingleAsync(s => s.Id == sensor.Id));
            await db.SaveChangesAsync();
        }

        await using var check = platform.ContextFactory.CreateDbContext();
        Assert.False(await check.Set<AlertRule>().AnyAsync(r => r.SensorId == sensor.Id));
        Assert.True(await check.Set<Alert>().AnyAsync(a => a.RuleId == rule.Id)); // o histórico permanece
    }
}
