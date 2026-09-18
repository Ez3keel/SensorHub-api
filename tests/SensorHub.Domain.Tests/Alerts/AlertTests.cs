using SensorHub.Domain.Alerts;
using SensorHub.Domain.Common;

namespace SensorHub.Domain.Tests.Alerts;

public class AlertTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

    private static AlertRule Rule(RuleType type = RuleType.Threshold) => AlertRule.Create(
        Guid.NewGuid(), "Temp forno", type, Comparison.GreaterThan, 80,
        type == RuleType.Threshold ? TimeSpan.FromMinutes(5) : TimeSpan.FromMinutes(2), 0, Severity.Critical, T0);

    [Fact]
    public void Fire_copies_rule_data_and_starts_in_firing()
    {
        var rule = Rule();

        var alert = Alert.Fire(rule, T0, 92.4);

        Assert.Equal(rule.Id, alert.RuleId);
        Assert.Equal(rule.SensorId, alert.SensorId);
        Assert.Equal(Severity.Critical, alert.Severity);
        Assert.Equal(AlertStatus.Firing, alert.Status);
        Assert.Equal(92.4, alert.TriggerValue);
        Assert.Contains("Temp forno", alert.Message);
        Assert.Contains("92.4", alert.Message);
    }

    [Fact]
    public void Id_is_deterministic_for_the_same_rule_and_firing_instant()
    {
        var rule = Rule();

        var a = Alert.Fire(rule, T0, 90);
        var b = Alert.Fire(rule, T0, 91); // reprocessamento do mesmo disparo

        Assert.Equal(a.Id, b.Id);
    }

    [Fact]
    public void Id_differs_across_rules_and_instants()
    {
        var rule = Rule();

        Assert.NotEqual(Alert.Fire(rule, T0, 1).Id, Alert.Fire(rule, T0.AddTicks(10), 1).Id);
        Assert.NotEqual(Alert.Fire(rule, T0, 1).Id, Alert.Fire(Rule(), T0, 1).Id);
    }

    [Fact]
    public void Acknowledge_moves_firing_to_acknowledged()
    {
        var alert = Alert.Fire(Rule(), T0, 90);

        alert.Acknowledge("  maria ", T0.AddMinutes(1));

        Assert.Equal(AlertStatus.Acknowledged, alert.Status);
        Assert.Equal("maria", alert.AcknowledgedBy);
        Assert.Equal(T0.AddMinutes(1), alert.AcknowledgedAt);
    }

    [Fact]
    public void Acknowledge_twice_or_after_resolve_is_forbidden()
    {
        var alert = Alert.Fire(Rule(), T0, 90);
        alert.Acknowledge("maria", T0);
        Assert.Throws<DomainException>(() => alert.Acknowledge("joao", T0));

        var resolved = Alert.Fire(Rule(), T0, 90);
        resolved.Resolve(T0, 10);
        Assert.Throws<DomainException>(() => resolved.Acknowledge("joao", T0));
    }

    [Fact]
    public void Acknowledge_requires_user()
    {
        Assert.Throws<DomainException>(() => Alert.Fire(Rule(), T0, 1).Acknowledge(" ", T0));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Resolve_works_from_firing_or_acknowledged(bool acknowledgeFirst)
    {
        var alert = Alert.Fire(Rule(), T0, 90);
        if (acknowledgeFirst) alert.Acknowledge("maria", T0);

        var changed = alert.Resolve(T0.AddMinutes(10), 70);

        Assert.True(changed);
        Assert.Equal(AlertStatus.Resolved, alert.Status);
        Assert.Equal(T0.AddMinutes(10), alert.ResolvedAt);
        Assert.Equal(70, alert.ResolvedValue);
    }

    [Fact]
    public void Resolve_is_idempotent()
    {
        var alert = Alert.Fire(Rule(), T0, 90);
        alert.Resolve(T0.AddMinutes(1), 70);

        var changed = alert.Resolve(T0.AddMinutes(9), 1);

        Assert.False(changed);
        Assert.Equal(T0.AddMinutes(1), alert.ResolvedAt);
        Assert.Equal(70, alert.ResolvedValue);
    }

    [Fact]
    public void NoData_alert_message_mentions_silence()
    {
        var alert = Alert.Fire(Rule(RuleType.NoData), T0, null);

        Assert.Contains("sem leituras", alert.Message);
    }
}
