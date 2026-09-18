using SensorHub.Domain.Alerts;
using SensorHub.Domain.Common;
using SensorHub.Domain.Readings;

namespace SensorHub.Domain.Tests.Alerts;

public class AlertRuleTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
    private static readonly Guid SensorId = Guid.NewGuid();

    private static AlertRule Rule(
        RuleType type = RuleType.Threshold, Comparison cmp = Comparison.GreaterThan, double threshold = 80,
        TimeSpan? duration = null, double hysteresis = 0) =>
        AlertRule.Create(SensorId, "regra", type, cmp, threshold, duration ?? TimeSpan.Zero, hysteresis, Severity.Warning, T0);

    private static Reading At(TimeSpan offset, double value) => Reading.Create(SensorId, T0 + offset, value);

    private static RuleEvaluation Eval(AlertRule rule, RuleState state, TimeSpan offset, double value) =>
        rule.Evaluate(state, At(offset, value), T0 + offset);

    // ---------------------------------------------------------------- criação

    [Fact]
    public void Create_validates_inputs()
    {
        Assert.Throws<DomainException>(() => AlertRule.Create(Guid.Empty, "r", RuleType.Threshold, Comparison.GreaterThan, 1, TimeSpan.Zero, 0, Severity.Info, T0));
        Assert.Throws<DomainException>(() => AlertRule.Create(SensorId, " ", RuleType.Threshold, Comparison.GreaterThan, 1, TimeSpan.Zero, 0, Severity.Info, T0));
        Assert.Throws<DomainException>(() => AlertRule.Create(SensorId, "r", RuleType.Threshold, Comparison.GreaterThan, double.NaN, TimeSpan.Zero, 0, Severity.Info, T0));
        Assert.Throws<DomainException>(() => AlertRule.Create(SensorId, "r", RuleType.Threshold, Comparison.GreaterThan, 1, TimeSpan.FromSeconds(-1), 0, Severity.Info, T0));
        Assert.Throws<DomainException>(() => AlertRule.Create(SensorId, "r", RuleType.Threshold, Comparison.GreaterThan, 1, TimeSpan.FromHours(25), 0, Severity.Info, T0));
        Assert.Throws<DomainException>(() => AlertRule.Create(SensorId, "r", RuleType.Threshold, Comparison.GreaterThan, 1, TimeSpan.Zero, -1, Severity.Info, T0));
    }

    [Theory]
    [InlineData(RuleType.NoData)]
    [InlineData(RuleType.WindowAverage)]
    [InlineData(RuleType.RateOfChange)]
    public void Non_threshold_rules_require_positive_duration(RuleType type)
    {
        Assert.Throws<DomainException>(() =>
            AlertRule.Create(SensorId, "r", type, Comparison.GreaterThan, 1, TimeSpan.Zero, 0, Severity.Info, T0));
    }

    // -------------------------------------------------------------- threshold

    [Fact]
    public void Threshold_without_duration_fires_immediately_and_only_once()
    {
        var rule = Rule();
        var state = rule.NewState();

        Assert.Equal(Transition.None, Eval(rule, state, TimeSpan.FromSeconds(0), 70).Transition);

        var fired = Eval(rule, state, TimeSpan.FromSeconds(1), 85);
        Assert.Equal(Transition.Fired, fired.Transition);
        Assert.Equal(85, fired.Value);
        Assert.Equal(RuleStatus.Firing, state.Status);

        // leituras seguintes acima do limite NÃO disparam de novo (sem alerta duplicado)
        for (var i = 2; i < 50; i++)
            Assert.Equal(Transition.None, Eval(rule, state, TimeSpan.FromSeconds(i), 90).Transition);
    }

    [Fact]
    public void Threshold_with_duration_only_fires_after_sustained_violation()
    {
        var rule = Rule(duration: TimeSpan.FromMinutes(5));
        var state = rule.NewState();

        Assert.Equal(Transition.None, Eval(rule, state, TimeSpan.Zero, 90).Transition);
        Assert.Equal(RuleStatus.Pending, state.Status);

        Assert.Equal(Transition.None, Eval(rule, state, TimeSpan.FromMinutes(4), 90).Transition);
        Assert.Equal(Transition.Fired, Eval(rule, state, TimeSpan.FromMinutes(5), 90).Transition);
        Assert.Equal(RuleStatus.Firing, state.Status);
    }

    [Fact]
    public void Threshold_with_duration_resets_when_signal_recovers_before_duration()
    {
        var rule = Rule(duration: TimeSpan.FromMinutes(5));
        var state = rule.NewState();

        Eval(rule, state, TimeSpan.Zero, 90);
        Eval(rule, state, TimeSpan.FromMinutes(3), 70); // voltou ao normal
        Assert.Equal(RuleStatus.Normal, state.Status);
        Assert.Null(state.ViolationStartedAt);

        // novo episódio: o relógio da duração recomeça do zero
        Eval(rule, state, TimeSpan.FromMinutes(4), 90);
        Assert.Equal(Transition.None, Eval(rule, state, TimeSpan.FromMinutes(8), 90).Transition);
        Assert.Equal(Transition.Fired, Eval(rule, state, TimeSpan.FromMinutes(9), 90).Transition);
    }

    [Fact]
    public void Threshold_resolves_when_signal_returns_to_normal()
    {
        var rule = Rule();
        var state = rule.NewState();
        Eval(rule, state, TimeSpan.FromSeconds(1), 90);

        var resolved = Eval(rule, state, TimeSpan.FromSeconds(2), 75);

        Assert.Equal(Transition.Resolved, resolved.Transition);
        Assert.Equal(RuleStatus.Normal, state.Status);
        Assert.Null(state.FiredAt);
    }

    [Fact]
    public void Threshold_less_than_comparison_works()
    {
        var rule = Rule(cmp: Comparison.LessThan, threshold: 10);
        var state = rule.NewState();

        Assert.Equal(Transition.None, Eval(rule, state, TimeSpan.FromSeconds(1), 12).Transition);
        Assert.Equal(Transition.Fired, Eval(rule, state, TimeSpan.FromSeconds(2), 9).Transition);
        Assert.Equal(Transition.Resolved, Eval(rule, state, TimeSpan.FromSeconds(3), 10).Transition);
    }

    [Fact]
    public void Value_exactly_at_threshold_is_not_a_violation()
    {
        var rule = Rule(threshold: 80);
        var state = rule.NewState();

        Assert.Equal(Transition.None, Eval(rule, state, TimeSpan.FromSeconds(1), 80).Transition);
    }

    [Fact]
    public void Hysteresis_prevents_flapping_around_the_threshold()
    {
        var rule = Rule(threshold: 80, hysteresis: 5);
        var state = rule.NewState();

        Assert.Equal(Transition.Fired, Eval(rule, state, TimeSpan.FromSeconds(1), 81).Transition);

        // oscila em torno do limite, mas não abaixo de 75: continua disparado, sem eventos
        Assert.Equal(Transition.None, Eval(rule, state, TimeSpan.FromSeconds(2), 79).Transition);
        Assert.Equal(Transition.None, Eval(rule, state, TimeSpan.FromSeconds(3), 81).Transition);
        Assert.Equal(Transition.None, Eval(rule, state, TimeSpan.FromSeconds(4), 76).Transition);
        Assert.Equal(RuleStatus.Firing, state.Status);

        Assert.Equal(Transition.Resolved, Eval(rule, state, TimeSpan.FromSeconds(5), 75).Transition);
    }

    // ------------------------------------------- idempotência / ordem (event time)

    [Fact]
    public void Redelivered_reading_is_ignored()
    {
        var rule = Rule();
        var state = rule.NewState();
        var reading = At(TimeSpan.FromSeconds(1), 90);

        Assert.Equal(Transition.Fired, rule.Evaluate(state, reading, T0).Transition);
        Assert.Equal(Transition.None, rule.Evaluate(state, reading, T0).Transition);
    }

    [Fact]
    public void Late_out_of_order_reading_is_ignored_and_does_not_change_state()
    {
        var rule = Rule();
        var state = rule.NewState();
        Eval(rule, state, TimeSpan.FromSeconds(10), 90); // Firing

        var late = Eval(rule, state, TimeSpan.FromSeconds(5), 10); // valor normal, mas atrasado

        Assert.Equal(Transition.None, late.Transition);
        Assert.Equal(RuleStatus.Firing, state.Status);
    }

    [Fact]
    public void State_survives_json_roundtrip_between_readings()
    {
        var rule = Rule(duration: TimeSpan.FromMinutes(5));
        var state = rule.NewState();
        Eval(rule, state, TimeSpan.Zero, 90);

        var restored = System.Text.Json.JsonSerializer.Deserialize<RuleState>(
            System.Text.Json.JsonSerializer.Serialize(state))!;

        Assert.Equal(Transition.Fired, Eval(rule, restored, TimeSpan.FromMinutes(5), 90).Transition);
    }

    // ---------------------------------------------------------------- NoData

    [Fact]
    public void NoData_fires_when_silence_exceeds_duration_and_resolves_when_data_returns()
    {
        var rule = Rule(RuleType.NoData, duration: TimeSpan.FromMinutes(2));
        var state = rule.NewState();
        var lastSeen = T0;

        Assert.Equal(Transition.None, rule.EvaluateSilence(state, lastSeen, T0.AddMinutes(1)).Transition);
        Assert.Equal(Transition.Fired, rule.EvaluateSilence(state, lastSeen, T0.AddMinutes(2)).Transition);
        Assert.Equal(Transition.None, rule.EvaluateSilence(state, lastSeen, T0.AddMinutes(9)).Transition); // sem repetir

        // sensor volta: uma leitura recente resolve
        var back = rule.Evaluate(state, At(TimeSpan.FromMinutes(10), 1), T0.AddMinutes(10));
        Assert.Equal(Transition.Resolved, back.Transition);
        Assert.Equal(RuleStatus.Normal, state.Status);
    }

    [Fact]
    public void NoData_ignores_sensors_never_seen()
    {
        var rule = Rule(RuleType.NoData, duration: TimeSpan.FromMinutes(2));

        Assert.Equal(Transition.None, rule.EvaluateSilence(rule.NewState(), null, T0.AddDays(1)).Transition);
    }

    [Fact]
    public void NoData_does_not_resolve_from_stale_replayed_reading()
    {
        var rule = Rule(RuleType.NoData, duration: TimeSpan.FromMinutes(2));
        var state = rule.NewState();
        rule.EvaluateSilence(state, T0, T0.AddMinutes(5)); // Firing

        // reprocessamento de leitura antiga (timestamp velho) não prova que o sensor voltou
        var stale = rule.Evaluate(state, At(TimeSpan.FromMinutes(1), 1), T0.AddMinutes(5));

        Assert.Equal(Transition.None, stale.Transition);
        Assert.Equal(RuleStatus.Firing, state.Status);
    }

    [Fact]
    public void EvaluateSilence_is_noop_for_other_rule_types()
    {
        var rule = Rule();
        Assert.Equal(Transition.None, rule.EvaluateSilence(rule.NewState(), T0, T0.AddDays(1)).Transition);
    }

    // --------------------------------------------------------- WindowAverage

    [Fact]
    public void WindowAverage_waits_for_window_to_warm_up()
    {
        var rule = Rule(RuleType.WindowAverage, threshold: 50, duration: TimeSpan.FromMinutes(1));
        var state = rule.NewState();

        // primeiro ponto já alto, mas a janela ainda não cobre 1 minuto: não dispara
        Assert.Equal(Transition.None, Eval(rule, state, TimeSpan.Zero, 100).Transition);
        Assert.Equal(Transition.None, Eval(rule, state, TimeSpan.FromSeconds(20), 100).Transition);
    }

    [Fact]
    public void WindowAverage_fires_on_average_not_on_single_spike()
    {
        var rule = Rule(RuleType.WindowAverage, threshold: 50, duration: TimeSpan.FromMinutes(1));
        var state = rule.NewState();

        // 60s de valores baixos com UM pico enorme: a média fica baixa → não dispara
        for (var s = 0; s <= 60; s++)
        {
            var value = s == 30 ? 200 : 10;
            Assert.Equal(Transition.None, Eval(rule, state, TimeSpan.FromSeconds(s), value).Transition);
        }
    }

    [Fact]
    public void WindowAverage_fires_when_average_is_sustained_high_and_resolves_when_it_drops()
    {
        var rule = Rule(RuleType.WindowAverage, threshold: 50, duration: TimeSpan.FromMinutes(1));
        var state = rule.NewState();
        var fired = 0;

        for (var s = 0; s <= 120; s++)
            if (Eval(rule, state, TimeSpan.FromSeconds(s), 70).Transition == Transition.Fired) fired++;

        Assert.Equal(1, fired);
        Assert.Equal(RuleStatus.Firing, state.Status);

        var resolved = 0;
        for (var s = 121; s <= 240; s++)
            if (Eval(rule, state, TimeSpan.FromSeconds(s), 10).Transition == Transition.Resolved) resolved++;

        Assert.Equal(1, resolved);
        Assert.Equal(RuleStatus.Normal, state.Status);
    }

    // ---------------------------------------------------------- RateOfChange

    [Fact]
    public void RateOfChange_fires_on_fast_rise_within_window()
    {
        var rule = Rule(RuleType.RateOfChange, threshold: 10, duration: TimeSpan.FromMinutes(1));
        var state = rule.NewState();

        Assert.Equal(Transition.None, Eval(rule, state, TimeSpan.Zero, 20).Transition);
        Assert.Equal(Transition.None, Eval(rule, state, TimeSpan.FromSeconds(20), 25).Transition); // +5
        Assert.Equal(Transition.Fired, Eval(rule, state, TimeSpan.FromSeconds(40), 35).Transition); // +15
    }

    [Fact]
    public void RateOfChange_ignores_slow_drift()
    {
        var rule = Rule(RuleType.RateOfChange, threshold: 10, duration: TimeSpan.FromMinutes(1));
        var state = rule.NewState();

        // +1°C por minuto por uma hora: nunca passa de ~1 na janela de 1 minuto
        for (var m = 0; m < 60; m++)
            Assert.Equal(Transition.None, Eval(rule, state, TimeSpan.FromMinutes(m), 20 + m).Transition);
    }

    [Fact]
    public void RateOfChange_less_than_detects_fast_drop()
    {
        var rule = Rule(RuleType.RateOfChange, cmp: Comparison.LessThan, threshold: -10, duration: TimeSpan.FromMinutes(1));
        var state = rule.NewState();

        Eval(rule, state, TimeSpan.Zero, 50);
        Assert.Equal(Transition.Fired, Eval(rule, state, TimeSpan.FromSeconds(30), 30).Transition);
    }

    [Fact]
    public void RateOfChange_resolves_once_the_window_no_longer_contains_the_jump()
    {
        var rule = Rule(RuleType.RateOfChange, threshold: 10, duration: TimeSpan.FromMinutes(1));
        var state = rule.NewState();
        Eval(rule, state, TimeSpan.Zero, 20);
        Eval(rule, state, TimeSpan.FromSeconds(30), 40); // Fired

        // estabiliza em 40: depois de 1 min o ponto antigo (20) sai da janela e a variação some
        var resolvedAt = Enumerable.Range(31, 120)
            .Select(s => Eval(rule, state, TimeSpan.FromSeconds(s), 40))
            .First(e => e.Transition == Transition.Resolved);

        Assert.Equal(0, resolvedAt.Value!.Value, precision: 6);
    }
}
