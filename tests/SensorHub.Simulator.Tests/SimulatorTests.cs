using SensorHub.Domain.Sensors;
using SensorHub.Simulator;
using SensorHub.Simulator.Load;
using SensorHub.Simulator.Signal;

namespace SensorHub.Simulator.Tests;

public class SignalGeneratorTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Same_seed_produces_same_signal()
    {
        var a = new SignalGenerator(SensorProfile.For(MetricType.Temperature), seed: 7);
        var b = new SignalGenerator(SensorProfile.For(MetricType.Temperature), seed: 7);

        for (var i = 0; i < 200; i++)
            Assert.Equal(a.Next(T0.AddSeconds(i)), b.Next(T0.AddSeconds(i)));
    }

    [Theory]
    [InlineData(MetricType.Temperature)]
    [InlineData(MetricType.Humidity)]
    [InlineData(MetricType.Vibration)]
    [InlineData(MetricType.Pressure)]
    public void Values_stay_inside_the_plausible_range(MetricType metric)
    {
        // spikes forçados: mesmo com picos o valor não pode sair da faixa física
        var profile = SensorProfile.For(metric) with { SpikeProbability = 0.5, SpikeMagnitude = 1e6 };
        var generator = new SignalGenerator(profile, seed: 1);

        for (var i = 0; i < 2000; i++)
            Assert.True(metric.IsPlausible(generator.Next(T0.AddSeconds(i))));
    }

    [Fact]
    public void Signal_oscillates_around_the_baseline()
    {
        var profile = SensorProfile.For(MetricType.Temperature) with { SpikeProbability = 0 };
        var generator = new SignalGenerator(profile, seed: 3);

        var values = Enumerable.Range(0, 600).Select(i => generator.Next(T0.AddSeconds(i))).ToList();

        Assert.InRange(values.Average(), profile.Baseline - 2, profile.Baseline + 2);
        Assert.True(values.Max() - values.Min() > profile.Amplitude, "o sinal deveria oscilar");
    }
}

public class SensorFleetTests
{
    [Fact]
    public void Creates_requested_number_of_sensors_with_unique_stable_ids()
    {
        var a = new SensorFleet(50);
        var b = new SensorFleet(50);

        Assert.Equal(50, a.Sensors.Count);
        Assert.Equal(50, a.Sensors.Select(s => s.Id).Distinct().Count());
        Assert.Equal(a.Sensors.Select(s => s.Id), b.Sensors.Select(s => s.Id));
    }

    [Fact]
    public void Distributes_metrics_devices_and_groups()
    {
        var fleet = new SensorFleet(40, sensorsPerDevice: 4, groupCount: 5);

        Assert.Equal(4, fleet.Sensors.Select(s => s.Metric).Distinct().Count());
        Assert.Equal(10, fleet.Sensors.Select(s => s.DeviceId).Distinct().Count());
        Assert.Equal(5, fleet.Sensors.Select(s => s.Group).Distinct().Count());
    }

    [Fact]
    public void Rejects_invalid_counts()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new SensorFleet(0));
    }
}

public class ReadingFactoryTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void NextBatch_returns_exact_count_and_covers_all_sensors_round_robin()
    {
        var factory = new ReadingFactory(new SensorFleet(10));

        var batch = factory.NextBatch(10, T0);

        Assert.Equal(10, batch.Count);
        Assert.Equal(10, batch.Select(r => r.SensorId).Distinct().Count());
    }

    [Fact]
    public void Natural_key_is_unique_even_when_time_does_not_advance()
    {
        var factory = new ReadingFactory(new SensorFleet(3));

        // "agora" congelado: mesmo instante para tudo. A chave (sensor, ts) ainda tem de ser única.
        var all = Enumerable.Range(0, 20).SelectMany(_ => factory.NextBatch(9, T0)).ToList();

        Assert.Equal(all.Count, all.Select(r => (r.SensorId, r.Timestamp)).Distinct().Count());
    }

    [Fact]
    public void Timestamps_are_truncated_to_microseconds()
    {
        var factory = new ReadingFactory(new SensorFleet(2));

        var batch = factory.NextBatch(4, T0.AddTicks(7));

        Assert.All(batch, r => Assert.Equal(0, r.Timestamp.Ticks % 10));
    }

    [Fact]
    public void Timestamps_per_sensor_are_strictly_increasing()
    {
        var factory = new ReadingFactory(new SensorFleet(2));

        var readings = Enumerable.Range(0, 50).SelectMany(_ => factory.NextBatch(4, T0)).ToList();

        foreach (var group in readings.GroupBy(r => r.SensorId))
        {
            var ts = group.Select(r => r.Timestamp).ToList();
            Assert.Equal(ts.OrderBy(t => t).ToList(), ts);
        }
    }

    [Fact]
    public void Duplicates_are_exact_copies_of_earlier_readings()
    {
        var factory = new ReadingFactory(new SensorFleet(5), new ReadingFactoryOptions(DuplicateProbability: 0.3));

        var all = Enumerable.Range(0, 100).SelectMany(i => factory.NextBatch(20, T0.AddSeconds(i))).ToList();

        var distinct = all.Distinct().Count();
        Assert.True(distinct < all.Count, "deveria haver leituras repetidas");
        // duplicatas são cópias exatas: nenhuma chave (sensor, ts) aparece com valores diferentes
        var conflicting = all.GroupBy(r => (r.SensorId, r.Timestamp)).Count(g => g.Select(r => r.Value).Distinct().Count() > 1);
        Assert.Equal(0, conflicting);
    }

    [Fact]
    public void Hot_sensor_emits_proportionally_more()
    {
        var fleet = new SensorFleet(10);
        var factory = new ReadingFactory(fleet, new ReadingFactoryOptions(HotSensorFactor: 10));

        var all = Enumerable.Range(0, 100).SelectMany(i => factory.NextBatch(19, T0.AddSeconds(i))).ToList();
        var hot = all.Count(r => r.SensorId == fleet.Sensors[0].Id);
        var other = all.Count(r => r.SensorId == fleet.Sensors[1].Id);

        Assert.InRange((double)hot / other, 9, 11);
    }

    [Fact]
    public void Silenced_sensors_stop_emitting_and_the_rest_keep_going()
    {
        var fleet = new SensorFleet(20);
        var factory = new ReadingFactory(fleet);
        factory.NextBatch(20, T0); // todos emitem antes

        var victims = factory.SilenceFraction(0.25);
        var after = Enumerable.Range(0, 50).SelectMany(i => factory.NextBatch(20, T0.AddSeconds(i + 1))).ToList();

        Assert.Equal(5, victims.Count);
        Assert.Equal(5, factory.SilencedCount);
        var silencedIds = victims.Select(i => fleet.Sensors[i].Id).ToHashSet();
        Assert.DoesNotContain(after, r => silencedIds.Contains(r.SensorId));
        Assert.Equal(15, after.Select(r => r.SensorId).Distinct().Count()); // os 15 restantes seguem emitindo
    }

    [Fact]
    public void Silencing_never_kills_the_hot_sensor_zero()
    {
        var factory = new ReadingFactory(new SensorFleet(10));

        var victims = factory.SilenceFraction(1.0);

        Assert.DoesNotContain(0, victims);
    }

    [Fact]
    public void A_fleet_of_one_keeps_emitting_because_sensor_zero_is_never_silenced()
    {
        var factory = new ReadingFactory(new SensorFleet(1));
        factory.SilenceFraction(1.0);

        Assert.NotEmpty(factory.NextBatch(3, T0));
    }

    [Theory]
    [InlineData(-0.1)]
    [InlineData(1.1)]
    public void Silence_fraction_must_be_a_valid_probability(double fraction)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ReadingFactory(new SensorFleet(5)).SilenceFraction(fraction));
    }

    [Fact]
    public void Rejects_invalid_options()
    {
        var fleet = new SensorFleet(2);
        Assert.Throws<ArgumentOutOfRangeException>(() => new ReadingFactory(fleet, new ReadingFactoryOptions(HotSensorFactor: 0)));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ReadingFactory(fleet, new ReadingFactoryOptions(DuplicateProbability: 2)));
    }

    [Fact]
    public void Generated_values_satisfy_the_domain_reading_invariants()
    {
        var factory = new ReadingFactory(new SensorFleet(20));

        foreach (var r in factory.NextBatch(200, T0))
            _ = SensorHub.Domain.Readings.Reading.Create(r.SensorId, r.Timestamp, r.Value); // não lança
    }
}

public class LoadRunnerTests
{
    [Fact]
    public async Task Sends_close_to_the_requested_rate()
    {
        var sink = new CountingSink();
        var factory = new ReadingFactory(new SensorFleet(20));

        var report = await new LoadRunner().RunAsync(factory, sink, new LoadOptions(2000, TimeSpan.FromSeconds(1.5), BatchSize: 50));

        Assert.Equal(0, report.Failed);
        Assert.Equal(report.Sent, sink.Count);
        Assert.InRange(report.Sent, 2400, 3600); // 2000/s * 1.5s = 3000, com tolerância de timing
    }

    [Fact]
    public async Task Counts_failures_when_sink_throws()
    {
        var factory = new ReadingFactory(new SensorFleet(5));

        var report = await new LoadRunner().RunAsync(factory, new FailingSink(), new LoadOptions(500, TimeSpan.FromSeconds(0.5), BatchSize: 10));

        Assert.True(report.Failed > 0);
        Assert.Equal(0, report.Sent);
        Assert.False(string.IsNullOrWhiteSpace(report.FirstError)); // o relatório diz POR QUE falhou, não só quantas
    }

    [Fact]
    public async Task Multiple_workers_share_the_target_rate()
    {
        var sink = new CountingSink();
        var factory = new ReadingFactory(new SensorFleet(20));

        var report = await new LoadRunner().RunAsync(factory, sink, new LoadOptions(2000, TimeSpan.FromSeconds(1.5), BatchSize: 50, Workers: 4));

        Assert.InRange(report.Sent, 2400, 3600);
    }

    [Fact]
    public async Task Scheduled_silence_stops_part_of_the_fleet_mid_run()
    {
        var sink = new RecordingSink();
        var fleet = new SensorFleet(20);
        var factory = new ReadingFactory(fleet);

        await new LoadRunner().RunAsync(factory, sink,
            new LoadOptions(2000, TimeSpan.FromSeconds(1.5), BatchSize: 50, SilenceAfter: TimeSpan.FromSeconds(0.5), SilenceFraction: 0.5));

        Assert.Equal(10, factory.SilencedCount);
        var all = sink.Readings;
        var late = all.Where(r => r.Timestamp > all.Min(x => x.Timestamp).AddSeconds(0.9)).ToList();
        Assert.True(late.Select(r => r.SensorId).Distinct().Count() <= 10, "depois do silêncio só metade da frota emite");
    }

    private sealed class RecordingSink : IReadingSink
    {
        private readonly List<SimulatedReading> _all = [];
        public IReadOnlyList<SimulatedReading> Readings { get { lock (_all) return _all.ToList(); } }

        public Task SendAsync(IReadOnlyList<SimulatedReading> batch, CancellationToken cancellationToken)
        {
            lock (_all) _all.AddRange(batch);
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task Cancellation_stops_the_run_early()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));

        var report = await new LoadRunner().RunAsync(
            new ReadingFactory(new SensorFleet(5)), new CountingSink(), new LoadOptions(100, TimeSpan.FromSeconds(30)), cts.Token);

        Assert.True(report.Elapsed < TimeSpan.FromSeconds(5));
    }

    private sealed class FailingSink : IReadingSink
    {
        public Task SendAsync(IReadOnlyList<SimulatedReading> batch, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("boom");
    }
}

public class SimulatorSettingsTests
{
    [Fact]
    public void Parse_uses_defaults()
    {
        var s = SimulatorSettings.Parse([]);

        Assert.Equal("dry-run", s.Mode);
        Assert.Equal(100, s.Sensors);
        Assert.Equal(1000, s.Rate);
    }

    [Fact]
    public void Parse_reads_options()
    {
        var s = SimulatorSettings.Parse(["--sensors", "500", "--rate", "20000", "--duplicates", "0.05", "--mode", "HTTP"]);

        Assert.Equal(500, s.Sensors);
        Assert.Equal(20000, s.Rate);
        Assert.Equal(0.05, s.DuplicateProbability);
        Assert.Equal("http", s.Mode);
    }

    [Fact]
    public void Parse_reads_the_mqtt_options_and_defaults_to_the_compose_port()
    {
        var defaults = SimulatorSettings.Parse(["--mode", "mqtt"]);
        Assert.Equal("localhost", defaults.MqttHost);
        Assert.Equal(1884, defaults.MqttPort);

        var custom = SimulatorSettings.Parse(["--mode", "mqtt", "--mqtt-host", "mosquitto", "--mqtt-port", "1883", "--api-key", "shk_x"]);
        Assert.Equal(("mosquitto", 1883, "shk_x"), (custom.MqttHost, custom.MqttPort, custom.ApiKey));
    }

    [Theory]
    [InlineData("--nope", "1")]
    [InlineData("sensors", "5")]
    public void Parse_rejects_unknown_or_malformed(string a, string b)
    {
        Assert.Throws<ArgumentException>(() => SimulatorSettings.Parse([a, b]));
    }

    [Fact]
    public void Parse_rejects_missing_value()
    {
        Assert.Throws<ArgumentException>(() => SimulatorSettings.Parse(["--rate"]));
    }
}
