using SensorHub.Application.Alerting;
using SensorHub.Application.Realtime;
using SensorHub.Domain.Alerts;
using SensorHub.Domain.Readings;

namespace SensorHub.Application.Tests.Realtime;

public class ReadingCoalescerTests
{
    private static readonly DateTimeOffset T0 = new(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);
    private static Reading R(Guid sensor, int seconds, double value) => Reading.Create(sensor, T0.AddSeconds(seconds), value);

    [Fact]
    public void Many_readings_of_a_sensor_collapse_into_the_newest_one()
    {
        var sensor = Guid.NewGuid();
        var coalescer = new ReadingCoalescer();

        coalescer.AddRange(Enumerable.Range(0, 1000).Select(i => R(sensor, i, i)));

        var drained = Assert.Single(coalescer.Drain());
        Assert.Equal(999, drained.Value);
    }

    [Fact]
    public void Late_or_redelivered_readings_never_replace_a_newer_one()
    {
        var sensor = Guid.NewGuid();
        var coalescer = new ReadingCoalescer();

        coalescer.Add(R(sensor, 10, 100));
        coalescer.Add(R(sensor, 5, 50));    // atrasada
        coalescer.Add(R(sensor, 10, 999));  // mesmo instante (reentrega)

        Assert.Equal(100, coalescer.Drain().Single().Value);
    }

    [Fact]
    public void Different_sensors_are_kept_apart()
    {
        var coalescer = new ReadingCoalescer();
        var sensors = Enumerable.Range(0, 50).Select(_ => Guid.NewGuid()).ToList();

        coalescer.AddRange(sensors.SelectMany(s => Enumerable.Range(0, 10).Select(i => R(s, i, i))));

        Assert.Equal(50, coalescer.Pending);
        var drained = coalescer.Drain();
        Assert.Equal(50, drained.Count);
        Assert.All(drained, r => Assert.Equal(9, r.Value));
    }

    [Fact]
    public void Drain_empties_the_buffer_and_a_second_drain_is_empty()
    {
        var coalescer = new ReadingCoalescer();
        coalescer.Add(R(Guid.NewGuid(), 1, 1));

        Assert.Single(coalescer.Drain());
        Assert.Empty(coalescer.Drain());
        Assert.Equal(0, coalescer.Pending);
    }

    [Fact]
    public async Task Concurrent_writers_and_a_draining_reader_never_lose_the_final_value()
    {
        var sensors = Enumerable.Range(0, 20).Select(_ => Guid.NewGuid()).ToList();
        var coalescer = new ReadingCoalescer();
        var seen = new System.Collections.Concurrent.ConcurrentDictionary<Guid, double>();
        using var stop = new CancellationTokenSource();

        var reader = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                foreach (var r in coalescer.Drain()) seen.AddOrUpdate(r.SensorId, r.Value, (_, old) => Math.Max(old, r.Value));
                await Task.Delay(1);
            }
        });

        var writers = sensors.Select(s => Task.Run(() =>
        {
            for (var i = 0; i < 2000; i++) coalescer.Add(R(s, i, i));
        })).ToList();
        await Task.WhenAll(writers);
        await Task.Delay(50);
        stop.Cancel();
        await reader;
        foreach (var r in coalescer.Drain()) seen.AddOrUpdate(r.SensorId, r.Value, (_, old) => Math.Max(old, r.Value));

        Assert.All(sensors, s => Assert.Equal(1999, seen[s])); // o último valor de cada sensor sempre chega
    }
}

public class RecentEventFilterTests
{
    private static AlertEvent Event(Guid id, AlertEventKind kind) =>
        new(1, id, Guid.NewGuid(), Guid.NewGuid(), kind, Severity.Critical, DateTimeOffset.UtcNow, 1, "m");

    [Fact]
    public void The_same_event_is_accepted_once()
    {
        var filter = new RecentEventFilter();
        var id = Guid.NewGuid();

        Assert.True(filter.TryAdd(Event(id, AlertEventKind.Fired)));
        Assert.False(filter.TryAdd(Event(id, AlertEventKind.Fired))); // republicado pelo motor após uma queda
    }

    [Fact]
    public void Fired_and_resolved_of_the_same_alert_are_different_events()
    {
        var filter = new RecentEventFilter();
        var id = Guid.NewGuid();

        Assert.True(filter.TryAdd(Event(id, AlertEventKind.Fired)));
        Assert.True(filter.TryAdd(Event(id, AlertEventKind.Resolved)));
    }

    [Fact]
    public void Memory_is_bounded_and_the_oldest_entries_are_forgotten()
    {
        var filter = new RecentEventFilter(capacity: 100);
        var first = Guid.NewGuid();
        filter.TryAdd(Event(first, AlertEventKind.Fired));

        for (var i = 0; i < 200; i++) filter.TryAdd(Event(Guid.NewGuid(), AlertEventKind.Fired));

        Assert.True(filter.TryAdd(Event(first, AlertEventKind.Fired))); // já foi expulso da janela recente
    }
}
