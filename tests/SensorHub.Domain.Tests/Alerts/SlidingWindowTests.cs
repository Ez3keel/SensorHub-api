using SensorHub.Domain.Alerts;

namespace SensorHub.Domain.Tests.Alerts;

public class SlidingWindowTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

    private static SlidingWindow Window() => new(TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(3));

    [Fact]
    public void Empty_window_has_no_stats()
    {
        Assert.Null(Window().Stats());
    }

    [Fact]
    public void Aggregates_count_sum_average_min_max()
    {
        var w = Window();
        w.Add(T0, 10);
        w.Add(T0.AddSeconds(1), 20);
        w.Add(T0.AddSeconds(10), 30);

        var s = w.Stats()!.Value;

        Assert.Equal(3, s.Count);
        Assert.Equal(60, s.Sum);
        Assert.Equal(20, s.Average);
        Assert.Equal(10, s.Min);
        Assert.Equal(30, s.Max);
        Assert.Equal(10, s.OldestValue);
        Assert.Equal(30, s.LatestValue);
        Assert.Equal(TimeSpan.FromSeconds(10), s.Coverage);
    }

    [Fact]
    public void Evicts_points_that_left_the_window()
    {
        var w = Window();
        w.Add(T0, 1000);                     // sai da janela
        w.Add(T0.AddMinutes(2), 5);
        w.Add(T0.AddMinutes(2).AddSeconds(1), 7);

        var s = w.Stats()!.Value;

        Assert.Equal(2, s.Count);
        Assert.Equal(6, s.Average);
        Assert.Equal(5, s.Min);
    }

    [Fact]
    public void Rejects_points_older_than_the_window()
    {
        var w = Window();
        w.Add(T0.AddMinutes(5), 1);

        var accepted = w.Add(T0, 999);

        Assert.False(accepted);
        Assert.Equal(1, w.Stats()!.Value.Count);
    }

    [Fact]
    public void Accepts_out_of_order_points_still_inside_the_window()
    {
        var w = Window();
        w.Add(T0.AddSeconds(30), 30);
        w.Add(T0.AddSeconds(10), 10); // chegou atrasado, mas dentro da janela

        var s = w.Stats()!.Value;

        Assert.Equal(2, s.Count);
        Assert.Equal(10, s.OldestValue);
        Assert.Equal(30, s.LatestValue);
    }

    [Fact]
    public void Latest_and_oldest_within_same_bucket_follow_timestamps_not_arrival_order()
    {
        var w = Window();
        w.Add(T0.AddSeconds(2), 200);
        w.Add(T0.AddSeconds(1), 100); // mesmo balde (3s), chegou depois porém é mais antigo

        var s = w.Stats()!.Value;

        Assert.Equal(100, s.OldestValue);
        Assert.Equal(200, s.LatestValue);
    }

    [Fact]
    public void Memory_is_bounded_by_bucket_count_regardless_of_sample_rate()
    {
        var w = Window();
        for (var i = 0; i < 100_000; i++)
            w.Add(T0.AddMilliseconds(i), i);

        Assert.True(w.Buckets.Count <= 21, $"buckets: {w.Buckets.Count}");
    }

    [Fact]
    public void Survives_json_roundtrip_so_state_can_live_in_redis()
    {
        var w = Window();
        w.Add(T0, 10);
        w.Add(T0.AddSeconds(20), 30);

        var json = System.Text.Json.JsonSerializer.Serialize(w);
        var back = System.Text.Json.JsonSerializer.Deserialize<SlidingWindow>(json)!;

        Assert.Equal(w.Stats(), back.Stats());
        back.Add(T0.AddSeconds(30), 50);
        Assert.Equal(3, back.Stats()!.Value.Count);
    }

    [Fact]
    public void Constructor_validates_arguments()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new SlidingWindow(TimeSpan.Zero, TimeSpan.FromSeconds(1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => new SlidingWindow(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2)));
    }
}
