using SensorHub.Domain.Common;
using SensorHub.Domain.Readings;

namespace SensorHub.Domain.Tests.Readings;

public class ReadingTests
{
    private static readonly Guid Sensor = Guid.NewGuid();

    [Fact]
    public void Create_valid_reading_keeps_fields()
    {
        var ts = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

        var reading = Reading.Create(Sensor, ts, 21.5);

        Assert.Equal(Sensor, reading.SensorId);
        Assert.Equal(ts, reading.Timestamp);
        Assert.Equal(21.5, reading.Value);
    }

    [Fact]
    public void Create_rejects_empty_sensor_id()
    {
        Assert.Throws<DomainException>(() => Reading.Create(Guid.Empty, DateTimeOffset.UtcNow, 1));
    }

    [Theory]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    [InlineData(double.NegativeInfinity)]
    public void Create_rejects_non_finite_values(double value)
    {
        Assert.Throws<DomainException>(() => Reading.Create(Sensor, DateTimeOffset.UtcNow, value));
    }

    [Fact]
    public void Create_normalizes_timestamp_to_utc()
    {
        var local = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.FromHours(-3));

        var reading = Reading.Create(Sensor, local, 1);

        Assert.Equal(TimeSpan.Zero, reading.Timestamp.Offset);
        Assert.Equal(local.UtcDateTime, reading.Timestamp.UtcDateTime);
    }

    [Fact]
    public void Create_truncates_to_microseconds_so_natural_key_matches_postgres_precision()
    {
        // 100ns de diferença: distintos em ticks, mas iguais no timestamptz do Postgres (microssegundos).
        var a = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero).AddTicks(1_000_001);
        var b = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero).AddTicks(1_000_009);

        var ra = Reading.Create(Sensor, a, 1);
        var rb = Reading.Create(Sensor, b, 1);

        Assert.Equal(ra.Timestamp, rb.Timestamp);
        Assert.Equal(0, ra.Timestamp.Ticks % 10);
    }

    [Fact]
    public void Readings_with_same_fields_are_equal()
    {
        var ts = DateTimeOffset.UtcNow;
        Assert.Equal(Reading.Create(Sensor, ts, 3), Reading.Create(Sensor, ts, 3));
    }
}
