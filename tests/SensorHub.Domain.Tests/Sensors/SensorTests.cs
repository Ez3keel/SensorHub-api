using SensorHub.Domain.Common;
using SensorHub.Domain.Sensors;

namespace SensorHub.Domain.Tests.Sensors;

public class SensorTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Create_sets_defaults_and_trims_text()
    {
        var sensor = Sensor.Create(Guid.NewGuid(), "  Forno 1  ", MetricType.Temperature, "°C", " fabrica-1 ", Now);

        Assert.NotEqual(Guid.Empty, sensor.Id);
        Assert.Equal("Forno 1", sensor.Name);
        Assert.Equal("fabrica-1", sensor.Group);
        Assert.True(sensor.Active);
        Assert.Equal(Now, sensor.CreatedAt);
    }

    [Fact]
    public void Create_preserves_an_explicit_id_and_generates_one_otherwise()
    {
        var wanted = Guid.NewGuid();

        Assert.Equal(wanted, Sensor.Create(Guid.NewGuid(), "s", MetricType.Humidity, "%", "g", Now, wanted).Id);
        Assert.NotEqual(Guid.Empty, Sensor.Create(Guid.NewGuid(), "s", MetricType.Humidity, "%", "g", Now, Guid.Empty).Id);
        Assert.NotEqual(Guid.Empty, Sensor.Create(Guid.NewGuid(), "s", MetricType.Humidity, "%", "g", Now).Id);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Create_requires_name(string name)
    {
        Assert.Throws<DomainException>(() =>
            Sensor.Create(Guid.NewGuid(), name, MetricType.Humidity, "%", "g", Now));
    }

    [Fact]
    public void Create_rejects_empty_device_and_unknown_metric()
    {
        Assert.Throws<DomainException>(() => Sensor.Create(Guid.Empty, "s", MetricType.Humidity, "%", "g", Now));
        Assert.Throws<DomainException>(() => Sensor.Create(Guid.NewGuid(), "s", (MetricType)99, "%", "g", Now));
    }

    [Fact]
    public void Create_rejects_overlong_name()
    {
        Assert.Throws<DomainException>(() =>
            Sensor.Create(Guid.NewGuid(), new string('x', 101), MetricType.Humidity, "%", "g", Now));
    }

    [Fact]
    public void Deactivate_and_activate_toggle_state()
    {
        var sensor = Sensor.Create(Guid.NewGuid(), "s", MetricType.Pressure, "hPa", "g", Now);

        sensor.Deactivate();
        Assert.False(sensor.Active);

        sensor.Activate();
        Assert.True(sensor.Active);
    }

    [Theory]
    [InlineData(MetricType.Temperature, 25, true)]
    [InlineData(MetricType.Temperature, -300, false)]
    [InlineData(MetricType.Temperature, 1e30, false)]
    [InlineData(MetricType.Humidity, 100, true)]
    [InlineData(MetricType.Humidity, 100.1, false)]
    [InlineData(MetricType.Humidity, -1, false)]
    [InlineData(MetricType.Vibration, 0, true)]
    [InlineData(MetricType.Vibration, -0.1, false)]
    [InlineData(MetricType.Pressure, 1013, true)]
    public void AcceptsValue_enforces_plausible_range(MetricType metric, double value, bool expected)
    {
        var sensor = Sensor.Create(Guid.NewGuid(), "s", metric, "u", "g", Now);

        Assert.Equal(expected, sensor.AcceptsValue(value));
    }

    [Fact]
    public void AcceptsValue_rejects_nan()
    {
        Assert.False(MetricType.Temperature.IsPlausible(double.NaN));
    }
}
