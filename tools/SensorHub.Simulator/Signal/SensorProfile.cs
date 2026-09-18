using SensorHub.Domain.Sensors;

namespace SensorHub.Simulator.Signal;

/// <summary>Parâmetros do sinal sintético de um sensor: senoide + ruído gaussiano + picos raros.</summary>
public sealed record SensorProfile(
    MetricType Metric,
    string Unit,
    double Baseline,
    double Amplitude,
    TimeSpan Period,
    double NoiseStdDev,
    double SpikeProbability,
    double SpikeMagnitude)
{
    public static SensorProfile For(MetricType metric) => metric switch
    {
        MetricType.Temperature => new(metric, "°C", Baseline: 60, Amplitude: 10, TimeSpan.FromMinutes(10), NoiseStdDev: 0.5, SpikeProbability: 0.0005, SpikeMagnitude: 30),
        MetricType.Humidity => new(metric, "%", 50, 20, TimeSpan.FromMinutes(30), 1.0, 0.0002, 25),
        MetricType.Vibration => new(metric, "mm/s", 5, 2, TimeSpan.FromMinutes(5), 0.3, 0.001, 25),
        MetricType.Pressure => new(metric, "hPa", 1013, 5, TimeSpan.FromMinutes(60), 0.5, 0.0001, 40),
        _ => throw new ArgumentOutOfRangeException(nameof(metric), metric, null)
    };
}
