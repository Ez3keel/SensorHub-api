using SensorHub.Domain.Sensors;
using SensorHub.Simulator.Signal;

namespace SensorHub.Simulator.Load;

public sealed record SimulatedSensor(Guid Id, Guid DeviceId, string Name, MetricType Metric, string Unit, string Group, SensorProfile Profile);

/// <summary>
/// Frota de sensores sintéticos. Os IDs são determinísticos (derivados do índice): duas execuções
/// do simulador falam dos MESMOS sensores, então dá para cadastrá-los uma vez e reusar nas cargas.
/// </summary>
public sealed class SensorFleet
{
    private static readonly MetricType[] Metrics =
        [MetricType.Temperature, MetricType.Humidity, MetricType.Vibration, MetricType.Pressure];

    public IReadOnlyList<SimulatedSensor> Sensors { get; }

    public SensorFleet(int sensorCount, int sensorsPerDevice = 4, int groupCount = 5)
    {
        if (sensorCount <= 0) throw new ArgumentOutOfRangeException(nameof(sensorCount));
        if (sensorsPerDevice <= 0) throw new ArgumentOutOfRangeException(nameof(sensorsPerDevice));
        if (groupCount <= 0) throw new ArgumentOutOfRangeException(nameof(groupCount));

        Sensors = Enumerable.Range(0, sensorCount)
            .Select(i =>
            {
                var metric = Metrics[i % Metrics.Length];
                var profile = SensorProfile.For(metric);
                return new SimulatedSensor(
                    Id: StableGuid(0x5E, i),
                    DeviceId: StableGuid(0xDE, i / sensorsPerDevice),
                    Name: $"sensor-{i:D5}-{metric.ToString().ToLowerInvariant()}",
                    Metric: metric,
                    Unit: profile.Unit,
                    Group: $"planta-{i % groupCount + 1}",
                    Profile: profile);
            })
            .ToList();
    }

    public static Guid StableGuid(byte kind, int index)
    {
        var bytes = new byte[16];
        bytes[0] = kind;
        BitConverter.GetBytes(index).CopyTo(bytes, 4);
        bytes[15] = 0x01;
        return new Guid(bytes);
    }
}
