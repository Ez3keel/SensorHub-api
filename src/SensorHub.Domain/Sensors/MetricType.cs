namespace SensorHub.Domain.Sensors;

public enum MetricType
{
    Temperature = 1,
    Humidity = 2,
    Vibration = 3,
    Pressure = 4
}

public static class MetricTypeExtensions
{
    /// <summary>
    /// Faixa fisicamente plausível para a métrica. Serve para descartar lixo (sensor com defeito
    /// enviando 1e30) na borda, antes de poluir o histórico e as regras de alerta.
    /// </summary>
    public static (double Min, double Max) PlausibleRange(this MetricType metric) => metric switch
    {
        MetricType.Temperature => (-273.15, 2000),
        MetricType.Humidity => (0, 100),
        MetricType.Vibration => (0, 1000),   // mm/s RMS
        MetricType.Pressure => (0, 100_000), // hPa
        _ => throw new ArgumentOutOfRangeException(nameof(metric), metric, "Métrica desconhecida.")
    };

    public static bool IsPlausible(this MetricType metric, double value)
    {
        var (min, max) = metric.PlausibleRange();
        return double.IsFinite(value) && value >= min && value <= max;
    }
}
