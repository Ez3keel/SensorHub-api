using System.Net;
using System.Net.Http.Json;
using SensorHub.Domain.Sensors;

namespace SensorHub.Simulator.Load;

public sealed record RegistrationReport(int SensorsCreated, int SensorsAlreadyExisted, int RulesCreated, int Failures);

/// <summary>
/// Cadastra a frota sintética na API de administração com os MESMOS ids que o simulador usa nas leituras
/// (por isso o cadastro aceita um id explícito). É idempotente: rodar de novo pula o que já existe (409).
/// </summary>
public sealed class FleetRegistrar(HttpClient http)
{
    /// <summary>Limite de "alta" por métrica, acima do qual o sinal sintético só chega com os picos raros do gerador.</summary>
    private static double HighLimit(MetricType metric) => metric switch
    {
        MetricType.Temperature => 78,
        MetricType.Humidity => 90,
        MetricType.Vibration => 15,
        MetricType.Pressure => 1040,
        _ => throw new ArgumentOutOfRangeException(nameof(metric))
    };

    public async Task<RegistrationReport> RegisterAsync(
        SensorFleet fleet, bool thresholdRules, bool noDataRules, int noDataSeconds, int parallelism = 8, CancellationToken cancellationToken = default)
    {
        int created = 0, existed = 0, rules = 0, failures = 0;

        await Parallel.ForEachAsync(fleet.Sensors, new ParallelOptions { MaxDegreeOfParallelism = parallelism, CancellationToken = cancellationToken },
            async (sensor, ct) =>
            {
                var response = await http.PostAsJsonAsync("api/sensors", new
                {
                    id = sensor.Id, deviceId = sensor.DeviceId, name = sensor.Name,
                    metric = sensor.Metric.ToString(), unit = sensor.Unit, group = sensor.Group
                }, ct);

                if (response.StatusCode == HttpStatusCode.Created) Interlocked.Increment(ref created);
                else if (response.StatusCode == HttpStatusCode.Conflict) { Interlocked.Increment(ref existed); return; } // já cadastrado (e suas regras também)
                else { Interlocked.Increment(ref failures); return; }

                if (thresholdRules && await PostRuleAsync(sensor.Id, $"{sensor.Metric} alta", "Threshold", "GreaterThan", HighLimit(sensor.Metric), 0, ct))
                    Interlocked.Increment(ref rules);
                else if (thresholdRules) Interlocked.Increment(ref failures);

                if (noDataRules && await PostRuleAsync(sensor.Id, "Sensor offline", "NoData", "GreaterThan", 0, noDataSeconds, ct))
                    Interlocked.Increment(ref rules);
                else if (noDataRules) Interlocked.Increment(ref failures);
            });

        return new RegistrationReport(created, existed, rules, failures);
    }

    private async Task<bool> PostRuleAsync(Guid sensorId, string name, string type, string comparison, double threshold, int durationSeconds, CancellationToken ct)
    {
        var response = await http.PostAsJsonAsync("api/alert-rules", new
        {
            sensorId, name, type, comparison, threshold, durationSeconds, hysteresis = 0, severity = type == "NoData" ? "Warning" : "Critical"
        }, ct);
        return response.StatusCode == HttpStatusCode.Created;
    }
}
