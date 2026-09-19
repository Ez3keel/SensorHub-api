using System.Net;
using System.Net.Http.Json;
using SensorHub.Domain.Sensors;

namespace SensorHub.Simulator.Load;

public sealed record RegistrationReport(
    int SensorsCreated, int SensorsAlreadyExisted, int RulesCreated, int Failures,
    int DevicesCreated = 0, int DevicesAlreadyExisted = 0, IReadOnlyDictionary<Guid, string>? ApiKeys = null);

/// <summary>Credenciais de administrador para a API de administração (com a segurança ligada).</summary>
public sealed record AdminCredentials(string Email, string Password);

/// <summary>
/// Cadastra a frota sintética na API de administração com os MESMOS ids que o simulador usa nas leituras
/// (por isso o cadastro aceita um id explícito). É idempotente: rodar de novo pula o que já existe (409).
/// Cria primeiro os dispositivos (cada sensor pertence a um) e devolve as chaves de API geradas: a chave só existe
/// em texto puro na resposta da criação, então um dispositivo que já existia (409) não tem chave a devolver.
/// </summary>
public sealed class FleetRegistrar(HttpClient http, AdminCredentials? admin = null)
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

        if (admin is not null) await LoginAsync(admin, cancellationToken);

        var (devicesCreated, devicesExisted, keys, deviceFailures) = await EnsureDevicesAsync(fleet, cancellationToken);
        if (deviceFailures > 0) // sem dispositivo, todo cadastro de sensor falharia com 404: melhor parar e dizer o motivo
            return new RegistrationReport(0, 0, 0, deviceFailures, devicesCreated, devicesExisted, keys);

        await Parallel.ForEachAsync(fleet.Sensors, new ParallelOptions { MaxDegreeOfParallelism = parallelism, CancellationToken = cancellationToken },
            async (sensor, ct) =>
            {
                var response = await PostAsync("api/sensors", new
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

        return new RegistrationReport(created, existed, rules, failures, devicesCreated, devicesExisted, keys);
    }

    /// <summary>POST que respeita o limite da API: em 429 espera o <c>Retry-After</c> (ou 1 s) e repete, em vez de contar falha.</summary>
    private async Task<HttpResponseMessage> PostAsync(string url, object body, CancellationToken ct)
    {
        for (var attempt = 0; ; attempt++)
        {
            var response = await http.PostAsJsonAsync(url, body, ct);
            if (response.StatusCode != HttpStatusCode.TooManyRequests || attempt >= 30) return response;

            var wait = response.Headers.RetryAfter?.Delta ?? TimeSpan.FromSeconds(1);
            response.Dispose();
            await Task.Delay(wait > TimeSpan.Zero ? wait : TimeSpan.FromSeconds(1), ct);
        }
    }

    private async Task LoginAsync(AdminCredentials credentials, CancellationToken ct)
    {
        var response = await PostAsync("api/auth/login", new { email = credentials.Email, password = credentials.Password }, ct);
        if (!response.IsSuccessStatusCode)
            throw new HttpRequestException($"Login de administrador falhou ({(int)response.StatusCode}).", null, response.StatusCode);

        using var doc = await System.Text.Json.JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync(ct), cancellationToken: ct);
        var token = doc.RootElement.GetProperty("accessToken").GetString();
        http.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);
    }

    private async Task<(int Created, int Existed, Dictionary<Guid, string> Keys, int Failures)> EnsureDevicesAsync(SensorFleet fleet, CancellationToken ct)
    {
        int created = 0, existed = 0, failures = 0;
        var keys = new Dictionary<Guid, string>();

        foreach (var deviceId in fleet.Sensors.Select(s => s.DeviceId).Distinct())
        {
            var response = await PostAsync("api/devices", new { id = deviceId, name = $"sim-{deviceId.ToString("N")[..8]}" }, ct);
            if (response.StatusCode == HttpStatusCode.Created)
            {
                created++;
                var key = await TryReadKeyAsync(response, ct);
                if (key is not null) keys[deviceId] = key;
            }
            else if (response.StatusCode == HttpStatusCode.Conflict) existed++;
            else failures++;
        }

        return (created, existed, keys, failures);
    }

    private static async Task<string?> TryReadKeyAsync(HttpResponseMessage response, CancellationToken ct)
    {
        try
        {
            using var doc = await System.Text.Json.JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync(ct), cancellationToken: ct);
            return doc.RootElement.TryGetProperty("apiKey", out var key) ? key.GetString() : null;
        }
        catch (System.Text.Json.JsonException) { return null; }
    }

    private async Task<bool> PostRuleAsync(Guid sensorId, string name, string type, string comparison, double threshold, int durationSeconds, CancellationToken ct)
    {
        var response = await PostAsync("api/alert-rules", new
        {
            sensorId, name, type, comparison, threshold, durationSeconds, hysteresis = 0, severity = type == "NoData" ? "Warning" : "Critical"
        }, ct);
        return response.StatusCode == HttpStatusCode.Created;
    }
}
