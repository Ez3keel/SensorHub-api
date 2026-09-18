using System.Net;
using System.Net.Http.Json;
using System.Text.Json;

namespace SensorHub.Simulator.Load;

/// <summary>
/// Envia lotes para <c>POST /api/readings/batch</c>. Se a API responder 503 (backpressure) ou a rede
/// falhar, reenvia o MESMO lote com backoff, como um dispositivo bem comportado faria. É seguro porque
/// a persistência é idempotente por (sensor_id, ts).
/// </summary>
public sealed class HttpReadingSink : IReadingSink
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    private readonly HttpClient _http;
    private readonly int _maxRetries;
    private long _retries;

    public HttpReadingSink(HttpClient http, string? apiKey = null, int maxRetries = 3)
    {
        _http = http;
        _maxRetries = maxRetries;
        if (!string.IsNullOrEmpty(apiKey))
            _http.DefaultRequestHeaders.TryAddWithoutValidation("X-Api-Key", apiKey);
    }

    /// <summary>Quantas vezes um lote precisou ser reenviado (mede o quanto o backpressure atuou).</summary>
    public long Retries => Interlocked.Read(ref _retries);

    public async Task SendAsync(IReadOnlyList<SimulatedReading> batch, CancellationToken cancellationToken)
    {
        var payload = batch.Select(r => new { sensorId = r.SensorId, timestamp = r.Timestamp, value = r.Value, unit = r.Unit });

        for (var attempt = 0; ; attempt++)
        {
            HttpResponseMessage? response = null;
            try
            {
                response = await _http.PostAsJsonAsync("api/readings/batch", payload, Json, cancellationToken);

                if (response.IsSuccessStatusCode) return;

                // 4xx (exceto 429) não melhora repetindo: falha definitiva
                var retryable = response.StatusCode is HttpStatusCode.ServiceUnavailable or HttpStatusCode.TooManyRequests
                                || (int)response.StatusCode >= 500;
                if (!retryable || attempt >= _maxRetries)
                    throw new HttpRequestException($"API respondeu {(int)response.StatusCode}.", null, response.StatusCode);

                var delay = response.Headers.RetryAfter?.Delta ?? TimeSpan.FromMilliseconds(200 * Math.Pow(2, attempt));
                Interlocked.Increment(ref _retries);
                await Task.Delay(delay, cancellationToken);
            }
            catch (HttpRequestException) when (attempt < _maxRetries && response is null)
            {
                Interlocked.Increment(ref _retries);
                await Task.Delay(TimeSpan.FromMilliseconds(200 * Math.Pow(2, attempt)), cancellationToken);
            }
            finally
            {
                response?.Dispose();
            }
        }
    }
}
