using Microsoft.Extensions.Diagnostics.HealthChecks;
using StackExchange.Redis;

namespace SensorHub.Infrastructure.Redis;

/// <summary>Readiness do Redis (PING). O Redis guarda só estado derivado, mas sem ele o dashboard perde o "agora".</summary>
public sealed class RedisHealthCheck(IConnectionMultiplexer redis) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            var latency = await redis.GetDatabase().PingAsync();
            return HealthCheckResult.Healthy($"PING {latency.TotalMilliseconds:F1} ms");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Redis inacessível", ex);
        }
    }
}
