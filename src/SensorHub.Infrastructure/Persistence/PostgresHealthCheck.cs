using Microsoft.Extensions.Diagnostics.HealthChecks;
using Npgsql;

namespace SensorHub.Infrastructure.Persistence;

/// <summary>Readiness: o banco responde e a tabela de leituras é uma hypertable (a migration da Fase 3 foi aplicada).</summary>
public sealed class PostgresHealthCheck(NpgsqlDataSource dataSource) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var command = dataSource.CreateCommand(
                "SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'readings'");
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(3));

            var hypertables = (long)(await command.ExecuteScalarAsync(timeout.Token))!;
            return hypertables == 1
                ? HealthCheckResult.Healthy("PostgreSQL/TimescaleDB ok")
                : HealthCheckResult.Unhealthy("tabela readings não é uma hypertable (migrations pendentes?)");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("PostgreSQL inacessível", ex);
        }
    }
}
