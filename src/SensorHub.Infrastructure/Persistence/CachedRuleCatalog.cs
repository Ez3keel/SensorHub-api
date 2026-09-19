using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using SensorHub.Application.Alerting;
using SensorHub.Domain.Alerts;

namespace SensorHub.Infrastructure.Persistence;

public sealed class AlertingOptions
{
    public const string SectionName = "Alerting";

    /// <summary>
    /// Validade do cache de regras em memória. O motor consulta as regras a CADA lote; ir ao banco a cada lote seria
    /// o gargalo. Preço: uma regra nova ou desabilitada leva até este tempo para valer (consistência eventual).
    /// </summary>
    public int RuleCacheSeconds { get; set; } = 10;

    /// <summary>Intervalo da varredura das regras "sem dados".</summary>
    public int NoDataSweepSeconds { get; set; } = 15;

    /// <summary>Se o motor está mais atrasado que isto no fluxo, a varredura "sem dados" se abstém.</summary>
    public int MaxProcessingDelaySeconds { get; set; } = 30;
}

/// <summary>Regras habilitadas em memória, recarregadas do banco no máximo a cada <c>RuleCacheSeconds</c>.</summary>
public sealed class CachedRuleCatalog(
    IDbContextFactory<SensorHubDbContext> contexts, IOptions<AlertingOptions> options, TimeProvider clock) : IRuleCatalog
{
    private readonly SemaphoreSlim _refresh = new(1, 1);
    private Snapshot? _snapshot;

    private sealed record Snapshot(DateTimeOffset LoadedAt, ILookup<Guid, AlertRule> BySensor, IReadOnlyList<AlertRule> NoData);

    public async Task<IReadOnlyList<AlertRule>> GetEnabledRulesForSensorAsync(Guid sensorId, CancellationToken cancellationToken) =>
        (await GetSnapshotAsync(cancellationToken)).BySensor[sensorId].ToList();

    public async Task<IReadOnlyList<AlertRule>> GetEnabledNoDataRulesAsync(CancellationToken cancellationToken) =>
        (await GetSnapshotAsync(cancellationToken)).NoData;

    private async Task<Snapshot> GetSnapshotAsync(CancellationToken cancellationToken)
    {
        var ttl = TimeSpan.FromSeconds(options.Value.RuleCacheSeconds);
        if (_snapshot is { } fresh && clock.GetUtcNow() - fresh.LoadedAt < ttl) return fresh;

        await _refresh.WaitAsync(cancellationToken);
        try
        {
            // outro chamador pode ter recarregado enquanto esperávamos o semáforo
            if (_snapshot is { } again && clock.GetUtcNow() - again.LoadedAt < ttl) return again;

            await using var db = await contexts.CreateDbContextAsync(cancellationToken);
            var rules = await db.Set<AlertRule>().AsNoTracking().Where(r => r.Enabled).ToListAsync(cancellationToken);

            _snapshot = new Snapshot(
                clock.GetUtcNow(),
                rules.ToLookup(r => r.SensorId),
                rules.Where(r => r.Type == RuleType.NoData).ToList());
            return _snapshot;
        }
        finally
        {
            _refresh.Release();
        }
    }
}
