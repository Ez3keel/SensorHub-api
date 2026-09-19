using Microsoft.EntityFrameworkCore;
using Npgsql;
using SensorHub.Application.Alerting;
using SensorHub.Domain.Alerts;

namespace SensorHub.Infrastructure.Persistence;

/// <summary>
/// Persistência dos alertas gerados pelo motor. Singleton: usa <see cref="IDbContextFactory{TContext}"/> para
/// criar um contexto curto por operação (o DbContext não é thread-safe nem deve viver em singleton).
/// </summary>
public sealed class PostgresAlertStore(IDbContextFactory<SensorHubDbContext> contexts) : IAlertStore
{
    private const string PrimaryKeyConstraint = "PK_alerts";
    private const string OneOpenPerRuleIndex = "ux_alerts_one_open_per_rule";

    public async Task<InsertOutcome> InsertFiredAsync(Alert alert, CancellationToken cancellationToken)
    {
        await using var db = await contexts.CreateDbContextAsync(cancellationToken);
        db.Set<Alert>().Add(alert);

        try
        {
            await db.SaveChangesAsync(cancellationToken);
            return InsertOutcome.Created;
        }
        catch (DbUpdateException ex) when (ex.InnerException is PostgresException { SqlState: PostgresErrorCodes.UniqueViolation } pg)
        {
            if (pg.ConstraintName == PrimaryKeyConstraint) return InsertOutcome.AlreadyExists;    // reprocessamento do MESMO disparo
            if (pg.ConstraintName == OneOpenPerRuleIndex) return InsertOutcome.OpenAlertExists;   // já há outro alerta aberto para a regra
            throw;
        }
    }

    public async Task<Alert?> ResolveOpenAsync(Guid ruleId, DateTimeOffset at, double? value, CancellationToken cancellationToken)
    {
        await using var db = await contexts.CreateDbContextAsync(cancellationToken);

        // O alerta aberto da regra OU o que já foi resolvido exatamente neste instante (reprocessamento).
        var alert = await db.Set<Alert>()
            .Where(a => a.RuleId == ruleId && (a.Status != AlertStatus.Resolved || a.ResolvedAt == at))
            .OrderByDescending(a => a.FiredAt)
            .FirstOrDefaultAsync(cancellationToken);

        if (alert is null) return null;

        if (alert.Resolve(at, value))
            await db.SaveChangesAsync(cancellationToken);

        return alert;
    }
}
