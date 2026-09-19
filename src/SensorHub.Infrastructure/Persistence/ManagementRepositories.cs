using Microsoft.EntityFrameworkCore;
using SensorHub.Application.Management;
using SensorHub.Domain.Alerts;
using SensorHub.Domain.Sensors;

namespace SensorHub.Infrastructure.Persistence;

/// <summary>Unit of Work da API: o próprio DbContext (escopo por requisição) é a unidade transacional.</summary>
internal sealed class EfUnitOfWork(SensorHubDbContext db) : IUnitOfWork
{
    public Task<int> SaveChangesAsync(CancellationToken cancellationToken) => db.SaveChangesAsync(cancellationToken);
}

internal sealed class SensorRepository(SensorHubDbContext db) : ISensorRepository
{
    public async Task AddAsync(Sensor sensor, CancellationToken cancellationToken) => await db.Set<Sensor>().AddAsync(sensor, cancellationToken);

    public Task<Sensor?> GetAsync(Guid id, CancellationToken cancellationToken) =>
        db.Set<Sensor>().FirstOrDefaultAsync(s => s.Id == id, cancellationToken);

    public async Task<IReadOnlyList<Sensor>> ListAsync(string? group, bool? active, int skip, int take, CancellationToken cancellationToken)
    {
        var query = db.Set<Sensor>().AsNoTracking();
        if (!string.IsNullOrWhiteSpace(group)) query = query.Where(s => s.Group == group);
        if (active is { } a) query = query.Where(s => s.Active == a);

        return await query.OrderBy(s => s.Name).ThenBy(s => s.Id).Skip(skip).Take(take).ToListAsync(cancellationToken);
    }
}

internal sealed class AlertRuleRepository(SensorHubDbContext db) : IAlertRuleRepository
{
    public async Task AddAsync(AlertRule rule, CancellationToken cancellationToken) => await db.Set<AlertRule>().AddAsync(rule, cancellationToken);

    public Task<AlertRule?> GetAsync(Guid id, CancellationToken cancellationToken) =>
        db.Set<AlertRule>().FirstOrDefaultAsync(r => r.Id == id, cancellationToken);

    public async Task<IReadOnlyList<AlertRule>> ListAsync(Guid? sensorId, CancellationToken cancellationToken)
    {
        var query = db.Set<AlertRule>().AsNoTracking();
        if (sensorId is { } id) query = query.Where(r => r.SensorId == id);
        return await query.OrderBy(r => r.CreatedAt).ToListAsync(cancellationToken);
    }

    public void Remove(AlertRule rule) => db.Set<AlertRule>().Remove(rule);
}

internal sealed class AlertRepository(SensorHubDbContext db) : IAlertRepository
{
    public Task<Alert?> GetAsync(Guid id, CancellationToken cancellationToken) =>
        db.Set<Alert>().FirstOrDefaultAsync(a => a.Id == id, cancellationToken);

    public async Task<IReadOnlyList<Alert>> ListAsync(AlertStatus? status, Guid? sensorId, int skip, int take, CancellationToken cancellationToken)
    {
        var query = db.Set<Alert>().AsNoTracking();
        if (status is { } s) query = query.Where(a => a.Status == s);
        if (sensorId is { } id) query = query.Where(a => a.SensorId == id);

        return await query.OrderByDescending(a => a.FiredAt).Skip(skip).Take(take).ToListAsync(cancellationToken);
    }
}
