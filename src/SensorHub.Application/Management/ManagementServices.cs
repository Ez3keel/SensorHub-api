using SensorHub.Application.Alerting;
using SensorHub.Domain.Alerts;
using SensorHub.Domain.Sensors;

namespace SensorHub.Application.Management;

/// <summary>Recurso inexistente (vira HTTP 404).</summary>
public sealed class NotFoundException(string entity, Guid id) : Exception($"{entity} '{id}' não encontrado(a).");

/// <summary>Conflito de estado ou de regra de negócio (vira HTTP 409).</summary>
public sealed class ConflictException(string message) : Exception(message);

public interface IUnitOfWork
{
    Task<int> SaveChangesAsync(CancellationToken cancellationToken);
}

public interface ISensorRepository
{
    Task AddAsync(Sensor sensor, CancellationToken cancellationToken);
    Task<Sensor?> GetAsync(Guid id, CancellationToken cancellationToken);
    Task<IReadOnlyList<Sensor>> ListAsync(string? group, bool? active, int skip, int take, CancellationToken cancellationToken);
}

public interface IAlertRuleRepository
{
    Task AddAsync(AlertRule rule, CancellationToken cancellationToken);
    Task<AlertRule?> GetAsync(Guid id, CancellationToken cancellationToken);
    Task<IReadOnlyList<AlertRule>> ListAsync(Guid? sensorId, CancellationToken cancellationToken);
    void Remove(AlertRule rule);
}

public interface IAlertRepository
{
    Task<Alert?> GetAsync(Guid id, CancellationToken cancellationToken);
    Task<IReadOnlyList<Alert>> ListAsync(AlertStatus? status, Guid? sensorId, int skip, int take, CancellationToken cancellationToken);
}

public sealed class SensorService(ISensorRepository sensors, IUnitOfWork unitOfWork, TimeProvider clock)
{
    public const int MaxPageSize = 500;

    public async Task<Sensor> CreateAsync(
        Guid deviceId, string name, MetricType metric, string unit, string group, CancellationToken cancellationToken, Guid? id = null)
    {
        if (id is { } wanted && wanted != Guid.Empty && await sensors.GetAsync(wanted, cancellationToken) is not null)
            throw new ConflictException($"Já existe um sensor com o id '{wanted}'.");

        var sensor = Sensor.Create(deviceId, name, metric, unit, group, clock.GetUtcNow(), id);
        await sensors.AddAsync(sensor, cancellationToken);
        await unitOfWork.SaveChangesAsync(cancellationToken);
        return sensor;
    }

    public async Task<Sensor> GetAsync(Guid id, CancellationToken cancellationToken) =>
        await sensors.GetAsync(id, cancellationToken) ?? throw new NotFoundException("Sensor", id);

    public Task<IReadOnlyList<Sensor>> ListAsync(string? group, bool? active, int skip, int take, CancellationToken cancellationToken) =>
        sensors.ListAsync(group, active, Math.Max(0, skip), Math.Clamp(take, 1, MaxPageSize), cancellationToken);

    public async Task<Sensor> SetActiveAsync(Guid id, bool active, CancellationToken cancellationToken)
    {
        var sensor = await GetAsync(id, cancellationToken);
        if (active) sensor.Activate(); else sensor.Deactivate();
        await unitOfWork.SaveChangesAsync(cancellationToken);
        return sensor;
    }
}

public sealed class AlertRuleService(
    IAlertRuleRepository rules, ISensorRepository sensors, IRuleStateStore states, IUnitOfWork unitOfWork, TimeProvider clock)
{
    public async Task<AlertRule> CreateAsync(
        Guid sensorId, string name, RuleType type, Comparison comparison, double threshold, TimeSpan duration,
        double hysteresis, Severity severity, CancellationToken cancellationToken)
    {
        _ = await sensors.GetAsync(sensorId, cancellationToken) ?? throw new NotFoundException("Sensor", sensorId);

        var rule = AlertRule.Create(sensorId, name, type, comparison, threshold, duration, hysteresis, severity, clock.GetUtcNow());
        await rules.AddAsync(rule, cancellationToken);
        await unitOfWork.SaveChangesAsync(cancellationToken);
        return rule;
    }

    public Task<IReadOnlyList<AlertRule>> ListAsync(Guid? sensorId, CancellationToken cancellationToken) =>
        rules.ListAsync(sensorId, cancellationToken);

    public async Task<AlertRule> SetEnabledAsync(Guid id, bool enabled, CancellationToken cancellationToken)
    {
        var rule = await rules.GetAsync(id, cancellationToken) ?? throw new NotFoundException("Regra", id);
        if (enabled) rule.Enable(); else rule.Disable();
        await unitOfWork.SaveChangesAsync(cancellationToken);

        // Desabilitar descarta o estado: ao reabilitar, a regra recomeça limpa (Normal), sem herdar uma janela antiga.
        if (!enabled) await states.DeleteAsync(id, cancellationToken);
        return rule;
    }

    public async Task DeleteAsync(Guid id, CancellationToken cancellationToken)
    {
        var rule = await rules.GetAsync(id, cancellationToken) ?? throw new NotFoundException("Regra", id);
        rules.Remove(rule);
        await unitOfWork.SaveChangesAsync(cancellationToken);
        await states.DeleteAsync(id, cancellationToken);
    }
}

public sealed class AlertService(IAlertRepository alerts, IUnitOfWork unitOfWork, TimeProvider clock)
{
    public const int MaxPageSize = 500;

    public Task<IReadOnlyList<Alert>> ListAsync(AlertStatus? status, Guid? sensorId, int skip, int take, CancellationToken cancellationToken) =>
        alerts.ListAsync(status, sensorId, Math.Max(0, skip), Math.Clamp(take, 1, MaxPageSize), cancellationToken);

    public async Task<Alert> GetAsync(Guid id, CancellationToken cancellationToken) =>
        await alerts.GetAsync(id, cancellationToken) ?? throw new NotFoundException("Alerta", id);

    public async Task<Alert> AcknowledgeAsync(Guid id, string user, CancellationToken cancellationToken)
    {
        var alert = await GetAsync(id, cancellationToken);
        try
        {
            alert.Acknowledge(user, clock.GetUtcNow());
        }
        catch (Domain.Common.DomainException ex) when (alert.Status != AlertStatus.Firing)
        {
            throw new ConflictException(ex.Message); // já reconhecido/resolvido: conflito de estado, não dado inválido
        }

        await unitOfWork.SaveChangesAsync(cancellationToken);
        return alert;
    }
}
