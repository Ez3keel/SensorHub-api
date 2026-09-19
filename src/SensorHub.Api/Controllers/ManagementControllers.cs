using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.RateLimiting;
using SensorHub.Api.Security;
using Microsoft.AspNetCore.Mvc;
using SensorHub.Application.Management;
using SensorHub.Domain.Alerts;
using SensorHub.Domain.Sensors;

namespace SensorHub.Api.Controllers;

public sealed record SensorResponse(Guid Id, Guid DeviceId, string Name, MetricType Metric, string Unit, string Group, bool Active, DateTimeOffset CreatedAt)
{
    public static SensorResponse From(Sensor s) => new(s.Id, s.DeviceId, s.Name, s.Metric, s.Unit, s.Group, s.Active, s.CreatedAt);
}

public sealed record RuleResponse(
    Guid Id, Guid SensorId, string Name, RuleType Type, Comparison Comparison, double Threshold,
    int DurationSeconds, double Hysteresis, Severity Severity, bool Enabled, DateTimeOffset CreatedAt)
{
    public static RuleResponse From(AlertRule r) =>
        new(r.Id, r.SensorId, r.Name, r.Type, r.Comparison, r.Threshold, (int)r.Duration.TotalSeconds, r.Hysteresis, r.Severity, r.Enabled, r.CreatedAt);
}

public sealed record AlertResponse(
    Guid Id, Guid RuleId, Guid SensorId, Severity Severity, AlertStatus Status, string Message, double? TriggerValue,
    DateTimeOffset FiredAt, DateTimeOffset? AcknowledgedAt, string? AcknowledgedBy, DateTimeOffset? ResolvedAt, double? ResolvedValue)
{
    public static AlertResponse From(Alert a) =>
        new(a.Id, a.RuleId, a.SensorId, a.Severity, a.Status, a.Message, a.TriggerValue, a.FiredAt, a.AcknowledgedAt, a.AcknowledgedBy, a.ResolvedAt, a.ResolvedValue);
}

/// <summary>Cadastro de sensores (canais de medição).</summary>
[ApiController]
[Route("api/sensors")]
[Authorize(Policy = Policies.Viewer)]
[EnableRateLimiting(Policies.ApiLimiter)]
public sealed class SensorsController(SensorService sensors) : ControllerBase
{
    /// <param name="Id">Opcional: preserva a identidade de um sensor já existente (importação, simulador).</param>
    public sealed record CreateSensorRequest(Guid DeviceId, string Name, MetricType Metric, string Unit, string Group, Guid? Id = null);
    public sealed record SetActiveRequest(bool Active);

    [HttpPost]
    [Authorize(Policy = Policies.Admin)]
    [ProducesResponseType(typeof(SensorResponse), StatusCodes.Status201Created)]
    public async Task<IActionResult> Create([FromBody] CreateSensorRequest request, CancellationToken cancellationToken)
    {
        var sensor = await sensors.CreateAsync(request.DeviceId, request.Name, request.Metric, request.Unit, request.Group, cancellationToken, request.Id);
        return CreatedAtAction(nameof(GetById), new { id = sensor.Id }, SensorResponse.From(sensor));
    }

    [HttpGet]
    public async Task<IActionResult> List(
        [FromQuery] string? group, [FromQuery] bool? active, [FromQuery] int skip = 0, [FromQuery] int take = 100,
        CancellationToken cancellationToken = default) =>
        Ok((await sensors.ListAsync(group, active, skip, take, cancellationToken)).Select(SensorResponse.From).ToList());

    [HttpGet("{id:guid}")]
    public async Task<IActionResult> GetById(Guid id, CancellationToken cancellationToken) =>
        Ok(SensorResponse.From(await sensors.GetAsync(id, cancellationToken)));

    [HttpPatch("{id:guid}/active")]
    [Authorize(Policy = Policies.Admin)]
    public async Task<IActionResult> SetActive(Guid id, [FromBody] SetActiveRequest request, CancellationToken cancellationToken) =>
        Ok(SensorResponse.From(await sensors.SetActiveAsync(id, request.Active, cancellationToken)));
}

/// <summary>Regras de alerta. Mudanças valem no motor em até <c>Alerting:RuleCacheSeconds</c> (cache de regras).</summary>
[ApiController]
[Route("api/alert-rules")]
[Authorize(Policy = Policies.Viewer)]
[EnableRateLimiting(Policies.ApiLimiter)]
public sealed class AlertRulesController(AlertRuleService rules) : ControllerBase
{
    public sealed record CreateRuleRequest(
        Guid SensorId, string Name, RuleType Type, Comparison Comparison, double Threshold,
        int DurationSeconds, double Hysteresis, Severity Severity);

    public sealed record SetEnabledRequest(bool Enabled);

    [HttpPost]
    [Authorize(Policy = Policies.Admin)]
    [ProducesResponseType(typeof(RuleResponse), StatusCodes.Status201Created)]
    public async Task<IActionResult> Create([FromBody] CreateRuleRequest r, CancellationToken cancellationToken)
    {
        var rule = await rules.CreateAsync(
            r.SensorId, r.Name, r.Type, r.Comparison, r.Threshold, TimeSpan.FromSeconds(r.DurationSeconds), r.Hysteresis, r.Severity, cancellationToken);
        return CreatedAtAction(nameof(List), new { sensorId = rule.SensorId }, RuleResponse.From(rule));
    }

    [HttpGet]
    public async Task<IActionResult> List([FromQuery] Guid? sensorId, CancellationToken cancellationToken) =>
        Ok((await rules.ListAsync(sensorId, cancellationToken)).Select(RuleResponse.From).ToList());

    [HttpPatch("{id:guid}/enabled")]
    [Authorize(Policy = Policies.Admin)]
    public async Task<IActionResult> SetEnabled(Guid id, [FromBody] SetEnabledRequest request, CancellationToken cancellationToken) =>
        Ok(RuleResponse.From(await rules.SetEnabledAsync(id, request.Enabled, cancellationToken)));

    [HttpDelete("{id:guid}")]
    [Authorize(Policy = Policies.Admin)]
    public async Task<IActionResult> Delete(Guid id, CancellationToken cancellationToken)
    {
        await rules.DeleteAsync(id, cancellationToken);
        return NoContent();
    }
}

/// <summary>Alertas gerados pelo motor: consulta e reconhecimento.</summary>
[ApiController]
[Route("api/alerts")]
[Authorize(Policy = Policies.Viewer)]
[EnableRateLimiting(Policies.ApiLimiter)]
public sealed class AlertsController(AlertService alerts) : ControllerBase
{
    public sealed record AcknowledgeRequest(string User);

    [HttpGet]
    public async Task<IActionResult> List(
        [FromQuery] AlertStatus? status, [FromQuery] Guid? sensorId, [FromQuery] int skip = 0, [FromQuery] int take = 100,
        CancellationToken cancellationToken = default) =>
        Ok((await alerts.ListAsync(status, sensorId, skip, take, cancellationToken)).Select(AlertResponse.From).ToList());

    [HttpGet("{id:guid}")]
    public async Task<IActionResult> GetById(Guid id, CancellationToken cancellationToken) =>
        Ok(AlertResponse.From(await alerts.GetAsync(id, cancellationToken)));

    [HttpPost("{id:guid}/acknowledge")]
    [Authorize(Policy = Policies.Operator)]
    public async Task<IActionResult> Acknowledge(Guid id, [FromBody] AcknowledgeRequest request, CancellationToken cancellationToken) =>
        Ok(AlertResponse.From(await alerts.AcknowledgeAsync(id, request.User, cancellationToken)));
}
