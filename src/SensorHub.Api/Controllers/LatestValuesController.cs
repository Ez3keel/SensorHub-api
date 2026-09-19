using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.RateLimiting;
using SensorHub.Api.Security;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using SensorHub.Application.LatestValues;

namespace SensorHub.Api.Controllers;

/// <summary>Último valor conhecido de cada sensor: leitura O(1) no Redis, com cache-aside no banco.</summary>
[ApiController]
[Route("api/sensors")]
[Authorize(Policy = Policies.Viewer)]
[EnableRateLimiting(Policies.ApiLimiter)]
public sealed class LatestValuesController(LastValueService lastValues, TimeProvider clock) : ControllerBase
{
    public const int MaxIdsPerRequest = 200;

    /// <summary>Último valor de UM sensor. 404 se o sensor não tem leitura recente (últimos 7 dias).</summary>
    [HttpGet("{sensorId:guid}/latest")]
    [ProducesResponseType(typeof(LatestDto), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetLatest(Guid sensorId, CancellationToken cancellationToken)
    {
        var latest = await lastValues.GetAsync(sensorId, cancellationToken);
        return latest is null ? NotFound() : Ok(ToDto(latest.Value));
    }

    /// <summary>
    /// Último valor de VÁRIOS sensores numa única chamada (o dashboard inicial): <c>?ids=a,b,c</c>.
    /// Sensores sem leitura recente simplesmente não aparecem na resposta.
    /// </summary>
    [HttpGet("latest")]
    [ProducesResponseType(typeof(IReadOnlyList<LatestDto>), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ValidationProblemDetails), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> GetLatestMany([FromQuery] string? ids, CancellationToken cancellationToken)
    {
        var parsed = new List<Guid>();
        foreach (var part in (ids ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (!Guid.TryParse(part, out var id))
                return Problem($"'{part}' não é um GUID válido.", statusCode: StatusCodes.Status400BadRequest);
            parsed.Add(id);
        }

        if (parsed.Count == 0)
            return Problem("Informe ao menos um id em ?ids=a,b,c.", statusCode: StatusCodes.Status400BadRequest);
        if (parsed.Count > MaxIdsPerRequest)
            return Problem($"No máximo {MaxIdsPerRequest} ids por requisição.", statusCode: StatusCodes.Status400BadRequest);

        var found = await lastValues.GetManyAsync(parsed, cancellationToken);
        return Ok(found.Values.OrderBy(v => v.SensorId).Select(ToDto).ToList());
    }

    private LatestDto ToDto(LastValue value) =>
        new(value.SensorId, value.Timestamp, value.Value, Math.Max(0, (clock.GetUtcNow() - value.Timestamp).TotalSeconds));

    /// <summary><c>AgeSeconds</c> permite ao cliente decidir se o valor está "velho" sem comparar relógios.</summary>
    public sealed record LatestDto(Guid SensorId, DateTimeOffset Timestamp, double Value, double AgeSeconds);
}
