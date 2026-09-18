using Microsoft.AspNetCore.Mvc;
using SensorHub.Application.Queries;

namespace SensorHub.Api.Controllers;

/// <summary>Consulta do histórico de um sensor: série agregada (para gráficos) e leituras brutas (para auditoria).</summary>
[ApiController]
[Route("api/sensors/{sensorId:guid}")]
public sealed class SeriesController(ReadingQueryService queries) : ControllerBase
{
    /// <summary>
    /// Série agregada por janela de tempo. <c>bucket</c>: auto (padrão), 1m, 5m, 15m, 30m, 1h, 6h ou 1d.
    /// Buckets diários e horários são alinhados ao UTC. Lê dos agregados contínuos, não do dado bruto.
    /// </summary>
    [HttpGet("series")]
    [ProducesResponseType(typeof(SeriesResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> GetSeries(
        Guid sensorId, [FromQuery] DateTimeOffset? from, [FromQuery] DateTimeOffset? to, [FromQuery] string? bucket,
        CancellationToken cancellationToken)
    {
        var points = await queries.GetSeriesAsync(sensorId, from, to, bucket, cancellationToken);

        return Ok(new SeriesResponse(
            sensorId,
            points.Select(p => new SeriesPointDto(p.Bucket, Math.Round(p.Average, 4), p.Min, p.Max, p.Count)).ToList()));
    }

    /// <summary>Leituras brutas em ordem cronológica (padrão: últimos 10 min, até 1000 pontos).</summary>
    [HttpGet("readings")]
    [ProducesResponseType(typeof(RawResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> GetRaw(
        Guid sensorId, [FromQuery] DateTimeOffset? from, [FromQuery] DateTimeOffset? to, [FromQuery] int? limit,
        CancellationToken cancellationToken)
    {
        var points = await queries.GetRawAsync(sensorId, from, to, limit, cancellationToken);

        return Ok(new RawResponse(sensorId, points.Select(p => new RawPointDto(p.Timestamp, p.Value)).ToList()));
    }

    public sealed record SeriesPointDto(DateTimeOffset Bucket, double Avg, double Min, double Max, long Count);
    public sealed record SeriesResponse(Guid SensorId, IReadOnlyList<SeriesPointDto> Points);
    public sealed record RawPointDto(DateTimeOffset Ts, double Value);
    public sealed record RawResponse(Guid SensorId, IReadOnlyList<RawPointDto> Points);
}
