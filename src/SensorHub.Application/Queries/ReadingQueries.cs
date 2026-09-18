using System.Text.RegularExpressions;

namespace SensorHub.Application.Queries;

/// <summary>Um ponto agregado da série: estatísticas de um intervalo (bucket) de tempo.</summary>
public readonly record struct SeriesPoint(DateTimeOffset Bucket, double Average, double Min, double Max, long Count);

public readonly record struct RawPoint(DateTimeOffset Timestamp, double Value);

public sealed record SeriesRequest(Guid SensorId, DateTimeOffset From, DateTimeOffset To, TimeSpan Bucket);

/// <summary>Porta de leitura do histórico (séries temporais).</summary>
public interface IReadingQueries
{
    /// <summary>Série agregada (média/min/max/contagem por bucket), lida dos agregados contínuos.</summary>
    Task<IReadOnlyList<SeriesPoint>> GetSeriesAsync(SeriesRequest request, CancellationToken cancellationToken);

    /// <summary>Leituras brutas de um sensor, em ordem cronológica.</summary>
    Task<IReadOnlyList<RawPoint>> GetRawAsync(Guid sensorId, DateTimeOffset from, DateTimeOffset to, int limit, CancellationToken cancellationToken);

    /// <summary>A leitura mais recente de um sensor (fonte da verdade para o cache-aside do último valor).</summary>
    Task<RawPoint?> GetLatestAsync(Guid sensorId, CancellationToken cancellationToken);
}

/// <summary>Parâmetro de consulta inválido (vira HTTP 400).</summary>
public sealed class QueryValidationException(string message) : Exception(message);

public sealed class QueryLimits
{
    public const int MaxSeriesPoints = 5_000;
    public const int MaxRawPoints = 10_000;
    public const int DefaultRawPoints = 1_000;
    public static readonly TimeSpan MaxRange = TimeSpan.FromDays(366);
}

/// <summary>
/// Regras da consulta de histórico. Existe para proteger o banco: sem limite de pontos, um
/// <c>from=2020&amp;bucket=1m</c> devolveria milhões de linhas e derrubaria API e banco com uma única requisição.
/// </summary>
public sealed partial class ReadingQueryService(IReadingQueries queries, TimeProvider clock)
{
    /// <summary>Buckets suportados: múltiplos de 1 minuto, a granularidade do agregado contínuo mais fino.</summary>
    public static readonly IReadOnlyList<TimeSpan> AllowedBuckets =
    [
        TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(15), TimeSpan.FromMinutes(30),
        TimeSpan.FromHours(1), TimeSpan.FromHours(6), TimeSpan.FromDays(1)
    ];

    private const int AutoTargetPoints = 500;

    public async Task<IReadOnlyList<SeriesPoint>> GetSeriesAsync(
        Guid sensorId, DateTimeOffset? from, DateTimeOffset? to, string? bucket, CancellationToken cancellationToken)
    {
        var (start, end) = ResolveRange(from, to, TimeSpan.FromHours(1));
        var size = ResolveBucket(bucket, end - start);

        var points = (end - start).Ticks / size.Ticks;
        if (points > QueryLimits.MaxSeriesPoints)
            throw new QueryValidationException(
                $"A consulta geraria {points} pontos (máximo {QueryLimits.MaxSeriesPoints}). Use um bucket maior ou um intervalo menor.");

        return await queries.GetSeriesAsync(new SeriesRequest(sensorId, start, end, size), cancellationToken);
    }

    public async Task<IReadOnlyList<RawPoint>> GetRawAsync(
        Guid sensorId, DateTimeOffset? from, DateTimeOffset? to, int? limit, CancellationToken cancellationToken)
    {
        var (start, end) = ResolveRange(from, to, TimeSpan.FromMinutes(10));
        var max = limit ?? QueryLimits.DefaultRawPoints;
        if (max is < 1 or > QueryLimits.MaxRawPoints)
            throw new QueryValidationException($"limit deve estar entre 1 e {QueryLimits.MaxRawPoints}.");

        return await queries.GetRawAsync(sensorId, start, end, max, cancellationToken);
    }

    private (DateTimeOffset Start, DateTimeOffset End) ResolveRange(DateTimeOffset? from, DateTimeOffset? to, TimeSpan defaultWindow)
    {
        var end = (to ?? clock.GetUtcNow()).ToUniversalTime();
        var start = (from ?? end - defaultWindow).ToUniversalTime();

        if (start >= end) throw new QueryValidationException("'from' deve ser anterior a 'to'.");
        if (end - start > QueryLimits.MaxRange) throw new QueryValidationException($"O intervalo máximo é {QueryLimits.MaxRange.TotalDays:0} dias.");
        return (start, end);
    }

    /// <summary>"auto" (ou omitido) escolhe o menor bucket que mantém a série em ~500 pontos: ideal para desenhar gráfico.</summary>
    private static TimeSpan ResolveBucket(string? bucket, TimeSpan range)
    {
        if (string.IsNullOrWhiteSpace(bucket) || bucket.Equals("auto", StringComparison.OrdinalIgnoreCase))
            return AllowedBuckets.FirstOrDefault(b => range.Ticks / b.Ticks <= AutoTargetPoints, AllowedBuckets[^1]);

        if (!TryParseBucket(bucket, out var parsed) || !AllowedBuckets.Contains(parsed))
            throw new QueryValidationException(
                $"bucket inválido '{bucket}'. Use: auto, {string.Join(", ", AllowedBuckets.Select(FormatBucket))}.");

        return parsed;
    }

    public static bool TryParseBucket(string text, out TimeSpan bucket)
    {
        bucket = default;
        var match = BucketPattern().Match(text.Trim());
        if (!match.Success) return false;

        var amount = int.Parse(match.Groups[1].Value);
        bucket = match.Groups[2].Value.ToLowerInvariant() switch
        {
            "m" => TimeSpan.FromMinutes(amount),
            "h" => TimeSpan.FromHours(amount),
            _ => TimeSpan.FromDays(amount)
        };
        return true;
    }

    public static string FormatBucket(TimeSpan bucket) =>
        bucket.TotalDays >= 1 && bucket.Ticks % TimeSpan.TicksPerDay == 0 ? $"{bucket.TotalDays:0}d"
        : bucket.TotalHours >= 1 && bucket.Ticks % TimeSpan.TicksPerHour == 0 ? $"{bucket.TotalHours:0}h"
        : $"{bucket.TotalMinutes:0}m";

    [GeneratedRegex(@"^(\d{1,3})([mhdMHD])$")]
    private static partial Regex BucketPattern();
}
