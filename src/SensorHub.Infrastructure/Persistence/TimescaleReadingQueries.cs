using Npgsql;
using SensorHub.Application.Queries;

namespace SensorHub.Infrastructure.Persistence;

/// <summary>
/// Consultas de histórico. A série agregada NUNCA lê a tabela bruta: usa os agregados contínuos
/// (<c>readings_1h</c> para buckets de 1 h ou mais, <c>readings_1m</c> para o resto). Ler 1 dia de 1 sensor a
/// 1 leitura/s custaria 86.400 linhas do bruto contra 24 do agregado de 1 h.
/// </summary>
public sealed class TimescaleReadingQueries(NpgsqlDataSource dataSource) : IReadingQueries
{
    public async Task<IReadOnlyList<SeriesPoint>> GetSeriesAsync(SeriesRequest request, CancellationToken cancellationToken)
    {
        // Só usa o agregado horário quando o bucket é múltiplo de 1 h (senão o alinhamento das bordas quebraria).
        var view = request.Bucket >= TimeSpan.FromHours(1) && request.Bucket.Ticks % TimeSpan.TicksPerHour == 0
            ? "readings_1h"
            : "readings_1m";

        // Reagrega pelos SOMA/CONTAGEM guardados: a média correta é sum(sum)/sum(count), nunca a média das médias.
        // O limite inferior é alinhado ao início do bucket para que o primeiro ponto não seja parcial.
        await using var command = dataSource.CreateCommand(
            $"""
            SELECT time_bucket(@bucket, bucket) AS b,
                   sum(sum) / sum(count)        AS average,
                   min(min)                     AS min,
                   max(max)                     AS max,
                   sum(count)::bigint           AS count
            FROM {view}
            WHERE sensor_id = @sensor
              AND bucket >= time_bucket(@bucket, @from)
              AND bucket <  @to
            GROUP BY b
            ORDER BY b
            """);
        command.Parameters.AddWithValue("bucket", request.Bucket);
        command.Parameters.AddWithValue("sensor", request.SensorId);
        command.Parameters.AddWithValue("from", request.From.UtcDateTime);
        command.Parameters.AddWithValue("to", request.To.UtcDateTime);

        var points = new List<SeriesPoint>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            points.Add(new SeriesPoint(
                new DateTimeOffset(reader.GetDateTime(0), TimeSpan.Zero),
                reader.GetDouble(1), reader.GetDouble(2), reader.GetDouble(3), reader.GetInt64(4)));
        }

        return points;
    }

    public async Task<RawPoint?> GetLatestAsync(Guid sensorId, CancellationToken cancellationToken)
    {
        // Sem limite inferior de tempo o planner teria que olhar todos os chunks; o limite de 7 dias
        // (a idade máxima aceita pela ingestão) restringe a busca aos chunks recentes. Sensor mudo há mais
        // de 7 dias é tratado como "sem leitura recente".
        await using var command = dataSource.CreateCommand(
            """
            SELECT ts, value FROM readings
            WHERE sensor_id = @sensor AND ts >= now() - interval '7 days'
            ORDER BY ts DESC
            LIMIT 1
            """);
        command.Parameters.AddWithValue("sensor", sensorId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken)
            ? new RawPoint(new DateTimeOffset(reader.GetDateTime(0), TimeSpan.Zero), reader.GetDouble(1))
            : null;
    }

    public async Task<IReadOnlyList<RawPoint>> GetRawAsync(
        Guid sensorId, DateTimeOffset from, DateTimeOffset to, int limit, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(
            """
            SELECT ts, value
            FROM readings
            WHERE sensor_id = @sensor AND ts >= @from AND ts < @to
            ORDER BY ts
            LIMIT @limit
            """);
        command.Parameters.AddWithValue("sensor", sensorId);
        command.Parameters.AddWithValue("from", from.UtcDateTime);
        command.Parameters.AddWithValue("to", to.UtcDateTime);
        command.Parameters.AddWithValue("limit", limit);

        var points = new List<RawPoint>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            points.Add(new RawPoint(new DateTimeOffset(reader.GetDateTime(0), TimeSpan.Zero), reader.GetDouble(1)));

        return points;
    }
}
