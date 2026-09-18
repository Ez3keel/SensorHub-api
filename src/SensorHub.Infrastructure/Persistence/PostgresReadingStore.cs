using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using SensorHub.Application.Processing;
using SensorHub.Domain.Readings;

namespace SensorHub.Infrastructure.Persistence;

/// <summary>
/// Grava lotes de leituras no PostgreSQL/TimescaleDB. Duas estratégias de bulk insert (comparadas em
/// benchmark, ver ADR Fase 2), ambas idempotentes por <c>(sensor_id, ts)</c> via ON CONFLICT DO NOTHING.
/// </summary>
public sealed class PostgresReadingStore(NpgsqlDataSource dataSource, IOptions<PersistenceOptions> options) : IReadingStore
{
    private readonly BulkInsertStrategy _strategy = options.Value.Strategy;
    private readonly int _timeout = options.Value.CommandTimeoutSeconds;

    public Task<int> InsertBatchAsync(IReadOnlyList<Reading> readings, CancellationToken cancellationToken)
    {
        if (readings.Count == 0) return Task.FromResult(0);

        // Ordenar por chave melhora a localidade no índice (menos páginas tocadas) e evita deadlocks
        // entre transações concorrentes que escrevem chaves sobrepostas em ordens diferentes.
        var sorted = readings.OrderBy(r => r.SensorId).ThenBy(r => r.Timestamp).ToArray();

        return _strategy switch
        {
            BulkInsertStrategy.Unnest => InsertWithUnnestAsync(sorted, cancellationToken),
            _ => InsertWithCopyStagingAsync(sorted, cancellationToken)
        };
    }

    private async Task<int> InsertWithCopyStagingAsync(Reading[] readings, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);

        await using (var create = new NpgsqlCommand(
            "CREATE TEMP TABLE readings_stage (sensor_id uuid NOT NULL, ts timestamptz NOT NULL, value double precision NOT NULL) ON COMMIT DROP",
            connection, transaction))
        {
            await create.ExecuteNonQueryAsync(ct);
        }

        await using (var writer = await connection.BeginBinaryImportAsync(
            "COPY readings_stage (sensor_id, ts, value) FROM STDIN (FORMAT BINARY)", ct))
        {
            foreach (var r in readings)
            {
                await writer.StartRowAsync(ct);
                await writer.WriteAsync(r.SensorId, NpgsqlDbType.Uuid, ct);
                await writer.WriteAsync(r.Timestamp.UtcDateTime, NpgsqlDbType.TimestampTz, ct);
                await writer.WriteAsync(r.Value, NpgsqlDbType.Double, ct);
            }

            await writer.CompleteAsync(ct);
        }

        int inserted;
        await using (var insert = new NpgsqlCommand(
            """
            INSERT INTO readings (sensor_id, ts, value)
            SELECT sensor_id, ts, value FROM readings_stage
            ON CONFLICT (sensor_id, ts) DO NOTHING
            """, connection, transaction) { CommandTimeout = _timeout })
        {
            inserted = await insert.ExecuteNonQueryAsync(ct);
        }

        await transaction.CommitAsync(ct);
        return inserted;
    }

    private async Task<int> InsertWithUnnestAsync(Reading[] readings, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand(
            """
            INSERT INTO readings (sensor_id, ts, value)
            SELECT * FROM unnest(@ids, @ts, @values)
            ON CONFLICT (sensor_id, ts) DO NOTHING
            """);
        command.CommandTimeout = _timeout;

        command.Parameters.Add(new NpgsqlParameter<Guid[]>("ids", NpgsqlDbType.Array | NpgsqlDbType.Uuid) { TypedValue = readings.Select(r => r.SensorId).ToArray() });
        command.Parameters.Add(new NpgsqlParameter<DateTime[]>("ts", NpgsqlDbType.Array | NpgsqlDbType.TimestampTz) { TypedValue = readings.Select(r => r.Timestamp.UtcDateTime).ToArray() });
        command.Parameters.Add(new NpgsqlParameter<double[]>("values", NpgsqlDbType.Array | NpgsqlDbType.Double) { TypedValue = readings.Select(r => r.Value).ToArray() });

        return await command.ExecuteNonQueryAsync(ct);
    }
}
