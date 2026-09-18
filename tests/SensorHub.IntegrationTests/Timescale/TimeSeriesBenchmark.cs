using System.Diagnostics;
using Npgsql;
using NpgsqlTypes;
using SensorHub.IntegrationTests.Infrastructure;
using Xunit.Abstractions;

namespace SensorHub.IntegrationTests.Timescale;

/// <summary>Só roda com <c>SENSORHUB_BENCH=1</c>: leva minutos e não deve pesar na suíte normal.</summary>
public sealed class BenchmarkFactAttribute : FactAttribute
{
    public BenchmarkFactAttribute()
    {
        if (Environment.GetEnvironmentVariable("SENSORHUB_BENCH") != "1")
            Skip = "Benchmark opt-in: defina SENSORHUB_BENCH=1 para executar.";
    }
}

/// <summary>
/// Compara o mesmo esquema, a mesma carga e as mesmas consultas em 3 variantes:
/// (1) tabela Postgres comum, (2) particionamento declarativo nativo por hora, (3) hypertable TimescaleDB
/// (chunks de 1 h). Mesmo caminho de escrita (unnest + ON CONFLICT DO NOTHING) nas três.
/// Resultado documentado no ADR (Fase 3).
/// </summary>
[Collection(PlatformCollection.Name)]
public class TimeSeriesBenchmark(PlatformFixture platform, ITestOutputHelper output)
{
    private const int Sensors = 500;
    private const int Steps = 6_000;            // 500 x 6000 = 3.000.000 linhas
    private const int StepSeconds = 30;         // 6000 x 30 s = 50 h de dados
    private const int BatchRows = 5_000;
    private static readonly DateTimeOffset Start = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private sealed record Variant(string Name, string Table);

    [BenchmarkFact]
    public async Task Compare_plain_postgres_native_partitioning_and_timescaledb()
    {
        var sensors = Enumerable.Range(0, Sensors).Select(_ => Guid.NewGuid()).ToArray();
        var probe = sensors[Sensors / 2];
        var hoursOfData = Steps * StepSeconds / 3600;

        var variants = new[]
        {
            new Variant("Postgres puro", "bench_plain"),
            new Variant("Particionamento nativo (1 h)", "bench_part"),
            new Variant("TimescaleDB (chunks 1 h)", "bench_hyper")
        };

        await CreateTablesAsync(hoursOfData);

        output.WriteLine($"Carga: {Sensors} sensores x {Steps} passos = {(long)Sensors * Steps:N0} linhas, {hoursOfData} h de dados, lotes de {BatchRows}");
        output.WriteLine("");

        // ---------------------------------------------------------------- ingestão
        var insertRates = new Dictionary<string, (double Overall, double Last10)>();
        foreach (var v in variants)
            insertRates[v.Name] = await LoadAsync(v.Table, sensors);

        // ---------------------------------------------------------------- consultas
        var t1 = Start.AddHours(hoursOfData - 1);
        var t2 = Start.AddHours(hoursOfData);
        var queries = new (string Label, string Sql, Func<NpgsqlCommand, string, Task> Bind)[]
        {
            ("Q1 1 sensor, ultima hora (bruto)",
                "SELECT ts, value FROM {t} WHERE sensor_id = @s AND ts >= @a AND ts < @b ORDER BY ts",
                (c, _) => { c.Parameters.AddWithValue("s", probe); c.Parameters.AddWithValue("a", t1.UtcDateTime); c.Parameters.AddWithValue("b", t2.UtcDateTime); return Task.CompletedTask; }),
            ("Q2 1 sensor, 50 h, media/min/max por hora",
                "SELECT date_trunc('hour', ts) h, avg(value), min(value), max(value) FROM {t} WHERE sensor_id = @s GROUP BY h ORDER BY h",
                (c, _) => { c.Parameters.AddWithValue("s", probe); return Task.CompletedTask; }),
            ("Q3 todos os sensores, ultima hora, media",
                "SELECT sensor_id, avg(value) FROM {t} WHERE ts >= @a AND ts < @b GROUP BY sensor_id",
                (c, _) => { c.Parameters.AddWithValue("a", t1.UtcDateTime); c.Parameters.AddWithValue("b", t2.UtcDateTime); return Task.CompletedTask; }),
            ("Q4 todos os sensores, 50 h, media (varredura total)",
                "SELECT avg(value), min(value), max(value), count(*) FROM {t}",
                (_, _) => Task.CompletedTask)
        };

        var queryMs = new Dictionary<(string, string), double>();
        foreach (var v in variants)
            foreach (var q in queries)
                queryMs[(v.Name, q.Label)] = await MedianMsAsync(v.Table, q.Sql, q.Bind, runs: 15);

        // Q2 via agregado contínuo (só o Timescale tem): o mesmo resultado sem tocar o dado bruto
        await ExecAsync("""
            CREATE MATERIALIZED VIEW bench_hyper_1h WITH (timescaledb.continuous) AS
            SELECT sensor_id, time_bucket(INTERVAL '1 hour', ts) AS bucket, sum(value) AS sum, count(*) AS count, min(value) AS min, max(value) AS max
            FROM bench_hyper GROUP BY sensor_id, time_bucket(INTERVAL '1 hour', ts) WITH NO DATA
            """);
        await ExecAsync("CALL refresh_continuous_aggregate('bench_hyper_1h', NULL, NULL)");
        var caggMs = await MedianMsAsync("bench_hyper_1h",
            "SELECT bucket, sum / count, min, max FROM {t} WHERE sensor_id = @s ORDER BY bucket",
            (c, _) => { c.Parameters.AddWithValue("s", probe); return Task.CompletedTask; }, runs: 15);

        // ---------------------------------------------------------------- armazenamento
        var sizes = new Dictionary<string, long>();
        foreach (var v in variants) sizes[v.Name] = await SizeAsync(v.Table);

        await ExecAsync("ALTER TABLE bench_hyper SET (timescaledb.enable_columnstore = true, timescaledb.segmentby = 'sensor_id', timescaledb.orderby = 'ts DESC')");
        await ExecAsync("SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('bench_hyper') c");
        var compressedSize = await SizeAsync("bench_hyper");
        var q2Compressed = await MedianMsAsync("bench_hyper", queries[1].Sql, queries[1].Bind, runs: 15);
        var q1Compressed = await MedianMsAsync("bench_hyper", queries[0].Sql, queries[0].Bind, runs: 15);

        // ---------------------------------------------------------------- retenção (apagar as primeiras 24 h)
        var cutoff = Start.AddHours(24).UtcDateTime;
        var retention = new Dictionary<string, double>();
        {
            var sw = Stopwatch.StartNew();
            await ExecAsync($"DELETE FROM bench_plain WHERE ts < '{cutoff:O}'");
            retention["Postgres puro"] = sw.Elapsed.TotalMilliseconds;
        }
        {
            var sw = Stopwatch.StartNew();
            for (var h = 0; h < 24; h++) await ExecAsync($"DROP TABLE bench_part_{h:D3}");
            retention["Particionamento nativo (1 h)"] = sw.Elapsed.TotalMilliseconds;
        }
        {
            var sw = Stopwatch.StartNew();
            await ExecAsync($"SELECT drop_chunks('bench_hyper', older_than => '{cutoff:O}'::timestamptz)");
            retention["TimescaleDB (chunks 1 h)"] = sw.Elapsed.TotalMilliseconds;
        }

        // ---------------------------------------------------------------- relatório
        output.WriteLine("== Ingestão (linhas/s) ==");
        foreach (var v in variants)
            output.WriteLine($"  {v.Name,-32} total {insertRates[v.Name].Overall,9:N0}   ultimos 10% {insertRates[v.Name].Last10,9:N0}");

        output.WriteLine("");
        output.WriteLine("== Consultas (mediana de 15 execucoes, ms) ==");
        output.WriteLine($"  {"",-58} {variants[0].Name,-15} {"Part. nativo",-15} {"Timescale",-15}");
        foreach (var q in queries)
            output.WriteLine($"  {q.Label,-58} {queryMs[(variants[0].Name, q.Label)],-15:F1} {queryMs[(variants[1].Name, q.Label)],-15:F1} {queryMs[(variants[2].Name, q.Label)],-15:F1}");
        output.WriteLine($"  {"Q2 via agregado continuo (readings_1h equivalente)",-58} {"-",-15} {"-",-15} {caggMs,-15:F1}");
        output.WriteLine($"  {"Timescale COMPRIMIDO: Q1 / Q2",-58} {"",-15} {"",-15} {q1Compressed:F1} / {q2Compressed:F1}");

        output.WriteLine("");
        output.WriteLine("== Armazenamento (tabela + indices) ==");
        foreach (var v in variants) output.WriteLine($"  {v.Name,-32} {sizes[v.Name] / 1024.0 / 1024.0,8:F1} MB");
        output.WriteLine($"  {"TimescaleDB comprimido",-32} {compressedSize / 1024.0 / 1024.0,8:F1} MB   ({(double)sizes["TimescaleDB (chunks 1 h)"] / compressedSize:F1}x menor)");

        output.WriteLine("");
        output.WriteLine("== Retencao: apagar as primeiras 24 h (1,5 milhao de linhas) ==");
        foreach (var v in variants) output.WriteLine($"  {v.Name,-32} {retention[v.Name],10:F0} ms");

        // sanidade: sobraram exatamente as linhas das 26 h finais (3120 passos x 500 sensores) em cada variante
        foreach (var v in variants)
            Assert.Equal(3_120L * Sensors, await CountAsync(v.Table));
    }

    // ==================================================================== infraestrutura do benchmark

    private async Task CreateTablesAsync(int hoursOfData)
    {
        await ExecAsync("DROP MATERIALIZED VIEW IF EXISTS bench_hyper_1h; DROP TABLE IF EXISTS bench_plain, bench_part, bench_hyper CASCADE");

        const string columns = "sensor_id uuid NOT NULL, ts timestamptz NOT NULL, value double precision NOT NULL, PRIMARY KEY (sensor_id, ts)";
        await ExecAsync($"CREATE TABLE bench_plain ({columns})");

        await ExecAsync($"CREATE TABLE bench_part ({columns}) PARTITION BY RANGE (ts)");
        for (var h = 0; h <= hoursOfData; h++)
        {
            var from = Start.AddHours(h).UtcDateTime;
            var to = Start.AddHours(h + 1).UtcDateTime;
            await ExecAsync($"CREATE TABLE bench_part_{h:D3} PARTITION OF bench_part FOR VALUES FROM ('{from:O}') TO ('{to:O}')");
        }

        await ExecAsync($"CREATE TABLE bench_hyper ({columns})");
        await ExecAsync("SELECT create_hypertable('bench_hyper', 'ts', chunk_time_interval => INTERVAL '1 hour')");
    }

    /// <summary>Carrega em ordem de tempo (como a ingestão real). Devolve linhas/s no total e nos últimos 10%.</summary>
    private async Task<(double Overall, double Last10)> LoadAsync(string table, Guid[] sensors)
    {
        var random = new Random(1);
        var total = (long)Sensors * Steps;
        var ids = new Guid[BatchRows];
        var times = new DateTime[BatchRows];
        var values = new double[BatchRows];
        var filled = 0;
        long written = 0;
        var overall = Stopwatch.StartNew();
        var lastTenStart = Stopwatch.StartNew();
        long lastTenRows = 0;

        async Task Flush()
        {
            if (filled == 0) return;
            await using var command = platform.DataSource.CreateCommand(
                $"INSERT INTO {table} (sensor_id, ts, value) SELECT * FROM unnest(@i, @t, @v) ON CONFLICT (sensor_id, ts) DO NOTHING");
            command.Parameters.Add(new NpgsqlParameter<Guid[]>("i", NpgsqlDbType.Array | NpgsqlDbType.Uuid) { TypedValue = ids[..filled] });
            command.Parameters.Add(new NpgsqlParameter<DateTime[]>("t", NpgsqlDbType.Array | NpgsqlDbType.TimestampTz) { TypedValue = times[..filled] });
            command.Parameters.Add(new NpgsqlParameter<double[]>("v", NpgsqlDbType.Array | NpgsqlDbType.Double) { TypedValue = values[..filled] });
            await command.ExecuteNonQueryAsync();
            written += filled;
            if (written > total * 0.9) lastTenRows += filled;
            filled = 0;
            if (written <= total * 0.9) lastTenStart.Restart();
        }

        for (var step = 0; step < Steps; step++)
        {
            var ts = Start.AddSeconds((long)step * StepSeconds).UtcDateTime;
            for (var s = 0; s < Sensors; s++)
            {
                ids[filled] = sensors[s];
                times[filled] = ts;
                values[filled] = 20 + 5 * Math.Sin(step / 100.0 + s) + random.NextDouble();
                if (++filled == BatchRows) await Flush();
            }
        }

        await Flush();
        return (total / overall.Elapsed.TotalSeconds, lastTenRows / Math.Max(0.001, lastTenStart.Elapsed.TotalSeconds));
    }

    private async Task<double> MedianMsAsync(string table, string sql, Func<NpgsqlCommand, string, Task> bind, int runs)
    {
        var times = new List<double>();
        for (var i = 0; i < runs + 1; i++)
        {
            await using var command = platform.DataSource.CreateCommand(sql.Replace("{t}", table));
            await bind(command, table);
            var sw = Stopwatch.StartNew();
            await using (var reader = await command.ExecuteReaderAsync())
                while (await reader.ReadAsync()) { }
            sw.Stop();
            if (i > 0) times.Add(sw.Elapsed.TotalMilliseconds); // descarta a 1ª (cache frio)
        }

        times.Sort();
        return times[times.Count / 2];
    }

    private async Task ExecAsync(string sql)
    {
        await using var command = platform.DataSource.CreateCommand(sql);
        command.CommandTimeout = 600;
        await command.ExecuteNonQueryAsync();
    }

    private async Task<long> CountAsync(string table)
    {
        await using var command = platform.DataSource.CreateCommand($"SELECT count(*) FROM {table}");
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private async Task<long> SizeAsync(string table)
    {
        // hypertable: soma dos chunks (pg_total_relation_size da tabela-mãe não os inclui)
        await using var command = platform.DataSource.CreateCommand(
            table == "bench_hyper"
                ? "SELECT coalesce(sum(total_bytes), 0)::bigint FROM (SELECT (hypertable_detailed_size('bench_hyper')).total_bytes AS total_bytes) s"
                : table == "bench_part"
                    ? "SELECT coalesce(sum(pg_total_relation_size(inhrelid)), 0)::bigint FROM pg_inherits WHERE inhparent = 'bench_part'::regclass"
                    : $"SELECT pg_total_relation_size('{table}')");
        return (long)(await command.ExecuteScalarAsync())!;
    }
}
