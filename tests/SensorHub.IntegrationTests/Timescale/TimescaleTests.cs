using Npgsql;
using SensorHub.Domain.Readings;
using SensorHub.IntegrationTests.Infrastructure;

namespace SensorHub.IntegrationTests.Timescale;

/// <summary>
/// Testes do ciclo de vida do dado no TimescaleDB: hypertable, chunks, columnstore (compressão), retenção
/// e agregados contínuos. Usam timestamps recentes (ontem/anteontem), fora do alcance das políticas
/// automáticas (colunar após 7 dias, retenção após 90), para não competirem com os jobs em background.
/// </summary>
[Collection(PlatformCollection.Name)]
public class TimescaleTests(PlatformFixture platform)
{
    private static readonly DateTimeOffset HourStart = new DateTimeOffset(DateTimeOffset.UtcNow.Date, TimeSpan.Zero).AddDays(-2).AddHours(10);

    private static Reading R(Guid sensor, DateTimeOffset ts, double value) => Reading.Create(sensor, ts, value);

    private async Task<T> Scalar<T>(string sql, params (string Name, object Value)[] parameters)
    {
        await using var command = platform.DataSource.CreateCommand(sql);
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value);
        return (T)(await command.ExecuteScalarAsync())!;
    }

    private async Task Exec(string sql)
    {
        await using var command = platform.DataSource.CreateCommand(sql);
        await command.ExecuteNonQueryAsync();
    }

    // ------------------------------------------------------------------ esquema

    [Fact]
    public async Task Readings_is_a_hypertable_with_one_day_chunks()
    {
        Assert.Equal(1L, await Scalar<long>("SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'readings'"));
        Assert.Equal(TimeSpan.FromDays(1), await Scalar<TimeSpan>(
            "SELECT time_interval FROM timescaledb_information.dimensions WHERE hypertable_name = 'readings'"));
    }

    [Fact]
    public async Task Columnstore_is_configured_segmented_by_sensor_and_ordered_by_time()
    {
        await using var command = platform.DataSource.CreateCommand(
            "SELECT attname, segmentby_column_index, orderby_column_index, orderby_asc FROM timescaledb_information.compression_settings WHERE hypertable_name = 'readings'");
        await using var reader = await command.ExecuteReaderAsync();
        var settings = new Dictionary<string, (int? Segment, int? Order, bool? Asc)>();
        while (await reader.ReadAsync())
            settings[reader.GetString(0)] = (
                reader.IsDBNull(1) ? null : reader.GetInt32(1),
                reader.IsDBNull(2) ? null : reader.GetInt32(2),
                reader.IsDBNull(3) ? null : reader.GetBoolean(3));

        Assert.Equal(1, settings["sensor_id"].Segment);
        Assert.Equal(1, settings["ts"].Order);
        Assert.False(settings["ts"].Asc); // DESC: o dado mais recente primeiro
    }

    [Fact]
    public async Task Lifecycle_policies_exist_for_raw_data_and_for_both_aggregates()
    {
        await using var command = platform.DataSource.CreateCommand(
            "SELECT hypertable_name, proc_name FROM timescaledb_information.jobs WHERE hypertable_name IS NOT NULL");
        await using var reader = await command.ExecuteReaderAsync();
        var jobs = new HashSet<(string, string)>();
        while (await reader.ReadAsync()) jobs.Add((reader.GetString(0), reader.GetString(1)));

        Assert.Contains(("readings", "policy_compression"), jobs);
        Assert.Contains(("readings", "policy_retention"), jobs);
        Assert.Contains(("readings_1m", "policy_refresh_continuous_aggregate"), jobs);
        Assert.Contains(("readings_1h", "policy_refresh_continuous_aggregate"), jobs);
        Assert.Contains(("readings_1m", "policy_retention"), jobs);
        Assert.Contains(("readings_1h", "policy_retention"), jobs);
    }

    [Fact]
    public async Task Raw_retention_is_shorter_than_minute_aggregate_retention_which_is_shorter_than_hourly()
    {
        async Task<TimeSpan> Retention(string hypertable) => await Scalar<TimeSpan>(
            "SELECT (config->>'drop_after')::interval FROM timescaledb_information.jobs WHERE hypertable_name = @h AND proc_name = 'policy_retention'",
            ("h", hypertable));

        var raw = await Retention("readings");
        var minute = await Retention("readings_1m");
        var hour = await Retention("readings_1h");

        Assert.Equal(TimeSpan.FromDays(90), raw);
        Assert.True(raw < minute && minute < hour, "quanto mais grosso o dado, mais tempo se guarda");
    }

    // ------------------------------------------------------------------ chunks e ciclo de vida

    [Fact]
    public async Task Data_is_split_into_one_chunk_per_day()
    {
        var sensor = Guid.NewGuid();
        var day0 = HourStart.AddDays(-20); // região sem outros testes
        var readings = Enumerable.Range(0, 5).Select(d => R(sensor, day0.AddDays(d), d)).ToList();

        await platform.CreateStore().InsertBatchAsync(readings, default);

        // chunk que contém cada leitura: 5 dias diferentes => 5 chunks distintos
        var chunkNames = new HashSet<string>();
        foreach (var reading in readings)
        {
            chunkNames.Add(await Scalar<string>(
                "SELECT chunk_name FROM timescaledb_information.chunks WHERE hypertable_name = 'readings' AND range_start <= @ts AND range_end > @ts",
                ("ts", reading.Timestamp.UtcDateTime)));
        }

        Assert.Equal(5, chunkNames.Count);
    }

    [Fact]
    public async Task Compressed_chunks_stay_queryable_and_still_deduplicate_and_accept_new_rows()
    {
        var sensor = Guid.NewGuid();
        var day = HourStart.AddDays(-30); // 32 dias atrás: dentro da retenção (90), fora dos outros testes
        var store = platform.CreateStore();
        var original = Enumerable.Range(0, 200).Select(i => R(sensor, day.AddSeconds(i), i)).ToList();
        await store.InsertBatchAsync(original, default);

        // Comprime o chunk (o que a política faria depois de 7 dias)
        await Exec($"SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('readings', older_than => now() - interval '7 days', newer_than => now() - interval '40 days') c");
        var compressed = await Scalar<long>(
            "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'readings' AND is_compressed AND range_start <= @day AND range_end > @day",
            ("day", day.UtcDateTime));
        Assert.True(compressed >= 1, "o chunk deveria estar comprimido");

        // 1) as leituras continuam consultáveis, com os mesmos valores
        var raw = await new SensorHub.Infrastructure.Persistence.TimescaleReadingQueries(platform.DataSource)
            .GetRawAsync(sensor, day.AddSeconds(-1), day.AddSeconds(500), 1000, default);
        Assert.Equal(200, raw.Count);
        Assert.Equal(original.Select(r => r.Value), raw.Select(r => r.Value));

        // 2) reenvio das mesmas leituras (reentrega do Kafka) continua sendo ignorado em chunk comprimido
        Assert.Equal(0, await store.InsertBatchAsync(original, default));
        Assert.Equal(200, await platform.CountReadingsAsync([sensor]));

        // 3) leitura NOVA (atrasada) ainda entra no chunk comprimido
        Assert.Equal(1, await store.InsertBatchAsync([R(sensor, day.AddSeconds(1000), 999)], default));
        Assert.Equal(201, await platform.CountReadingsAsync([sensor]));
    }

    [Fact]
    public async Task Dropping_expired_chunks_removes_old_data_and_keeps_recent_data()
    {
        var sensor = Guid.NewGuid();
        var old = HourStart.AddDays(-150);
        var recent = HourStart.AddDays(-1);
        await platform.CreateStore().InsertBatchAsync([R(sensor, old, 1), R(sensor, recent, 2)], default);

        // É o que o job de retenção executa (aqui com a mesma janela de 90 dias)
        await Exec("SELECT drop_chunks('readings', older_than => interval '90 days')");

        var remaining = await Scalar<long>("SELECT count(*) FROM readings WHERE sensor_id = @id", ("id", sensor));
        Assert.Equal(1, remaining);
        Assert.Equal(2d, await Scalar<double>("SELECT value FROM readings WHERE sensor_id = @id", ("id", sensor)));
    }

    // ------------------------------------------------------------------ agregados contínuos

    private async Task RefreshAggregatesAsync()
    {
        await Exec("CALL refresh_continuous_aggregate('readings_1m', NULL, NULL)");
        await Exec("CALL refresh_continuous_aggregate('readings_1h', NULL, NULL)");
    }

    private async Task<List<(DateTimeOffset Bucket, double Sum, long Count, double Min, double Max)>> ReadView(string view, Guid sensor)
    {
        await using var command = platform.DataSource.CreateCommand(
            $"SELECT bucket, sum, count, min, max FROM {view} WHERE sensor_id = @id ORDER BY bucket");
        command.Parameters.AddWithValue("id", sensor);
        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<(DateTimeOffset, double, long, double, double)>();
        while (await reader.ReadAsync())
            rows.Add((new DateTimeOffset(reader.GetDateTime(0), TimeSpan.Zero), reader.GetDouble(1), Convert.ToInt64(reader.GetValue(2)), reader.GetDouble(3), reader.GetDouble(4)));
        return rows;
    }

    [Fact]
    public async Task Minute_aggregate_matches_a_hand_computed_result()
    {
        var sensor = Guid.NewGuid();
        var t = HourStart.AddDays(-3);
        // minuto 0: 10, 20, 60 ; minuto 1: 5
        await platform.CreateStore().InsertBatchAsync(
            [R(sensor, t.AddSeconds(5), 10), R(sensor, t.AddSeconds(20), 20), R(sensor, t.AddSeconds(59), 60), R(sensor, t.AddSeconds(61), 5)], default);
        await RefreshAggregatesAsync();

        var rows = await ReadView("readings_1m", sensor);

        Assert.Equal(2, rows.Count);
        Assert.Equal((t, 90d, 3L, 10d, 60d), rows[0]);
        Assert.Equal((t.AddMinutes(1), 5d, 1L, 5d, 5d), rows[1]);
    }

    [Fact]
    public async Task Hourly_average_uses_sum_and_count_not_the_average_of_averages()
    {
        var sensor = Guid.NewGuid();
        var t = HourStart.AddDays(-3).AddHours(2);
        // minuto 0: 1 leitura de 10 (média 10). minuto 1: 3 leituras de 20 (média 20).
        // Média das médias = 15 (ERRADO). Média real = (10 + 60) / 4 = 17,5.
        await platform.CreateStore().InsertBatchAsync(
            [R(sensor, t.AddSeconds(1), 10), R(sensor, t.AddSeconds(61), 20), R(sensor, t.AddSeconds(62), 20), R(sensor, t.AddSeconds(63), 20)], default);
        await RefreshAggregatesAsync();

        var queries = new SensorHub.Infrastructure.Persistence.TimescaleReadingQueries(platform.DataSource);
        var series = await queries.GetSeriesAsync(new(sensor, t, t.AddHours(1), TimeSpan.FromHours(1)), default);

        var point = Assert.Single(series);
        Assert.Equal(17.5, point.Average);
        Assert.Equal(4, point.Count);
        Assert.Equal(10, point.Min);
        Assert.Equal(20, point.Max);
    }

    [Fact]
    public async Task Real_time_aggregation_includes_data_that_was_never_materialized()
    {
        var sensor = Guid.NewGuid();
        // Dados ACIMA da marca d'água do agregado (o instante até onde ele já foi materializado). Só esses são
        // cobertos pela agregação em tempo real; timestamps no futuro garantem que estão sempre acima dela,
        // não importa quantos refreshes outros testes já tenham feito.
        var t = DateTimeOffset.UtcNow.AddDays(1);
        await platform.CreateStore().InsertBatchAsync([R(sensor, t, 40), R(sensor, t.AddSeconds(10), 60)], default);
        // SEM refresh: nada foi materializado para este sensor

        var queries = new SensorHub.Infrastructure.Persistence.TimescaleReadingQueries(platform.DataSource);
        var series = await queries.GetSeriesAsync(new(sensor, t.AddMinutes(-1), t.AddMinutes(2), TimeSpan.FromMinutes(1)), default);

        Assert.Equal(2, series.Sum(p => p.Count));
        Assert.Equal(50, series.Sum(p => p.Average * p.Count) / series.Sum(p => p.Count));
    }

    [Fact]
    public async Task Late_data_behind_the_watermark_is_invisible_until_the_next_refresh()
    {
        // Achado real (ver ADR): a agregação em tempo real só une o dado MAIS NOVO que a marca d'água. Uma
        // leitura ATRASADA cujo balde já está atrás dela não aparece na série até o próximo refresh.
        var sensor = Guid.NewGuid();
        var t = HourStart.AddDays(-7);
        var store = platform.CreateStore();
        await store.InsertBatchAsync([R(sensor, t, 1)], default);
        await RefreshAggregatesAsync(); // avança a marca d'água muito além de t

        await store.InsertBatchAsync([R(sensor, t.AddSeconds(30), 3)], default); // atrasada: mesmo balde, atrás da marca
        var queries = new SensorHub.Infrastructure.Persistence.TimescaleReadingQueries(platform.DataSource);
        var before = await queries.GetSeriesAsync(new(sensor, t, t.AddMinutes(1), TimeSpan.FromMinutes(1)), default);
        Assert.Equal(1, before.Single().Count); // ainda não enxerga a leitura atrasada

        await RefreshAggregatesAsync(); // é o que a política de refresh faz a cada 30 s
        var after = await queries.GetSeriesAsync(new(sensor, t, t.AddMinutes(1), TimeSpan.FromMinutes(1)), default);
        Assert.Equal(2, after.Single().Count);
        Assert.Equal(2, after.Single().Average);
    }

    [Fact]
    public async Task Late_data_invalidates_a_materialized_bucket_and_the_refresh_corrects_it()
    {
        var sensor = Guid.NewGuid();
        var t = HourStart.AddDays(-4);
        var store = platform.CreateStore();
        await store.InsertBatchAsync([R(sensor, t.AddSeconds(1), 10)], default);
        await RefreshAggregatesAsync();
        Assert.Equal(1, (await ReadView("readings_1m", sensor)).Single().Count);

        // chega uma leitura ATRASADA para um balde que já estava materializado
        await store.InsertBatchAsync([R(sensor, t.AddSeconds(30), 30)], default);
        await RefreshAggregatesAsync();

        var bucket = (await ReadView("readings_1m", sensor)).Single();
        Assert.Equal(2, bucket.Count);
        Assert.Equal(40d, bucket.Sum);
    }

    [Fact]
    public async Task Five_minute_series_rolls_up_minute_buckets_correctly()
    {
        var sensor = Guid.NewGuid();
        var t = HourStart.AddDays(-5).AddHours(1);
        var readings = Enumerable.Range(0, 600).Select(i => R(sensor, t.AddSeconds(i), i)).ToList(); // 10 min, 1/s
        await platform.CreateStore().InsertBatchAsync(readings, default);
        await RefreshAggregatesAsync();

        var series = await new SensorHub.Infrastructure.Persistence.TimescaleReadingQueries(platform.DataSource)
            .GetSeriesAsync(new(sensor, t, t.AddMinutes(10), TimeSpan.FromMinutes(5)), default);

        Assert.Equal(2, series.Count);
        Assert.Equal(t, series[0].Bucket);
        Assert.Equal(300, series[0].Count);
        Assert.Equal(149.5, series[0].Average);   // média de 0..299
        Assert.Equal(0, series[0].Min);
        Assert.Equal(299, series[0].Max);
        Assert.Equal(449.5, series[1].Average);   // média de 300..599
    }
}
