using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using SensorHub.Domain.Readings;
using SensorHub.IntegrationTests.Infrastructure;

namespace SensorHub.IntegrationTests.Timescale;

[Collection(PlatformCollection.Name)]
public class SeriesApiTests(PlatformFixture platform)
{
    private readonly HttpClient _http = platform.Api.CreateClient();

    // Uma região de tempo exclusiva desta classe (semana passada, hora cheia)
    private static readonly DateTimeOffset T0 =
        new DateTimeOffset(DateTimeOffset.UtcNow.Date, TimeSpan.Zero).AddDays(-6).AddHours(8);

    private static string Iso(DateTimeOffset value) => Uri.EscapeDataString(value.ToString("O"));

    private async Task SeedAsync(Guid sensor, int seconds)
    {
        var readings = Enumerable.Range(0, seconds).Select(i => Reading.Create(sensor, T0.AddSeconds(i), i)).ToList();
        await platform.CreateStore().InsertBatchAsync(readings, default);
        await using var m = platform.DataSource.CreateCommand("CALL refresh_continuous_aggregate('readings_1m', NULL, NULL)");
        await m.ExecuteNonQueryAsync();
        await using var h = platform.DataSource.CreateCommand("CALL refresh_continuous_aggregate('readings_1h', NULL, NULL)");
        await h.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Series_returns_aggregated_points_for_the_requested_bucket()
    {
        var sensor = Guid.NewGuid();
        await SeedAsync(sensor, 600); // 10 min, 1 leitura/s, valor = segundo

        var response = await _http.GetAsync($"/api/sensors/{sensor}/series?from={Iso(T0)}&to={Iso(T0.AddMinutes(10))}&bucket=5m");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        var points = body.RootElement.GetProperty("points").EnumerateArray().ToList();
        Assert.Equal(2, points.Count);
        Assert.Equal(149.5, points[0].GetProperty("avg").GetDouble());
        Assert.Equal(0, points[0].GetProperty("min").GetDouble());
        Assert.Equal(299, points[0].GetProperty("max").GetDouble());
        Assert.Equal(300, points[0].GetProperty("count").GetInt64());
        Assert.Equal(body.RootElement.GetProperty("sensorId").GetGuid(), sensor);
    }

    [Fact]
    public async Task Series_supports_hourly_buckets_from_the_hourly_aggregate()
    {
        var sensor = Guid.NewGuid();
        await SeedAsync(sensor, 3600 + 60); // cruza a virada da hora

        var response = await _http.GetAsync($"/api/sensors/{sensor}/series?from={Iso(T0)}&to={Iso(T0.AddHours(2))}&bucket=1h");

        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        var points = body.RootElement.GetProperty("points").EnumerateArray().ToList();
        Assert.Equal(2, points.Count);
        Assert.Equal(3600, points[0].GetProperty("count").GetInt64());
        Assert.Equal(60, points[1].GetProperty("count").GetInt64());
    }

    [Fact]
    public async Task Series_picks_an_automatic_bucket_when_none_is_given()
    {
        var sensor = Guid.NewGuid();
        await SeedAsync(sensor, 3600);

        var response = await _http.GetAsync($"/api/sensors/{sensor}/series?from={Iso(T0)}&to={Iso(T0.AddHours(8))}");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        // 8 h com bucket auto = 1 min (480 pontos); só a 1ª hora tem dado = 60 baldes
        Assert.Equal(60, body.RootElement.GetProperty("points").GetArrayLength());
    }

    [Fact]
    public async Task Unknown_sensor_returns_an_empty_series_not_an_error()
    {
        var response = await _http.GetAsync($"/api/sensors/{Guid.NewGuid()}/series?from={Iso(T0)}&to={Iso(T0.AddHours(1))}");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal(0, body.RootElement.GetProperty("points").GetArrayLength());
    }

    [Theory]
    [InlineData("bucket=3m")]                                  // bucket não suportado
    [InlineData("bucket=abc")]
    [InlineData("from=2026-01-01T00:00:00Z&to=2026-02-01T00:00:00Z&bucket=1m")]  // pontos demais
    [InlineData("from=2026-02-01T00:00:00Z&to=2026-01-01T00:00:00Z")]            // intervalo invertido
    [InlineData("from=2020-01-01T00:00:00Z&to=2026-01-01T00:00:00Z&bucket=1d")]  // intervalo longo demais
    public async Task Invalid_series_queries_are_rejected_with_400_and_a_helpful_message(string query)
    {
        var response = await _http.GetAsync($"/api/sensors/{Guid.NewGuid()}/series?{query}");

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.False(string.IsNullOrWhiteSpace(body.RootElement.GetProperty("detail").GetString()));
    }

    [Fact]
    public async Task Malformed_sensor_id_or_date_is_a_client_error()
    {
        Assert.Equal(HttpStatusCode.NotFound, (await _http.GetAsync("/api/sensors/not-a-guid/series")).StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, (await _http.GetAsync($"/api/sensors/{Guid.NewGuid()}/series?from=ontem")).StatusCode);
    }

    [Fact]
    public async Task Raw_readings_come_back_in_chronological_order_and_respect_the_limit()
    {
        var sensor = Guid.NewGuid();
        await SeedAsync(sensor, 100);

        var response = await _http.GetAsync($"/api/sensors/{sensor}/readings?from={Iso(T0)}&to={Iso(T0.AddMinutes(5))}&limit=10");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        var values = body.RootElement.GetProperty("points").EnumerateArray().Select(p => p.GetProperty("value").GetDouble()).ToList();
        Assert.Equal(Enumerable.Range(0, 10).Select(i => (double)i), values);
    }

    [Theory]
    [InlineData("limit=0")]
    [InlineData("limit=10001")]
    public async Task Raw_limit_out_of_range_is_rejected(string query)
    {
        var response = await _http.GetAsync($"/api/sensors/{Guid.NewGuid()}/readings?{query}");

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task End_to_end_reading_ingested_over_http_shows_up_in_the_series_after_the_worker_persists_it()
    {
        // Pipeline inteiro: POST /api/readings -> Kafka -> consumer -> Timescale -> GET /series
        var sensor = Guid.NewGuid();
        var now = DateTimeOffset.UtcNow;
        var post = await _http.PostAsJsonAsync("/api/readings/batch",
            Enumerable.Range(0, 30).Select(i => new { sensorId = sensor, timestamp = now.AddSeconds(-30 + i), value = 100.0 + i }));
        Assert.Equal(HttpStatusCode.Accepted, post.StatusCode);

        // o consumer de persistência roda aqui, no processo do teste, no tópico de produção da API
        var kafkaOptions = new SensorHub.Infrastructure.Kafka.KafkaOptions { BootstrapServers = platform.BootstrapServers };
        var handler = new SensorHub.Application.Processing.PersistReadingsHandler(
            platform.CreateStore(), Microsoft.Extensions.Logging.Abstractions.NullLogger<SensorHub.Application.Processing.PersistReadingsHandler>.Instance);
        using var dlq = new SensorHub.Infrastructure.Kafka.KafkaDeadLetterSink(Microsoft.Extensions.Options.Options.Create(kafkaOptions));
        using var consumer = new SensorHub.Infrastructure.Kafka.KafkaBatchConsumer(
            "e2e", kafkaOptions, new SensorHub.Infrastructure.Kafka.BatchConsumerOptions { GroupId = $"e2e-{Guid.NewGuid():N}", MaxWaitMs = 100 },
            handler, dlq, Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);
        await consumer.StartAsync(default);
        try
        {
            await TestHelpers.EventuallyAsync(async () => await platform.CountReadingsAsync([sensor]) == 30, "30 leituras persistidas");

            // Em produção a política de refresh (a cada 30 s) materializa leituras que chegaram atrás da marca
            // d'água do agregado; aqui o teste dispara o mesmo refresh para não depender do relógio do job.
            await using (var refresh = platform.DataSource.CreateCommand("CALL refresh_continuous_aggregate('readings_1m', NULL, NULL)"))
                await refresh.ExecuteNonQueryAsync();

            var response = await _http.GetAsync($"/api/sensors/{sensor}/series?from={Iso(now.AddMinutes(-2))}&to={Iso(now.AddMinutes(1))}&bucket=1m");
            using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
            var total = body.RootElement.GetProperty("points").EnumerateArray().Sum(p => p.GetProperty("count").GetInt64());
            Assert.Equal(30, total);
        }
        finally
        {
            await consumer.StopAsync(default);
        }
    }
}
