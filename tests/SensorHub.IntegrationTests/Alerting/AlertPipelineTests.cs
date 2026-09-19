using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SensorHub.Application.Alerting;
using SensorHub.Application.Contracts;
using SensorHub.Infrastructure.Kafka;
using SensorHub.Infrastructure.Persistence;
using SensorHub.Infrastructure.Redis;
using SensorHub.IntegrationTests.Infrastructure;
using static SensorHub.IntegrationTests.Infrastructure.TestHelpers;

namespace SensorHub.IntegrationTests.Alerting;

/// <summary>
/// O pipeline de alertas inteiro, com infraestrutura REAL: leituras no Kafka -> consumer do grupo de alertas ->
/// motor (regras do Postgres, estado no Redis) -> alerta no banco + evento no tópico de alertas -> API.
/// </summary>
[Collection(PlatformCollection.Name)]
public class AlertPipelineTests(PlatformFixture platform)
{
    private readonly HttpClient _http = platform.Api.CreateClient();

    private sealed class Harness : IAsyncDisposable
    {
        public required AlertEngine Engine { get; init; }
        public required KafkaBatchConsumer Consumer { get; init; }
        public required KafkaDeadLetterSink Dlq { get; init; }
        public required KafkaAlertEventPublisher Publisher { get; init; }

        public async ValueTask DisposeAsync()
        {
            await Consumer.StopAsync(default);
            Consumer.Dispose();
            Dlq.Dispose();
            Publisher.Dispose();
        }
    }

    private async Task<Harness> StartEngineAsync(KafkaOptions kafka)
    {
        var redisOptions = Options.Create(new RedisOptions { KeyPrefix = "test:" });
        var catalog = new CachedRuleCatalog(platform.ContextFactory, Options.Create(new AlertingOptions { RuleCacheSeconds = 0 }), TimeProvider.System);
        var publisher = new KafkaAlertEventPublisher(Options.Create(kafka));
        var engine = new AlertEngine(
            catalog, new RedisRuleStateStore(platform.Redis, redisOptions), new PostgresAlertStore(platform.ContextFactory),
            publisher, TimeProvider.System, NullLogger<AlertEngine>.Instance);

        var dlq = new KafkaDeadLetterSink(Options.Create(kafka));
        var consumer = new KafkaBatchConsumer("alerts", kafka,
            new BatchConsumerOptions { GroupId = $"alerts-{Guid.NewGuid():N}", MaxBatchSize = 500, MaxWaitMs = 100, InitialRetryDelayMs = 100, MaxRetryDelayMs = 400 },
            new EvaluateAlertsHandler(engine), dlq, NullLogger.Instance);
        await consumer.StartAsync(default);
        return new Harness { Engine = engine, Consumer = consumer, Dlq = dlq, Publisher = publisher };
    }

    private async Task<Guid> CreateSensorViaApiAsync()
    {
        var response = await _http.PostAsJsonAsync("/api/sensors",
            new { deviceId = platform.SharedDeviceId, name = $"s-{Guid.NewGuid():N}"[..14], metric = "Temperature", unit = "°C", group = "pipeline" });
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        return doc.RootElement.GetProperty("id").GetGuid();
    }

    private async Task<Guid> CreateRuleViaApiAsync(Guid sensor, string type = "Threshold", int durationSeconds = 0)
    {
        var response = await _http.PostAsJsonAsync("/api/alert-rules", new
        {
            sensorId = sensor, name = "Temp alta", type, comparison = "GreaterThan",
            threshold = 80, durationSeconds, hysteresis = 0, severity = "Critical"
        });
        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        return doc.RootElement.GetProperty("id").GetGuid();
    }

    private async Task<JsonElement> GetAlertsAsync(Guid sensor)
    {
        using var doc = JsonDocument.Parse(await _http.GetStringAsync($"/api/alerts?sensorId={sensor}"));
        return doc.RootElement.Clone();
    }

    private static List<AlertEvent> ReadAlertEvents(KafkaOptions kafka, Guid sensor, int expected)
    {
        var messages = ReadAll(kafka.BootstrapServers, kafka.AlertsTopic, expected, TimeSpan.FromSeconds(20));
        return messages.Select(m => AlertEventSerializer.Deserialize(m.Message.Value)!).Where(e => e.SensorId == sensor).ToList();
    }

    [Fact]
    public async Task A_sustained_violation_produces_exactly_one_alert_then_resolves_when_the_signal_recovers()
    {
        var kafka = await platform.CreateIsolatedTopicsAsync();
        var sensor = await CreateSensorViaApiAsync();
        await CreateRuleViaApiAsync(sensor);
        await using var engine = await StartEngineAsync(kafka);

        // 300 leituras normais, 500 acima do limite, 200 normais, ao longo de 1000 s
        var t = new DateTimeOffset(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);
        var messages = Enumerable.Range(0, 1000)
            .Select(i => Message(sensor, t.AddSeconds(i), i is >= 300 and < 800 ? 95 : 60))
            .ToList();
        await PublishAsync(kafka, messages);

        await EventuallyAsync(async () =>
            (await GetAlertsAsync(sensor)).EnumerateArray().Count(a => a.GetProperty("status").GetString() == "Resolved") == 1,
            "1 alerta disparado e resolvido");

        var alerts = await GetAlertsAsync(sensor);
        var alert = Assert.Single(alerts.EnumerateArray()); // 500 leituras violando => UM alerta
        Assert.Equal("Critical", alert.GetProperty("severity").GetString());
        Assert.Equal(t.AddSeconds(300), alert.GetProperty("firedAt").GetDateTimeOffset());
        Assert.Equal(t.AddSeconds(800), alert.GetProperty("resolvedAt").GetDateTimeOffset());

        var events = ReadAlertEvents(kafka, sensor, expected: 2);
        Assert.Equal([AlertEventKind.Fired, AlertEventKind.Resolved], events.Select(e => e.Kind));
        Assert.All(events, e => Assert.Equal(alert.GetProperty("id").GetGuid(), e.AlertId));
    }

    [Fact]
    public async Task The_same_stream_replayed_by_a_brand_new_consumer_group_creates_no_new_alerts()
    {
        var kafka = await platform.CreateIsolatedTopicsAsync();
        var sensor = await CreateSensorViaApiAsync();
        await CreateRuleViaApiAsync(sensor);
        var t = new DateTimeOffset(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);
        await PublishAsync(kafka, Enumerable.Range(0, 200).Select(i => Message(sensor, t.AddSeconds(i), i < 100 ? 60 : 95)).ToList());

        await using (var first = await StartEngineAsync(kafka))
            await EventuallyAsync(async () => (await GetAlertsAsync(sensor)).GetArrayLength() == 1, "alerta disparado");

        // Outro consumer group (ex.: reset de offsets): relê TUDO. O estado no Redis e o banco absorvem o replay.
        await using (var second = await StartEngineAsync(kafka))
            await EventuallyAsync(() => Task.FromResult(second.Consumer.Consumed >= 200), "replay consumido");

        Assert.Equal(1, (await GetAlertsAsync(sensor)).GetArrayLength());
    }

    [Fact]
    public async Task Disabled_rules_stop_producing_alerts()
    {
        var kafka = await platform.CreateIsolatedTopicsAsync();
        var sensor = await CreateSensorViaApiAsync();
        var rule = await CreateRuleViaApiAsync(sensor);
        await _http.PatchAsJsonAsync($"/api/alert-rules/{rule}/enabled", new { enabled = false });
        await using var engine = await StartEngineAsync(kafka);

        await PublishAsync(kafka, Enumerable.Range(0, 100).Select(i => Message(sensor, new DateTimeOffset(2026, 6, 1, 12, 0, 0, TimeSpan.Zero).AddSeconds(i), 99)).ToList());
        await EventuallyAsync(() => Task.FromResult(engine.Consumer.Consumed >= 100), "consumido");

        Assert.Equal(0, (await GetAlertsAsync(sensor)).GetArrayLength());
    }

    [Fact]
    public async Task No_data_alert_fires_when_a_sensor_goes_silent_and_resolves_when_it_returns()
    {
        var kafka = await platform.CreateIsolatedTopicsAsync();
        var sensor = await CreateSensorViaApiAsync();
        await CreateRuleViaApiAsync(sensor, "NoData", durationSeconds: 2);
        await using var engine = await StartEngineAsync(kafka);

        var lastSeen = DateTimeOffset.UtcNow;
        await PublishAsync(kafka, [Message(sensor, lastSeen, 50)]);
        await EventuallyAsync(() => Task.FromResult(engine.Consumer.Consumed >= 1), "leitura consumida");
        Assert.Equal(0, await engine.Engine.SweepNoDataAsync(TimeSpan.FromSeconds(30), default)); // ainda não passou 2 s

        await Task.Delay(TimeSpan.FromSeconds(3)); // o sensor "fica mudo"
        Assert.Equal(1, await engine.Engine.SweepNoDataAsync(TimeSpan.FromSeconds(30), default));
        Assert.Equal(0, await engine.Engine.SweepNoDataAsync(TimeSpan.FromSeconds(30), default)); // varreduras seguintes não repetem

        var alert = Assert.Single((await GetAlertsAsync(sensor)).EnumerateArray());
        Assert.Equal("Firing", alert.GetProperty("status").GetString());
        // o instante do disparo é "última leitura + 2 s", não o relógio da varredura
        Assert.Equal(lastSeen.AddSeconds(2), alert.GetProperty("firedAt").GetDateTimeOffset(), TimeSpan.FromMilliseconds(1));

        await PublishAsync(kafka, [Message(sensor, DateTimeOffset.UtcNow, 51)]); // o sensor volta
        await EventuallyAsync(async () =>
            (await GetAlertsAsync(sensor)).EnumerateArray().Single().GetProperty("status").GetString() == "Resolved", "alerta de silêncio resolvido");

        var events = ReadAlertEvents(kafka, sensor, expected: 2);
        Assert.Equal([AlertEventKind.Fired, AlertEventKind.Resolved], events.Select(e => e.Kind));
    }

    [Fact]
    public async Task An_acknowledged_alert_is_still_resolved_by_the_engine_and_keeps_who_acknowledged_it()
    {
        var kafka = await platform.CreateIsolatedTopicsAsync();
        var sensor = await CreateSensorViaApiAsync();
        await CreateRuleViaApiAsync(sensor);
        await using var engine = await StartEngineAsync(kafka);
        var t = new DateTimeOffset(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);

        await PublishAsync(kafka, [Message(sensor, t, 95)]);
        await EventuallyAsync(async () => (await GetAlertsAsync(sensor)).GetArrayLength() == 1, "alerta aberto");
        var id = (await GetAlertsAsync(sensor)).EnumerateArray().Single().GetProperty("id").GetGuid();
        Assert.Equal(HttpStatusCode.OK, (await _http.PostAsJsonAsync($"/api/alerts/{id}/acknowledge", new { user = "maria" })).StatusCode);

        await PublishAsync(kafka, [Message(sensor, t.AddSeconds(30), 60)]);

        await EventuallyAsync(async () =>
            (await GetAlertsAsync(sensor)).EnumerateArray().Single().GetProperty("status").GetString() == "Resolved", "resolvido");
        var alert = (await GetAlertsAsync(sensor)).EnumerateArray().Single();
        Assert.Equal("maria", alert.GetProperty("acknowledgedBy").GetString());
    }

    [Fact]
    public async Task Alert_events_use_the_sensor_id_as_key_so_a_sensors_events_stay_ordered()
    {
        var kafka = await platform.CreateIsolatedTopicsAsync();
        var sensor = await CreateSensorViaApiAsync();
        await CreateRuleViaApiAsync(sensor);
        await using var engine = await StartEngineAsync(kafka);
        var t = new DateTimeOffset(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);

        // 3 episódios: 3 disparos e 3 resoluções
        var values = new[] { 95, 60, 96, 61, 97, 62 };
        await PublishAsync(kafka, values.Select((v, i) => Message(sensor, t.AddSeconds(i * 10), v)).ToList());
        await EventuallyAsync(async () => (await GetAlertsAsync(sensor)).GetArrayLength() == 3, "3 alertas");

        var messages = ReadAll(kafka.BootstrapServers, kafka.AlertsTopic, 6, TimeSpan.FromSeconds(20));
        Assert.Equal(6, messages.Count);
        Assert.All(messages, m => Assert.Equal(sensor.ToString("D"), m.Message.Key));
        Assert.Single(messages.Select(m => m.Partition).Distinct()); // mesma chave, mesma partição
        Assert.Equal(
            [AlertEventKind.Fired, AlertEventKind.Resolved, AlertEventKind.Fired, AlertEventKind.Resolved, AlertEventKind.Fired, AlertEventKind.Resolved],
            messages.OrderBy(m => m.Offset.Value).Select(m => AlertEventSerializer.Deserialize(m.Message.Value)!.Kind));
        Assert.All(messages, m => Assert.Equal(
            AlertEventSerializer.Deserialize(m.Message.Value)!.Kind.ToString(),
            Encoding.UTF8.GetString(m.Message.Headers.GetLastBytes("event-kind")))); // o header espelha o corpo
    }
}
