using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using SensorHub.IntegrationTests.Infrastructure;

namespace SensorHub.IntegrationTests.Ingestion;

[Collection(PlatformCollection.Name)]
public class IngestionApiTests(PlatformFixture kafka)
{
    private const string Topic = "sensorhub.readings";
    private readonly HttpClient _http = kafka.Api.CreateClient();

    private static object Reading(Guid sensor, DateTimeOffset ts, double value, string? unit = "°C") =>
        new { sensorId = sensor, timestamp = ts, value, unit };

    [Fact]
    public async Task Single_reading_is_accepted_and_lands_in_kafka_keyed_by_sensor()
    {
        var sensor = Guid.NewGuid();
        var ts = DateTimeOffset.UtcNow.AddSeconds(-2);

        var response = await _http.PostAsJsonAsync("/api/readings", Reading(sensor, ts, 23.5));

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        var consumed = TopicReader.Read(kafka.BootstrapServers, Topic, 1, new HashSet<Guid> { sensor });
        var only = Assert.Single(consumed);
        Assert.Equal(sensor.ToString("D"), only.Key);
        Assert.Equal(23.5, only.Message.Value);
        Assert.Equal("°C", only.Message.Unit);
        Assert.Equal(ts.ToUniversalTime().Ticks / 10, only.Message.Timestamp.Ticks / 10);
    }

    [Fact]
    public async Task Same_sensor_always_goes_to_the_same_partition_and_keeps_order()
    {
        var sensor = Guid.NewGuid();
        var start = DateTimeOffset.UtcNow.AddMinutes(-1);
        var batch = Enumerable.Range(0, 200).Select(i => Reading(sensor, start.AddMilliseconds(i * 10), i)).ToList();

        // 4 requisições sequenciais de 50: a ordem entre requisições é a ordem de envio
        foreach (var chunk in batch.Chunk(50))
            Assert.Equal(HttpStatusCode.Accepted, (await _http.PostAsJsonAsync("/api/readings/batch", chunk)).StatusCode);

        var consumed = TopicReader.Read(kafka.BootstrapServers, Topic, 200, new HashSet<Guid> { sensor });

        Assert.Equal(200, consumed.Count);
        Assert.Single(consumed.Select(c => c.Partition).Distinct()); // 1 sensor = 1 partição
        Assert.Equal(Enumerable.Range(0, 200).Select(i => (double)i), consumed.OrderBy(c => c.Offset).Select(c => c.Message.Value));
    }

    [Fact]
    public async Task Different_sensors_are_spread_across_partitions()
    {
        var sensors = Enumerable.Range(0, 60).Select(_ => Guid.NewGuid()).ToList();
        var now = DateTimeOffset.UtcNow.AddSeconds(-5);

        var response = await _http.PostAsJsonAsync("/api/readings/batch", sensors.Select(s => Reading(s, now, 1)));
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

        var consumed = TopicReader.Read(kafka.BootstrapServers, Topic, 60, sensors.ToHashSet());

        Assert.Equal(60, consumed.Count);
        Assert.True(consumed.Select(c => c.Partition).Distinct().Count() >= 4,
            "60 sensores deveriam usar a maioria das 6 partições");
    }

    [Fact]
    public async Task Batch_accepts_valid_readings_and_reports_invalid_ones_by_index()
    {
        var good = Guid.NewGuid();
        var now = DateTimeOffset.UtcNow.AddSeconds(-1);
        var payload = new object[]
        {
            Reading(good, now, 1),
            Reading(Guid.Empty, now, 2),                   // sensorId inválido
            Reading(good, now.AddHours(2), 3),             // futuro
            Reading(good, now.AddMilliseconds(5), 4)
        };

        var response = await _http.PostAsJsonAsync("/api/readings/batch", payload);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal(2, body.RootElement.GetProperty("accepted").GetInt32());
        Assert.Equal([1, 2], body.RootElement.GetProperty("rejected").EnumerateArray().Select(r => r.GetProperty("index").GetInt32()));

        var consumed = TopicReader.Read(kafka.BootstrapServers, Topic, 2, new HashSet<Guid> { good });
        Assert.Equal([1d, 4d], consumed.OrderBy(c => c.Offset).Select(c => c.Message.Value));
    }

    [Fact]
    public async Task Batch_where_everything_is_invalid_returns_422()
    {
        var response = await _http.PostAsJsonAsync("/api/readings/batch", new[] { Reading(Guid.Empty, DateTimeOffset.UtcNow, 1) });

        Assert.Equal(HttpStatusCode.UnprocessableEntity, response.StatusCode);
    }

    [Fact]
    public async Task Single_invalid_reading_returns_400_problem_details()
    {
        var response = await _http.PostAsJsonAsync("/api/readings", Reading(Guid.Empty, DateTimeOffset.UtcNow, 1));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Contains("sensorId", await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task Empty_batch_returns_400()
    {
        var response = await _http.PostAsJsonAsync("/api/readings/batch", Array.Empty<object>());

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task Batch_above_max_size_returns_400()
    {
        var now = DateTimeOffset.UtcNow;
        var tooMany = Enumerable.Range(0, 1001).Select(i => Reading(Guid.NewGuid(), now, i));

        var response = await _http.PostAsJsonAsync("/api/readings/batch", tooMany);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Contains("1000", await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task Malformed_json_returns_400_not_500()
    {
        var response = await _http.PostAsync("/api/readings",
            new StringContent("{ isso nao e json", Encoding.UTF8, "application/json"));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task Missing_timestamp_is_filled_with_server_time()
    {
        var sensor = Guid.NewGuid();
        var before = DateTimeOffset.UtcNow;

        var response = await _http.PostAsJsonAsync("/api/readings", new { sensorId = sensor, value = 5.0 });

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        var message = Assert.Single(TopicReader.Read(kafka.BootstrapServers, Topic, 1, new HashSet<Guid> { sensor })).Message;
        Assert.InRange(message.Timestamp, before.AddSeconds(-1), DateTimeOffset.UtcNow.AddSeconds(1));
    }

    [Fact]
    public async Task Topics_are_provisioned_with_the_configured_partition_count()
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = kafka.BootstrapServers }).Build();

        var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));

        Assert.Equal(6, metadata.Topics.Single(t => t.Topic == "sensorhub.readings").Partitions.Count);
        Assert.Single(metadata.Topics.Single(t => t.Topic == "sensorhub.readings.dlq").Partitions);
        Assert.Equal(3, metadata.Topics.Single(t => t.Topic == "sensorhub.alerts").Partitions.Count);
    }

    [Fact]
    public async Task Health_endpoints_report_ready_when_kafka_is_up()
    {
        Assert.Equal(HttpStatusCode.OK, (await _http.GetAsync("/health/live")).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await _http.GetAsync("/health/ready")).StatusCode);
    }

    [Fact]
    public async Task Ingestion_sustains_high_throughput_and_loses_nothing()
    {
        const int total = 20_000;
        const int batchSize = 500;
        var sensors = Enumerable.Range(0, 200).Select(_ => Guid.NewGuid()).ToList();
        var start = DateTimeOffset.UtcNow.AddMinutes(-5);

        var requests = Enumerable.Range(0, total / batchSize).Select(b =>
            Enumerable.Range(0, batchSize).Select(i =>
            {
                var n = b * batchSize + i;
                // (sensor, ts) único: 1ms de diferença por leitura global
                return Reading(sensors[n % sensors.Count], start.AddMilliseconds(n), n);
            }).ToList()).ToList();

        var clock = Stopwatch.StartNew();
        var responses = await Task.WhenAll(requests.Select(batch => _http.PostAsJsonAsync("/api/readings/batch", batch)));
        clock.Stop();

        Assert.All(responses, r => Assert.Equal(HttpStatusCode.Accepted, r.StatusCode));
        var rate = total / clock.Elapsed.TotalSeconds;
        Console.WriteLine($"[throughput] {total:N0} leituras em {clock.Elapsed.TotalSeconds:F2}s = {rate:N0} leituras/s (via HTTP + acks=all)");
        Assert.True(rate > 1_000, $"vazão de {rate:N0}/s abaixo do mínimo esperado");

        var consumed = TopicReader.Read(kafka.BootstrapServers, Topic, total, sensors.ToHashSet(), TimeSpan.FromSeconds(90));
        Assert.Equal(total, consumed.Count);
    }
}

public class IngestionUnavailableTests
{
    [Fact]
    public async Task Returns_503_with_retry_after_when_kafka_is_unreachable()
    {
        // Broker inexistente + timeout curto: o publish falha rápido e a API traduz para 503, não 500.
        await using var factory = new ApiFactory("localhost:1", new()
        {
            ["Kafka:ProvisionTopics"] = "false",
            ["Kafka:Producer:MessageTimeoutMs"] = "1500"
        });
        var http = factory.CreateClient();

        var response = await http.PostAsJsonAsync("/api/readings",
            new { sensorId = Guid.NewGuid(), timestamp = DateTimeOffset.UtcNow, value = 1.0 });

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.True(response.Headers.TryGetValues("Retry-After", out var retry));
        Assert.Equal("1", retry.Single());
    }

    [Fact]
    public async Task Readiness_reports_unhealthy_when_kafka_is_unreachable()
    {
        await using var factory = new ApiFactory("localhost:1", new() { ["Kafka:ProvisionTopics"] = "false" });
        var http = factory.CreateClient();

        var live = await http.GetAsync("/health/live");
        var ready = await http.GetAsync("/health/ready");

        Assert.Equal(HttpStatusCode.OK, live.StatusCode);                       // o processo está vivo
        Assert.Equal(HttpStatusCode.ServiceUnavailable, ready.StatusCode);      // mas não está pronto
    }
}
