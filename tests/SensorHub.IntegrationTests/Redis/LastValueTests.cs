using System.Net;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SensorHub.Application.LatestValues;
using SensorHub.Application.Processing;
using SensorHub.Domain.Readings;
using SensorHub.Infrastructure.Kafka;
using SensorHub.IntegrationTests.Infrastructure;
using StackExchange.Redis;
using static SensorHub.IntegrationTests.Infrastructure.TestHelpers;

namespace SensorHub.IntegrationTests.Redis;

[Collection(PlatformCollection.Name)]
public class RedisLastValueStoreTests(PlatformFixture platform)
{
    private static readonly DateTimeOffset T0 = new(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);

    private static Reading R(Guid sensor, double seconds, double value) =>
        Reading.Create(sensor, T0.AddSeconds(seconds), value);

    [Fact]
    public async Task Roundtrips_timestamp_at_microsecond_precision_and_exact_double()
    {
        var sensor = Guid.NewGuid();
        var store = platform.CreateLastValueStore();
        var reading = Reading.Create(sensor, T0.AddTicks(1234560), 0.1 + 0.2);

        Assert.Equal(1, await store.SetIfNewerAsync([reading], default));

        var stored = await store.GetAsync(sensor, default);
        Assert.Equal(reading.Timestamp, stored!.Value.Timestamp);
        Assert.Equal(reading.Value, stored.Value.Value); // igualdade exata de double
    }

    [Fact]
    public async Task Missing_sensor_returns_null()
    {
        Assert.Null(await platform.CreateLastValueStore().GetAsync(Guid.NewGuid(), default));
    }

    [Fact]
    public async Task Newer_reading_replaces_the_stored_one()
    {
        var sensor = Guid.NewGuid();
        var store = platform.CreateLastValueStore();
        await store.SetIfNewerAsync([R(sensor, 1, 10)], default);

        Assert.Equal(1, await store.SetIfNewerAsync([R(sensor, 2, 20)], default));

        Assert.Equal(20, (await store.GetAsync(sensor, default))!.Value.Value);
    }

    [Fact]
    public async Task Older_or_equal_timestamps_never_regress_the_state()
    {
        var sensor = Guid.NewGuid();
        var store = platform.CreateLastValueStore();
        await store.SetIfNewerAsync([R(sensor, 10, 100)], default);

        Assert.Equal(0, await store.SetIfNewerAsync([R(sensor, 5, 50)], default));    // atrasada
        Assert.Equal(0, await store.SetIfNewerAsync([R(sensor, 10, 999)], default));  // mesmo instante (reentrega)

        var stored = (await store.GetAsync(sensor, default))!.Value;
        Assert.Equal(100, stored.Value);
        Assert.Equal(T0.AddSeconds(10), stored.Timestamp);
    }

    [Fact]
    public async Task Concurrent_writers_in_random_order_always_converge_on_the_newest_reading()
    {
        var sensor = Guid.NewGuid();
        var store = platform.CreateLastValueStore();
        // 8 escritores gravam os mesmos 200 timestamps em ordens embaralhadas diferentes
        var tasks = Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
        {
            foreach (var i in Enumerable.Range(0, 200).OrderBy(_ => Random.Shared.Next()))
                await store.SetIfNewerAsync([R(sensor, i, i)], default);
        }));

        await Task.WhenAll(tasks);

        var stored = (await store.GetAsync(sensor, default))!.Value;
        Assert.Equal(199, stored.Value); // o script Lua é atômico: comparar-e-gravar não é interrompido
    }

    [Fact]
    public async Task A_batch_of_many_sensors_is_written_in_one_call_and_counts_only_real_updates()
    {
        var sensors = NewSensors(500);
        var store = platform.CreateLastValueStore();
        await store.SetIfNewerAsync(sensors.Select(s => R(s, 10, 1)).ToList(), default);

        // metade nova, metade velha
        var updated = await store.SetIfNewerAsync(
            sensors.Select((s, i) => R(s, i % 2 == 0 ? 20 : 5, 2)).ToList(), default);

        Assert.Equal(250, updated);
    }

    [Fact]
    public async Task GetMany_returns_only_sensors_that_exist()
    {
        var known = NewSensors(3);
        var unknown = Guid.NewGuid();
        var store = platform.CreateLastValueStore();
        await store.SetIfNewerAsync(known.Select((s, i) => R(s, 1, i)).ToList(), default);

        var result = await store.GetManyAsync([.. known, unknown], default);

        Assert.Equal(3, result.Count);
        Assert.DoesNotContain(unknown, result.Keys);
        Assert.Equal([0d, 1d, 2d], known.Select(k => result[k].Value));
    }

    [Fact]
    public async Task Keys_expire_so_retired_sensors_do_not_leave_garbage()
    {
        var sensor = Guid.NewGuid();
        await platform.CreateLastValueStore(ttlDays: 7).SetIfNewerAsync([R(sensor, 1, 1)], default);

        var ttl = await platform.Redis.GetDatabase().KeyTimeToLiveAsync($"test:last:{sensor:D}");

        Assert.NotNull(ttl);
        Assert.InRange(ttl!.Value, TimeSpan.FromDays(6.9), TimeSpan.FromDays(7));
    }
}

[Collection(PlatformCollection.Name)]
public class LatestValuesApiTests(PlatformFixture platform)
{
    private readonly HttpClient _http = platform.Api.CreateClient();
    private static readonly DateTimeOffset Now = DateTimeOffset.UtcNow;

    [Fact]
    public async Task Latest_returns_the_cached_value_with_its_age()
    {
        var sensor = Guid.NewGuid();
        await platform.CreateLastValueStore().SetIfNewerAsync([Reading.Create(sensor, DateTimeOffset.UtcNow.AddSeconds(-30), 21.5)], default);

        var response = await _http.GetAsync($"/api/sensors/{sensor}/latest");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal(21.5, body.RootElement.GetProperty("value").GetDouble());
        Assert.InRange(body.RootElement.GetProperty("ageSeconds").GetDouble(), 29, 40);
    }

    [Fact]
    public async Task Unknown_sensor_is_404()
    {
        Assert.Equal(HttpStatusCode.NotFound, (await _http.GetAsync($"/api/sensors/{Guid.NewGuid()}/latest")).StatusCode);
    }

    [Fact]
    public async Task Cache_miss_is_served_from_the_database_and_repopulates_redis()
    {
        var sensor = Guid.NewGuid();
        var ts = DateTimeOffset.UtcNow.AddMinutes(-2);
        await platform.CreateStore().InsertBatchAsync([Reading.Create(sensor, ts.AddSeconds(-10), 1), Reading.Create(sensor, ts, 2)], default);
        var key = $"test:last:{sensor:D}";
        Assert.False(await platform.Redis.GetDatabase().KeyExistsAsync(key)); // nunca passou pelo Redis

        var response = await _http.GetAsync($"/api/sensors/{sensor}/latest");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal(2, body.RootElement.GetProperty("value").GetDouble()); // a MAIS RECENTE, do banco
        Assert.True(await platform.Redis.GetDatabase().KeyExistsAsync(key)); // cache-aside repovoou
    }

    [Fact]
    public async Task Flushing_redis_loses_nothing_because_the_database_is_the_source_of_truth()
    {
        var sensor = Guid.NewGuid();
        await platform.CreateStore().InsertBatchAsync([Reading.Create(sensor, DateTimeOffset.UtcNow.AddMinutes(-1), 77)], default);
        await platform.CreateLastValueStore().SetIfNewerAsync([Reading.Create(sensor, DateTimeOffset.UtcNow.AddMinutes(-1), 77)], default);
        await platform.Redis.GetDatabase().KeyDeleteAsync($"test:last:{sensor:D}"); // "Redis reiniciou / chave expirou"

        var response = await _http.GetAsync($"/api/sensors/{sensor}/latest");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal(77, body.RootElement.GetProperty("value").GetDouble());
    }

    [Fact]
    public async Task Many_returns_the_known_sensors_only()
    {
        var a = Guid.NewGuid();
        var b = Guid.NewGuid();
        var store = platform.CreateLastValueStore();
        await store.SetIfNewerAsync([Reading.Create(a, DateTimeOffset.UtcNow.AddSeconds(-5), 1), Reading.Create(b, DateTimeOffset.UtcNow.AddSeconds(-5), 2)], default);

        var response = await _http.GetAsync($"/api/sensors/latest?ids={a},{b},{Guid.NewGuid()}");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal(2, body.RootElement.GetArrayLength());
    }

    [Theory]
    [InlineData("")]
    [InlineData("ids=")]
    [InlineData("ids=nao-e-guid")]
    public async Task Many_validates_the_ids_parameter(string query)
    {
        Assert.Equal(HttpStatusCode.BadRequest, (await _http.GetAsync($"/api/sensors/latest?{query}")).StatusCode);
    }

    [Fact]
    public async Task Many_rejects_more_than_the_maximum_number_of_ids()
    {
        var ids = string.Join(",", Enumerable.Range(0, 201).Select(_ => Guid.NewGuid()));

        Assert.Equal(HttpStatusCode.BadRequest, (await _http.GetAsync($"/api/sensors/latest?ids={ids}")).StatusCode);
    }

    [Fact]
    public async Task Readiness_is_unhealthy_when_redis_is_unreachable_but_liveness_stays_ok()
    {
        await using var factory = new ApiFactory(platform.BootstrapServers,
            postgresConnection: platform.ConnectionString, redisConnection: "localhost:1,abortConnect=false,connectTimeout=300");
        var http = factory.CreateClient();

        Assert.Equal(HttpStatusCode.OK, (await http.GetAsync("/health/live")).StatusCode);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await http.GetAsync("/health/ready")).StatusCode);
    }
}

[Collection(PlatformCollection.Name)]
public class LastValueConsumerTests(PlatformFixture platform)
{
    private static async Task<(KafkaBatchConsumer Consumer, KafkaDeadLetterSink Dlq)> StartAsync(
        KafkaOptions kafka, IReadingBatchHandler handler, string group)
    {
        var dlq = new KafkaDeadLetterSink(Options.Create(kafka));
        var consumer = new KafkaBatchConsumer("lastvalue", kafka,
            new BatchConsumerOptions { GroupId = group, MaxBatchSize = 1000, MaxWaitMs = 100, InitialRetryDelayMs = 100, MaxRetryDelayMs = 400 },
            handler, dlq, NullLogger.Instance);
        await consumer.StartAsync(default);
        return (consumer, dlq);
    }

    private UpdateLastValueHandler LastValueHandler(ILastValueStore? store = null) =>
        new(store ?? platform.CreateLastValueStore(), NullLogger<UpdateLastValueHandler>.Instance);

    [Fact]
    public async Task Stream_of_readings_ends_with_the_newest_value_of_each_sensor_in_redis()
    {
        var kafka = await platform.CreateIsolatedTopicsAsync();
        var sensors = NewSensors(20);
        await PublishAsync(kafka, Readings(sensors, 4000)); // valor = índice global; o mais novo de cada sensor é o de maior índice
        var (consumer, dlq) = await StartAsync(kafka, LastValueHandler(), $"g-{Guid.NewGuid():N}");
        try
        {
            var store = platform.CreateLastValueStore();
            await EventuallyAsync(async () => (await store.GetManyAsync(sensors, default)).Count == 20
                && consumer.Consumed >= 4000, "os 20 sensores têm último valor");

            var latest = await store.GetManyAsync(sensors, default);
            for (var s = 0; s < sensors.Count; s++)
                Assert.Equal(3980 + s, latest[sensors[s]].Value); // último índice congruente a s módulo 20
        }
        finally
        {
            await consumer.StopAsync(default);
            consumer.Dispose();
            dlq.Dispose();
        }
    }

    [Fact]
    public async Task Out_of_order_timestamps_on_the_stream_do_not_regress_redis()
    {
        var kafka = await platform.CreateIsolatedTopicsAsync();
        var sensor = Guid.NewGuid();
        var t = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        // mesma partição (mesma chave): o Kafka mantém a ordem de PUBLICAÇÃO, mas os timestamps vão e voltam
        await PublishAsync(kafka, [Message(sensor, t.AddSeconds(10), 100), Message(sensor, t.AddSeconds(3), 30), Message(sensor, t.AddSeconds(7), 70)]);
        var (consumer, dlq) = await StartAsync(kafka, LastValueHandler(), $"g-{Guid.NewGuid():N}");
        try
        {
            await EventuallyAsync(() => Task.FromResult(consumer.Consumed >= 3), "consumidas");

            var stored = (await platform.CreateLastValueStore().GetAsync(sensor, default))!.Value;
            Assert.Equal(100, stored.Value);
            Assert.Equal(t.AddSeconds(10), stored.Timestamp);
        }
        finally
        {
            await consumer.StopAsync(default);
            consumer.Dispose();
            dlq.Dispose();
        }
    }

    [Fact]
    public async Task Persistence_and_lastvalue_groups_read_the_same_topic_independently()
    {
        var kafka = await platform.CreateIsolatedTopicsAsync();
        var sensors = NewSensors(10);
        await PublishAsync(kafka, Readings(sensors, 1000));
        var persistence = new PersistReadingsHandler(platform.CreateStore(), NullLogger<PersistReadingsHandler>.Instance);
        var (c1, d1) = await StartAsync(kafka, persistence, $"persist-{Guid.NewGuid():N}");
        var (c2, d2) = await StartAsync(kafka, LastValueHandler(), $"last-{Guid.NewGuid():N}");
        try
        {
            await EventuallyAsync(async () => await platform.CountReadingsAsync(sensors) == 1000, "1000 linhas no banco");
            await EventuallyAsync(async () => (await platform.CreateLastValueStore().GetManyAsync(sensors, default)).Count == 10, "10 últimos valores no Redis");
        }
        finally
        {
            foreach (var c in new[] { c1, c2 }) { await c.StopAsync(default); c.Dispose(); }
            d1.Dispose();
            d2.Dispose();
        }
    }

    [Fact]
    public async Task A_redis_outage_is_retried_without_losing_or_committing_the_batch()
    {
        var kafka = await platform.CreateIsolatedTopicsAsync();
        var sensors = NewSensors(5);
        await PublishAsync(kafka, Readings(sensors, 500));
        var flaky = new FlakyStore(platform.CreateLastValueStore(), failures: 3);
        var (consumer, dlq) = await StartAsync(kafka, LastValueHandler(flaky), $"g-{Guid.NewGuid():N}");
        try
        {
            await EventuallyAsync(async () => (await platform.CreateLastValueStore().GetManyAsync(sensors, default)).Count == 5, "recuperou depois das falhas");

            Assert.Equal(3, consumer.TransientRetries);
            Assert.Equal(0, consumer.DeadLettered); // queda de infra NÃO vai para a DLQ
        }
        finally
        {
            await consumer.StopAsync(default);
            consumer.Dispose();
            dlq.Dispose();
        }
    }

    private sealed class FlakyStore(ILastValueStore inner, int failures) : ILastValueStore
    {
        private int _failures = failures;

        public Task<int> SetIfNewerAsync(IReadOnlyCollection<Reading> readings, CancellationToken cancellationToken) =>
            Interlocked.Decrement(ref _failures) >= 0
                ? throw new TimeoutException("redis fora do ar")
                : inner.SetIfNewerAsync(readings, cancellationToken);

        public Task<LastValue?> GetAsync(Guid sensorId, CancellationToken cancellationToken) => inner.GetAsync(sensorId, cancellationToken);

        public Task<IReadOnlyDictionary<Guid, LastValue>> GetManyAsync(IReadOnlyCollection<Guid> sensorIds, CancellationToken cancellationToken) =>
            inner.GetManyAsync(sensorIds, cancellationToken);
    }
}
