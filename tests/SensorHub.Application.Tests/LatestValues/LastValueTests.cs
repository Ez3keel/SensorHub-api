using Microsoft.Extensions.Logging.Abstractions;
using SensorHub.Application.LatestValues;
using SensorHub.Application.Processing;
using SensorHub.Application.Queries;
using SensorHub.Domain.Readings;

namespace SensorHub.Application.Tests.LatestValues;

public class UpdateLastValueHandlerTests
{
    private static readonly DateTimeOffset T0 = new(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);

    private static ConsumedReading C(Guid sensor, int seconds, double value) =>
        new(Reading.Create(sensor, T0.AddSeconds(seconds), value), T0);

    [Fact]
    public async Task Reduces_the_batch_to_the_newest_reading_per_sensor()
    {
        var a = Guid.NewGuid();
        var b = Guid.NewGuid();
        var store = new FakeStore();
        var handler = new UpdateLastValueHandler(store, NullLogger<UpdateLastValueHandler>.Instance);

        // 5000 leituras de 2 sensores devem virar apenas 2 escritas no Redis
        var batch = Enumerable.Range(0, 2500).SelectMany(i => new[] { C(a, i, i), C(b, i, -i) }).ToList();
        await handler.HandleAsync(batch, default);

        var written = Assert.Single(store.Writes);
        Assert.Equal(2, written.Count);
        Assert.Equal(2499, written.Single(r => r.SensorId == a).Value);
        Assert.Equal(-2499, written.Single(r => r.SensorId == b).Value);
    }

    [Fact]
    public async Task Picks_the_newest_by_timestamp_not_by_position_in_the_batch()
    {
        var a = Guid.NewGuid();
        var store = new FakeStore();
        var handler = new UpdateLastValueHandler(store, NullLogger<UpdateLastValueHandler>.Instance);

        await handler.HandleAsync([C(a, 10, 100), C(a, 5, 50), C(a, 7, 70)], default); // chegaram fora de ordem

        Assert.Equal(100, store.Writes.Single().Single().Value);
    }

    [Fact]
    public async Task Store_failure_propagates_so_the_consumer_retries_without_committing()
    {
        var store = new FakeStore { Failure = new InvalidOperationException("redis fora") };
        var handler = new UpdateLastValueHandler(store, NullLogger<UpdateLastValueHandler>.Instance);

        await Assert.ThrowsAsync<InvalidOperationException>(() => handler.HandleAsync([C(Guid.NewGuid(), 1, 1)], default));
    }

    private sealed class FakeStore : ILastValueStore
    {
        public List<IReadOnlyCollection<Reading>> Writes { get; } = [];
        public Exception? Failure { get; init; }

        public Task<int> SetIfNewerAsync(IReadOnlyCollection<Reading> readings, CancellationToken cancellationToken)
        {
            if (Failure is not null) throw Failure;
            Writes.Add(readings);
            return Task.FromResult(readings.Count);
        }

        public Task<LastValue?> GetAsync(Guid sensorId, CancellationToken cancellationToken) => Task.FromResult<LastValue?>(null);

        public Task<IReadOnlyDictionary<Guid, LastValue>> GetManyAsync(IReadOnlyCollection<Guid> sensorIds, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyDictionary<Guid, LastValue>>(new Dictionary<Guid, LastValue>());
    }
}

public class LastValueServiceTests
{
    private static readonly DateTimeOffset T0 = new(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Cache_hit_never_touches_the_database()
    {
        var sensor = Guid.NewGuid();
        var cache = new InMemoryStore();
        cache.Data[sensor] = new LastValue(sensor, T0, 42);
        var db = new FakeQueries();
        var service = new LastValueService(cache, db);

        var value = await service.GetAsync(sensor, default);

        Assert.Equal(42, value!.Value.Value);
        Assert.Equal(0, db.LatestCalls);
    }

    [Fact]
    public async Task Cache_miss_falls_back_to_the_database_and_repopulates_the_cache()
    {
        var sensor = Guid.NewGuid();
        var cache = new InMemoryStore();
        var db = new FakeQueries { Latest = { [sensor] = new RawPoint(T0, 7.5) } };
        var service = new LastValueService(cache, db);

        var first = await service.GetAsync(sensor, default);
        var second = await service.GetAsync(sensor, default);

        Assert.Equal(7.5, first!.Value.Value);
        Assert.Equal(7.5, second!.Value.Value);
        Assert.Equal(1, db.LatestCalls); // a 2ª leitura veio do cache
        Assert.True(cache.Data.ContainsKey(sensor));
    }

    [Fact]
    public async Task Unknown_sensor_returns_null_and_does_not_cache_a_negative_result()
    {
        var cache = new InMemoryStore();
        var service = new LastValueService(cache, new FakeQueries());

        Assert.Null(await service.GetAsync(Guid.NewGuid(), default));
        Assert.Empty(cache.Data);
    }

    [Fact]
    public async Task GetMany_mixes_hits_and_database_fallbacks_and_omits_unknown_sensors()
    {
        var hit = Guid.NewGuid();
        var miss = Guid.NewGuid();
        var unknown = Guid.NewGuid();
        var cache = new InMemoryStore();
        cache.Data[hit] = new LastValue(hit, T0, 1);
        var db = new FakeQueries { Latest = { [miss] = new RawPoint(T0.AddSeconds(5), 2) } };
        var service = new LastValueService(cache, db);

        var result = await service.GetManyAsync([hit, miss, unknown, hit], default);

        Assert.Equal(2, result.Count);
        Assert.Equal(1, result[hit].Value);
        Assert.Equal(2, result[miss].Value);
        Assert.Equal(2, db.LatestCalls); // só os que faltaram no cache
    }

    private sealed class InMemoryStore : ILastValueStore
    {
        public Dictionary<Guid, LastValue> Data { get; } = [];

        public Task<int> SetIfNewerAsync(IReadOnlyCollection<Reading> readings, CancellationToken cancellationToken)
        {
            var updated = 0;
            foreach (var r in readings)
            {
                if (Data.TryGetValue(r.SensorId, out var current) && current.Timestamp >= r.Timestamp) continue;
                Data[r.SensorId] = new LastValue(r.SensorId, r.Timestamp, r.Value);
                updated++;
            }

            return Task.FromResult(updated);
        }

        public Task<LastValue?> GetAsync(Guid sensorId, CancellationToken cancellationToken) =>
            Task.FromResult<LastValue?>(Data.TryGetValue(sensorId, out var v) ? v : null);

        public Task<IReadOnlyDictionary<Guid, LastValue>> GetManyAsync(IReadOnlyCollection<Guid> sensorIds, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyDictionary<Guid, LastValue>>(
                sensorIds.Where(Data.ContainsKey).Distinct().ToDictionary(id => id, id => Data[id]));
    }

    private sealed class FakeQueries : IReadingQueries
    {
        public Dictionary<Guid, RawPoint> Latest { get; } = [];
        public int LatestCalls { get; private set; }

        public Task<RawPoint?> GetLatestAsync(Guid sensorId, CancellationToken cancellationToken)
        {
            LatestCalls++;
            return Task.FromResult<RawPoint?>(Latest.TryGetValue(sensorId, out var p) ? p : null);
        }

        public Task<IReadOnlyList<SeriesPoint>> GetSeriesAsync(SeriesRequest request, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<SeriesPoint>>([]);

        public Task<IReadOnlyList<RawPoint>> GetRawAsync(Guid sensorId, DateTimeOffset from, DateTimeOffset to, int limit, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<RawPoint>>([]);
    }
}
