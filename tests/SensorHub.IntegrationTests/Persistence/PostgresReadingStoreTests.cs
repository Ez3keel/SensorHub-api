using System.Diagnostics;
using SensorHub.Domain.Readings;
using SensorHub.Infrastructure.Persistence;
using SensorHub.IntegrationTests.Infrastructure;
using Xunit.Abstractions;

namespace SensorHub.IntegrationTests.Persistence;

[Collection(PlatformCollection.Name)]
public class PostgresReadingStoreTests(PlatformFixture platform, ITestOutputHelper output)
{
    private static readonly DateTimeOffset T0 = new(2026, 3, 1, 12, 0, 0, TimeSpan.Zero);

    public static TheoryData<BulkInsertStrategy> Strategies => new() { BulkInsertStrategy.CopyStaging, BulkInsertStrategy.Unnest };

    private static Reading R(Guid sensor, int msOffset, double value) => Reading.Create(sensor, T0.AddMilliseconds(msOffset), value);

    [Theory, MemberData(nameof(Strategies))]
    public async Task Inserts_batch_and_reports_inserted_count(BulkInsertStrategy strategy)
    {
        var sensor = Guid.NewGuid();
        var store = platform.CreateStore(strategy);

        var inserted = await store.InsertBatchAsync(Enumerable.Range(0, 100).Select(i => R(sensor, i, i)).ToList(), default);

        Assert.Equal(100, inserted);
        Assert.Equal(100, await platform.CountReadingsAsync([sensor]));
    }

    [Theory, MemberData(nameof(Strategies))]
    public async Task Empty_batch_is_a_noop(BulkInsertStrategy strategy)
    {
        Assert.Equal(0, await platform.CreateStore(strategy).InsertBatchAsync([], default));
    }

    [Theory, MemberData(nameof(Strategies))]
    public async Task Duplicates_inside_the_same_batch_are_stored_once(BulkInsertStrategy strategy)
    {
        var sensor = Guid.NewGuid();
        var store = platform.CreateStore(strategy);

        var inserted = await store.InsertBatchAsync([R(sensor, 0, 1), R(sensor, 0, 1), R(sensor, 0, 1), R(sensor, 1, 2)], default);

        Assert.Equal(2, inserted);
        Assert.Equal(2, await platform.CountReadingsAsync([sensor]));
    }

    [Theory, MemberData(nameof(Strategies))]
    public async Task Reinserting_the_same_batch_is_idempotent(BulkInsertStrategy strategy)
    {
        var sensor = Guid.NewGuid();
        var store = platform.CreateStore(strategy);
        var batch = Enumerable.Range(0, 50).Select(i => R(sensor, i, i)).ToList();

        Assert.Equal(50, await store.InsertBatchAsync(batch, default));
        Assert.Equal(0, await store.InsertBatchAsync(batch, default)); // reentrega do Kafka

        Assert.Equal(50, await platform.CountReadingsAsync([sensor]));
    }

    [Theory, MemberData(nameof(Strategies))]
    public async Task Partially_overlapping_batch_inserts_only_the_new_rows(BulkInsertStrategy strategy)
    {
        var sensor = Guid.NewGuid();
        var store = platform.CreateStore(strategy);
        await store.InsertBatchAsync(Enumerable.Range(0, 10).Select(i => R(sensor, i, i)).ToList(), default);

        var inserted = await store.InsertBatchAsync(Enumerable.Range(5, 10).Select(i => R(sensor, i, i)).ToList(), default);

        Assert.Equal(5, inserted);
        Assert.Equal(15, await platform.CountReadingsAsync([sensor]));
    }

    [Theory, MemberData(nameof(Strategies))]
    public async Task First_write_wins_when_the_same_key_arrives_with_a_different_value(BulkInsertStrategy strategy)
    {
        var sensor = Guid.NewGuid();
        var store = platform.CreateStore(strategy);
        await store.InsertBatchAsync([R(sensor, 0, 10)], default);

        await store.InsertBatchAsync([R(sensor, 0, 999)], default);

        await using var cmd = platform.DataSource.CreateCommand("SELECT value FROM readings WHERE sensor_id = @id");
        cmd.Parameters.AddWithValue("id", sensor);
        Assert.Equal(10d, (double)(await cmd.ExecuteScalarAsync())!);
    }

    [Theory, MemberData(nameof(Strategies))]
    public async Task Timestamp_and_value_survive_the_roundtrip_exactly(BulkInsertStrategy strategy)
    {
        var sensor = Guid.NewGuid();
        var ts = new DateTimeOffset(2026, 3, 1, 12, 30, 45, TimeSpan.FromHours(-3)).AddTicks(1234560); // µs, com fuso
        var reading = Reading.Create(sensor, ts, 0.1 + 0.2);

        await platform.CreateStore(strategy).InsertBatchAsync([reading], default);

        await using var cmd = platform.DataSource.CreateCommand("SELECT ts, value FROM readings WHERE sensor_id = @id");
        cmd.Parameters.AddWithValue("id", sensor);
        await using var rows = await cmd.ExecuteReaderAsync();
        Assert.True(await rows.ReadAsync());
        Assert.Equal(reading.Timestamp, new DateTimeOffset(rows.GetDateTime(0), TimeSpan.Zero));
        Assert.Equal(reading.Value, rows.GetDouble(1)); // igualdade EXATA de double
    }

    [Theory, MemberData(nameof(Strategies))]
    public async Task Concurrent_writers_with_overlapping_keys_neither_deadlock_nor_duplicate(BulkInsertStrategy strategy)
    {
        var sensors = TestHelpers.NewSensors(20);
        var store = platform.CreateStore(strategy);
        // 6 escritores gravam o MESMO conjunto de chaves, em ordens de origem diferentes
        var batches = Enumerable.Range(0, 6).Select(w =>
            sensors.SelectMany(s => Enumerable.Range(0, 100).Select(i => R(s, i, i))).OrderBy(_ => Random.Shared.Next()).ToList()).ToList();

        await Task.WhenAll(batches.Select(b => store.InsertBatchAsync(b, default)));

        Assert.Equal(sensors.Count * 100, await platform.CountReadingsAsync(sensors));
    }

    [Fact]
    public async Task Benchmark_bulk_insert_strategies()
    {
        const int batchSize = 5000;
        const int batches = 20; // 100 mil linhas por estratégia
        var results = new List<(BulkInsertStrategy Strategy, double Rate)>();

        foreach (var strategy in new[] { BulkInsertStrategy.CopyStaging, BulkInsertStrategy.Unnest, BulkInsertStrategy.CopyStaging, BulkInsertStrategy.Unnest })
        {
            var sensors = TestHelpers.NewSensors(500);
            var store = platform.CreateStore(strategy);
            var all = Enumerable.Range(0, batchSize * batches).Select(i => R(sensors[i % sensors.Count], i, i)).ToList();

            var clock = Stopwatch.StartNew();
            foreach (var chunk in all.Chunk(batchSize))
                await store.InsertBatchAsync(chunk, default);
            clock.Stop();

            var rate = all.Count / clock.Elapsed.TotalSeconds;
            results.Add((strategy, rate));
            Assert.Equal(all.Count, await platform.CountReadingsAsync(sensors));
        }

        // a 1ª rodada de cada estratégia inclui aquecimento (JIT, pool); a 2ª é a mais representativa
        foreach (var (strategy, rate) in results)
            output.WriteLine($"[bench] {strategy,-12} {rate,10:N0} linhas/s (lotes de {batchSize})");
    }
}
