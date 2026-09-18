using Microsoft.Extensions.Time.Testing;
using SensorHub.Application.Queries;

namespace SensorHub.Application.Tests.Queries;

public class ReadingQueryServiceTests
{
    private static readonly DateTimeOffset Now = new(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);
    private static readonly Guid Sensor = Guid.NewGuid();

    private readonly CapturingQueries _queries = new();
    private readonly ReadingQueryService _service;

    public ReadingQueryServiceTests() => _service = new ReadingQueryService(_queries, new FakeTimeProvider(Now));

    [Fact]
    public async Task Defaults_to_the_last_hour_with_an_automatic_bucket()
    {
        await _service.GetSeriesAsync(Sensor, null, null, null, default);

        var request = Assert.Single(_queries.SeriesRequests);
        Assert.Equal(Now, request.To);
        Assert.Equal(Now.AddHours(-1), request.From);
        Assert.Equal(TimeSpan.FromMinutes(1), request.Bucket); // 60 pontos
    }

    [Theory]
    [InlineData(1, 1)]         // 1 h  -> 60 pontos      -> 1 min
    [InlineData(8, 1)]         // 8 h  -> 480 pontos     -> 1 min
    [InlineData(24, 5)]        // 24 h -> 288 (5 min)
    [InlineData(24 * 7, 30)]   // 7 d  -> 336 (30 min)
    [InlineData(24 * 30, 120)] // 30 d -> 720 (1 h) mas 120 min não é permitido: cai em 6 h (120 pontos)
    public async Task Auto_bucket_keeps_the_series_near_500_points(int hours, int expectedMinutesOrSpecial)
    {
        await _service.GetSeriesAsync(Sensor, Now.AddHours(-hours), Now, "auto", default);

        var bucket = _queries.SeriesRequests.Single().Bucket;
        var points = TimeSpan.FromHours(hours).Ticks / bucket.Ticks;
        Assert.True(points <= 500, $"{points} pontos com bucket {bucket}");
        Assert.Contains(bucket, ReadingQueryService.AllowedBuckets);
        if (hours <= 24 * 7) Assert.Equal(TimeSpan.FromMinutes(expectedMinutesOrSpecial), bucket);
    }

    [Theory]
    [InlineData("1m", 1)]
    [InlineData("5m", 5)]
    [InlineData("15M", 15)]
    [InlineData("1h", 60)]
    [InlineData("6h", 360)]
    [InlineData("1d", 1440)]
    public async Task Accepts_the_supported_buckets(string bucket, int minutes)
    {
        await _service.GetSeriesAsync(Sensor, Now.AddDays(2), Now.AddDays(3), bucket, default);

        Assert.Equal(TimeSpan.FromMinutes(minutes), _queries.SeriesRequests.Single().Bucket);
    }

    [Theory]
    [InlineData("2m")]
    [InlineData("30s")]
    [InlineData("1w")]
    [InlineData("abc")]
    [InlineData("-5m")]
    [InlineData("7h")]
    public async Task Rejects_unsupported_buckets_listing_the_valid_ones(string bucket)
    {
        var ex = await Assert.ThrowsAsync<QueryValidationException>(() =>
            _service.GetSeriesAsync(Sensor, Now.AddHours(-1), Now, bucket, default));

        Assert.Contains("1m", ex.Message);
        Assert.Empty(_queries.SeriesRequests); // nunca chegou ao banco
    }

    [Fact]
    public async Task Rejects_requests_that_would_return_too_many_points()
    {
        // 30 dias a 1 minuto = 43.200 pontos
        var ex = await Assert.ThrowsAsync<QueryValidationException>(() =>
            _service.GetSeriesAsync(Sensor, Now.AddDays(-30), Now, "1m", default));

        Assert.Contains("5000", ex.Message);
        Assert.Empty(_queries.SeriesRequests);
    }

    [Fact]
    public async Task Accepts_exactly_the_maximum_number_of_points()
    {
        // 5000 minutos = 5000 pontos
        await _service.GetSeriesAsync(Sensor, Now.AddMinutes(-5000), Now, "1m", default);

        Assert.Single(_queries.SeriesRequests);
    }

    [Fact]
    public async Task Rejects_inverted_or_empty_ranges()
    {
        await Assert.ThrowsAsync<QueryValidationException>(() => _service.GetSeriesAsync(Sensor, Now, Now.AddHours(-1), null, default));
        await Assert.ThrowsAsync<QueryValidationException>(() => _service.GetSeriesAsync(Sensor, Now, Now, null, default));
    }

    [Fact]
    public async Task Rejects_ranges_beyond_the_maximum()
    {
        await Assert.ThrowsAsync<QueryValidationException>(() =>
            _service.GetSeriesAsync(Sensor, Now.AddDays(-400), Now, "1d", default));
    }

    [Fact]
    public async Task Normalizes_the_range_to_utc()
    {
        var local = new DateTimeOffset(2026, 6, 1, 9, 0, 0, TimeSpan.FromHours(-3)); // = 12:00 UTC

        await _service.GetSeriesAsync(Sensor, local.AddHours(-2), local, "5m", default);

        var request = _queries.SeriesRequests.Single();
        Assert.Equal(TimeSpan.Zero, request.To.Offset);
        Assert.Equal(new DateTimeOffset(2026, 6, 1, 12, 0, 0, TimeSpan.Zero), request.To);
    }

    [Fact]
    public async Task Raw_defaults_and_limits_are_enforced()
    {
        await _service.GetRawAsync(Sensor, null, null, null, default);
        var call = _queries.RawCalls.Single();
        Assert.Equal(QueryLimits.DefaultRawPoints, call.Limit);
        Assert.Equal(Now.AddMinutes(-10), call.From);

        await Assert.ThrowsAsync<QueryValidationException>(() => _service.GetRawAsync(Sensor, null, null, 0, default));
        await Assert.ThrowsAsync<QueryValidationException>(() => _service.GetRawAsync(Sensor, null, null, QueryLimits.MaxRawPoints + 1, default));
        await _service.GetRawAsync(Sensor, null, null, QueryLimits.MaxRawPoints, default); // borda: aceito
    }

    [Theory]
    [InlineData("5m", true, 5)]
    [InlineData("1h", true, 60)]
    [InlineData("1d", true, 1440)]
    [InlineData("5", false, 0)]
    [InlineData("m5", false, 0)]
    [InlineData("", false, 0)]
    public void TryParseBucket_reads_amount_and_unit(string text, bool ok, int minutes)
    {
        Assert.Equal(ok, ReadingQueryService.TryParseBucket(text, out var bucket));
        if (ok) Assert.Equal(TimeSpan.FromMinutes(minutes), bucket);
    }

    [Fact]
    public void FormatBucket_roundtrips_allowed_buckets()
    {
        foreach (var bucket in ReadingQueryService.AllowedBuckets)
        {
            Assert.True(ReadingQueryService.TryParseBucket(ReadingQueryService.FormatBucket(bucket), out var parsed));
            Assert.Equal(bucket, parsed);
        }
    }

    private sealed class CapturingQueries : IReadingQueries
    {
        public List<SeriesRequest> SeriesRequests { get; } = [];
        public List<(DateTimeOffset From, int Limit)> RawCalls { get; } = [];

        public Task<IReadOnlyList<SeriesPoint>> GetSeriesAsync(SeriesRequest request, CancellationToken cancellationToken)
        {
            SeriesRequests.Add(request);
            return Task.FromResult<IReadOnlyList<SeriesPoint>>([]);
        }

        public Task<RawPoint?> GetLatestAsync(Guid sensorId, CancellationToken cancellationToken) => Task.FromResult<RawPoint?>(null);

        public Task<IReadOnlyList<RawPoint>> GetRawAsync(Guid sensorId, DateTimeOffset from, DateTimeOffset to, int limit, CancellationToken cancellationToken)
        {
            RawCalls.Add((from, limit));
            return Task.FromResult<IReadOnlyList<RawPoint>>([]);
        }
    }
}
