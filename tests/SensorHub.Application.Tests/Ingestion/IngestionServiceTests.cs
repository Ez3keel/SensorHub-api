using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using SensorHub.Application.Contracts;
using SensorHub.Application.Ingestion;

namespace SensorHub.Application.Tests.Ingestion;

[Collection("telemetry")]
public class IngestionServiceTests
{
    private static readonly DateTimeOffset Now = new(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);
    private static readonly Guid Sensor = Guid.NewGuid();

    private readonly FakeTimeProvider _clock = new(Now);
    private readonly RecordingPublisher _publisher = new();
    private readonly IngestionService _service;

    public IngestionServiceTests()
    {
        var options = Options.Create(new IngestionOptions());
        _service = new IngestionService(new ReadingRequestValidator(options, _clock), _publisher, _clock);
    }

    [Fact]
    public async Task Valid_reading_is_published_with_schema_version_and_ingest_time()
    {
        var result = await _service.IngestAsync([new(Sensor, Now.AddSeconds(-1), 21.5, "°C")], default);

        Assert.Equal(1, result.Accepted);
        Assert.Empty(result.Rejected);
        var message = Assert.Single(_publisher.Published);
        Assert.Equal(ReadingMessage.CurrentSchemaVersion, message.SchemaVersion);
        Assert.Equal(Sensor, message.SensorId);
        Assert.Equal(21.5, message.Value);
        Assert.Equal("°C", message.Unit);
        Assert.Equal(Now, message.IngestedAt);
    }

    [Fact]
    public async Task Missing_timestamp_defaults_to_server_time()
    {
        await _service.IngestAsync([new(Sensor, null, 1, null)], default);

        Assert.Equal(Now, _publisher.Published.Single().Timestamp);
    }

    [Fact]
    public async Task Timestamp_is_normalized_to_utc_microseconds()
    {
        var local = new DateTimeOffset(2026, 6, 1, 9, 0, 0, TimeSpan.FromHours(-3)).AddTicks(7);

        await _service.IngestAsync([new(Sensor, local, 1, null)], default);

        var published = _publisher.Published.Single().Timestamp;
        Assert.Equal(TimeSpan.Zero, published.Offset);
        Assert.Equal(0, published.Ticks % 10);
    }

    [Fact]
    public async Task Invalid_readings_are_rejected_individually_and_valid_ones_still_published()
    {
        var batch = new List<ReadingRequest>
        {
            new(Sensor, Now, 1, null),                    // ok
            new(Guid.Empty, Now, 1, null),                // sensorId vazio
            new(Sensor, Now, double.NaN, null),           // NaN
            new(Sensor, Now.AddHours(1), 1, null),        // futuro demais
            new(Sensor, Now.AddDays(-30), 1, null),       // antigo demais
            new(Sensor, Now, 2, new string('x', 17)),     // unit longa
            new(Sensor, Now.AddSeconds(1), 3, "%"),       // ok
        };

        var result = await _service.IngestAsync(batch, default);

        Assert.Equal(2, result.Accepted);
        Assert.Equal([1, 2, 3, 4, 5], result.Rejected.Select(r => r.Index));
        Assert.Equal([1d, 3d], _publisher.Published.Select(m => m.Value));
    }

    [Fact]
    public async Task Rejection_carries_human_readable_errors()
    {
        var result = await _service.IngestAsync([new(Guid.Empty, Now, double.PositiveInfinity, null)], default);

        var rejected = Assert.Single(result.Rejected);
        Assert.Contains(rejected.Errors, e => e.Contains("sensorId"));
        Assert.Contains(rejected.Errors, e => e.Contains("finito"));
    }

    [Fact]
    public async Task Nothing_is_published_when_every_reading_is_invalid()
    {
        var result = await _service.IngestAsync([new(Guid.Empty, Now, 1, null)], default);

        Assert.Equal(0, result.Accepted);
        Assert.Equal(0, _publisher.Calls);
    }

    [Fact]
    public async Task Whole_batch_goes_to_the_publisher_in_a_single_call()
    {
        var batch = Enumerable.Range(0, 500).Select(i => new ReadingRequest(Sensor, Now.AddMilliseconds(i), i, null)).ToList();

        await _service.IngestAsync(batch, default);

        Assert.Equal(1, _publisher.Calls);
        Assert.Equal(500, _publisher.Published.Count);
    }

    [Fact]
    public async Task Publisher_failure_propagates_so_the_client_can_retry()
    {
        _publisher.Failure = new IngestionUnavailableException("broker fora");

        await Assert.ThrowsAsync<IngestionUnavailableException>(() =>
            _service.IngestAsync([new(Sensor, Now, 1, null)], default));
    }

    [Fact]
    public async Task Timestamp_within_future_skew_is_accepted()
    {
        var result = await _service.IngestAsync([new(Sensor, Now.AddMinutes(4), 1, null)], default);

        Assert.Equal(1, result.Accepted);
    }

    private sealed class RecordingPublisher : IReadingPublisher
    {
        public List<ReadingMessage> Published { get; } = [];
        public int Calls { get; private set; }
        public Exception? Failure { get; set; }

        public Task PublishAsync(IReadOnlyList<ReadingMessage> messages, CancellationToken cancellationToken)
        {
            Calls++;
            if (Failure is not null) throw Failure;
            Published.AddRange(messages);
            return Task.CompletedTask;
        }
    }
}
