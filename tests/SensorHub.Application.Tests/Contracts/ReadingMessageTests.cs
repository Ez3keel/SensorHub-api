using System.Text;
using SensorHub.Application.Contracts;
using SensorHub.Domain.Common;

namespace SensorHub.Application.Tests.Contracts;

public class ReadingMessageTests
{
    private static readonly DateTimeOffset Ts = new(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Roundtrip_preserves_all_fields()
    {
        var original = new ReadingMessage(1, Guid.NewGuid(), Ts.AddTicks(120), 42.125, "°C", Ts.AddSeconds(1));

        var back = ReadingMessageSerializer.Deserialize(ReadingMessageSerializer.Serialize(original));

        Assert.Equal(original, back);
    }

    [Fact]
    public void Wire_format_is_camel_case_and_omits_null_unit()
    {
        var message = new ReadingMessage(1, Guid.Parse("11111111-1111-1111-1111-111111111111"), Ts, 1.5, null, Ts);

        var json = Encoding.UTF8.GetString(ReadingMessageSerializer.Serialize(message));

        Assert.Contains("\"sensorId\":\"11111111-1111-1111-1111-111111111111\"", json);
        Assert.Contains("\"schemaVersion\":1", json);
        Assert.DoesNotContain("unit", json);
    }

    [Fact]
    public void Deserialize_tolerates_unknown_fields_from_newer_producers()
    {
        var json = """{"schemaVersion":2,"sensorId":"11111111-1111-1111-1111-111111111111","timestamp":"2026-06-01T12:00:00+00:00","value":1,"ingestedAt":"2026-06-01T12:00:00+00:00","futureField":"x"}""";

        var message = ReadingMessageSerializer.Deserialize(Encoding.UTF8.GetBytes(json));

        Assert.NotNull(message);
        Assert.Equal(2, message.SchemaVersion);
    }

    [Fact]
    public void ToReading_reapplies_domain_invariants()
    {
        var invalid = new ReadingMessage(1, Guid.Empty, Ts, 1, null, Ts);

        Assert.Throws<DomainException>(() => invalid.ToReading());
    }
}
