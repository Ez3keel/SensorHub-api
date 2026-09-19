using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SensorHub.Application.Contracts;
using SensorHub.Application.Ingestion;
using SensorHub.Application.Security;
using SensorHub.Domain.Sensors;
using SensorHub.MqttBridge;

namespace SensorHub.IntegrationTests.Mqtt;

/// <summary>Regras do bridge que não precisam de broker nem de containers.</summary>
public class MqttTopicsTests
{
    private static readonly Guid Device = Guid.Parse("11111111-2222-3333-4444-555555555555");

    [Fact]
    public void The_readings_topic_round_trips()
    {
        Assert.True(MqttTopics.TryGetDeviceId(MqttTopics.ReadingsTopic(Device), out var parsed));
        Assert.Equal(Device, parsed);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("sensorhub/devices/readings")]
    [InlineData("sensorhub/devices/nao-e-guid/readings")]
    [InlineData("sensorhub/devices/11111111-2222-3333-4444-555555555555/readings/extra")]
    [InlineData("sensorhub/devices/11111111-2222-3333-4444-555555555555/commands")]
    [InlineData("outro/devices/11111111-2222-3333-4444-555555555555/readings")]
    [InlineData("sensorhub/devices/+/readings")]
    [InlineData("sensorhub/devices/11111111222233334444555555555555/readings")] // formato "N": só aceitamos o canônico
    public void Anything_outside_the_scheme_is_not_a_device_topic(string? topic) =>
        Assert.False(MqttTopics.TryGetDeviceId(topic, out _));
}

public class ReadingPayloadTests
{
    private static PayloadParseResult Parse(string json, int max = 1000) => ReadingPayload.Parse(System.Text.Encoding.UTF8.GetBytes(json), max);
    private const string One = """{"sensorId":"11111111-2222-3333-4444-555555555555","timestamp":"2026-01-01T00:00:00Z","value":21.5,"unit":"°C"}""";

    [Fact]
    public void A_single_object_is_one_reading()
    {
        var result = Parse(One);

        Assert.True(result.IsValid);
        var reading = Assert.Single(result.Readings!);
        Assert.Equal(21.5, reading.Value);
        Assert.Equal("°C", reading.Unit);
    }

    [Fact]
    public void The_timestamp_is_optional_so_devices_without_a_clock_can_publish()
    {
        var result = Parse("""{"sensorId":"11111111-2222-3333-4444-555555555555","value":1}""");

        Assert.Null(Assert.Single(result.Readings!).Timestamp);
    }

    [Fact]
    public void An_array_is_a_batch() => Assert.Equal(3, Parse($"[{One},{One},{One}]").Readings!.Count);

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("isto não é json")]
    [InlineData("42")]
    [InlineData("\"texto\"")]
    [InlineData("null")]
    [InlineData("[]")]
    [InlineData("[null]")]
    [InlineData("{\"sensorId\":\"nao-e-guid\",\"value\":1}")]
    [InlineData("{\"value\":\"nao-e-numero\"}")]
    [InlineData("{")]
    public void Anything_that_is_not_a_reading_or_a_batch_is_invalid(string json)
    {
        var result = Parse(json);

        Assert.False(result.IsValid);
        Assert.False(string.IsNullOrWhiteSpace(result.Error));
    }

    [Fact]
    public void A_batch_over_the_limit_is_invalid() => Assert.False(Parse($"[{One},{One},{One}]", max: 2).IsValid);
}

public class MqttAccessPolicyTests
{
    private static readonly Guid Me = Guid.Parse("11111111-2222-3333-4444-555555555555");
    private static readonly Guid Other = Guid.Parse("99999999-2222-3333-4444-555555555555");

    [Fact]
    public void A_device_may_only_write_to_its_own_readings_topic()
    {
        Assert.True(MqttAccessPolicy.DeviceMayAccess(Me.ToString(), MqttTopics.ReadingsTopic(Me), MqttAccessPolicy.AccessWrite));

        Assert.False(MqttAccessPolicy.DeviceMayAccess(Me.ToString(), MqttTopics.ReadingsTopic(Other), MqttAccessPolicy.AccessWrite)); // topico alheio
        Assert.False(MqttAccessPolicy.DeviceMayAccess(Me.ToString(), MqttTopics.ReadingsTopic(Me), MqttAccessPolicy.AccessRead));      // não lê
        Assert.False(MqttAccessPolicy.DeviceMayAccess(Me.ToString(), MqttTopics.ReadingsTopic(Me), MqttAccessPolicy.AccessSubscribe)); // não assina
        Assert.False(MqttAccessPolicy.DeviceMayAccess(Me.ToString(), "#", MqttAccessPolicy.AccessWrite));
        Assert.False(MqttAccessPolicy.DeviceMayAccess("nao-e-guid", MqttTopics.ReadingsTopic(Me), MqttAccessPolicy.AccessWrite));
    }

    [Fact]
    public async Task The_bridge_authenticates_only_with_its_own_secret()
    {
        var policy = new MqttAccessPolicy(Options.Create(new MqttBridgeOptions { BridgeUsername = "bridge", BridgePassword = "segredo-do-bridge-123456" }), new FakeAuthenticator(null));

        Assert.True(await policy.AuthenticateAsync("bridge", "segredo-do-bridge-123456", default));
        Assert.False(await policy.AuthenticateAsync("bridge", "segredo-do-bridge-123457", default));
        Assert.False(await policy.AuthenticateAsync("bridge", "", default));
        Assert.True(policy.IsBridge("bridge"));
        Assert.False(policy.IsBridge("alguem"));
    }

    [Fact]
    public async Task A_device_needs_a_key_that_belongs_to_that_very_device()
    {
        var policy = new MqttAccessPolicy(Options.Create(new MqttBridgeOptions { BridgePassword = "segredo-do-bridge-123456" }),
            new FakeAuthenticator(new DeviceIdentity(Me, "meu")));

        Assert.True(await policy.AuthenticateAsync(Me.ToString(), "shk_qualquer", default));
        Assert.False(await policy.AuthenticateAsync(Other.ToString(), "shk_qualquer", default)); // a chave é do Me, não do Other
        Assert.False(await policy.AuthenticateAsync(Me.ToString(), null, default));
    }

    [Fact]
    public async Task An_unknown_key_never_authenticates()
    {
        var policy = new MqttAccessPolicy(Options.Create(new MqttBridgeOptions { BridgePassword = "segredo-do-bridge-123456" }), new FakeAuthenticator(null));

        Assert.False(await policy.AuthenticateAsync(Me.ToString(), "shk_qualquer", default));
    }

    [Theory]
    [InlineData("", false)]
    [InlineData("curta", false)]
    [InlineData("segredo-do-bridge-123456", true)]
    public void The_host_refuses_to_start_with_a_weak_bridge_secret(string secret, bool ok)
    {
        var options = new MqttBridgeOptions { BridgePassword = secret };

        if (ok) options.Validate(isDevelopment: false);
        else Assert.Throws<InvalidOperationException>(() => options.Validate(isDevelopment: false));
    }

    [Fact]
    public void The_development_secret_is_refused_outside_development()
    {
        var options = new MqttBridgeOptions { BridgePassword = "dev-only-bridge-secret-0123456789" };

        options.Validate(isDevelopment: true);
        Assert.Throws<InvalidOperationException>(() => options.Validate(isDevelopment: false));
    }

    [Fact]
    public void Roles_default_to_both_and_can_be_split()
    {
        Assert.True(new MqttBridgeOptions().HasRole("auth"));
        Assert.True(new MqttBridgeOptions().HasRole("bridge"));
        Assert.True(new MqttBridgeOptions { Roles = ["AUTH"] }.HasRole("auth"));
        Assert.False(new MqttBridgeOptions { Roles = ["auth"] }.HasRole("bridge"));
    }

    private sealed class FakeAuthenticator(DeviceIdentity? identity) : IDeviceAuthenticator
    {
        public Task<DeviceIdentity?> AuthenticateAsync(string apiKey, CancellationToken cancellationToken) => Task.FromResult(identity);
    }
}

/// <summary>O destino de cada mensagem (ack x reentrega) com um publicador de Kafka falso, sem broker MQTT.</summary>
public class MqttBridgeProcessingTests
{
    private static readonly Guid DeviceId = Guid.Parse("11111111-2222-3333-4444-555555555555");
    private static readonly Guid SensorId = Guid.Parse("aaaaaaaa-2222-3333-4444-555555555555");
    private static readonly Guid OtherSensor = Guid.Parse("bbbbbbbb-2222-3333-4444-555555555555");

    private readonly FakePublisher _publisher = new();

    private MqttBridgeService Service(int retries = 2)
    {
        var clock = TimeProvider.System;
        var ingestion = new IngestionService(
            new ReadingRequestValidator(Options.Create(new IngestionOptions()), clock), _publisher, clock, new FakeRegistry());
        return new MqttBridgeService(ingestion, Options.Create(new MqttBridgeOptions { PublishRetries = retries }), NullLogger<MqttBridgeService>.Instance);
    }

    private static byte[] Body(Guid sensor, double value = 20) =>
        System.Text.Encoding.UTF8.GetBytes($$"""{"sensorId":"{{sensor}}","timestamp":"{{DateTimeOffset.UtcNow.AddSeconds(-1):O}}","value":{{value}},"unit":"°C"}""");

    [Fact]
    public async Task A_valid_message_is_published_and_acknowledged()
    {
        var result = await Service().ProcessAsync(MqttTopics.ReadingsTopic(DeviceId), Body(SensorId), default);

        Assert.Equal(MessageDisposition.Ack, result);
        Assert.Single(_publisher.Published);
    }

    [Theory]
    [InlineData("isto não é json")]
    [InlineData("[]")]
    public async Task A_malformed_message_is_acknowledged_and_dropped_never_redelivered(string body)
    {
        var result = await Service().ProcessAsync(MqttTopics.ReadingsTopic(DeviceId), System.Text.Encoding.UTF8.GetBytes(body), default);

        Assert.Equal(MessageDisposition.Ack, result); // reentregar uma mensagem venenosa travaria a fila do dispositivo
        Assert.Empty(_publisher.Published);
    }

    [Fact]
    public async Task A_topic_outside_the_scheme_is_dropped()
    {
        Assert.Equal(MessageDisposition.Ack, await Service().ProcessAsync("qualquer/coisa", Body(SensorId), default));
        Assert.Empty(_publisher.Published);
    }

    [Fact]
    public async Task A_reading_for_a_sensor_of_another_device_is_rejected_but_the_valid_ones_in_the_batch_pass()
    {
        var now = DateTimeOffset.UtcNow.AddSeconds(-1);
        var batch = System.Text.Encoding.UTF8.GetBytes(
            $$"""[{"sensorId":"{{SensorId}}","timestamp":"{{now:O}}","value":1,"unit":"°C"},{"sensorId":"{{OtherSensor}}","timestamp":"{{now:O}}","value":2,"unit":"°C"}]""");

        var result = await Service().ProcessAsync(MqttTopics.ReadingsTopic(DeviceId), batch, default);

        Assert.Equal(MessageDisposition.Ack, result);
        Assert.Equal(SensorId, Assert.Single(_publisher.Published).SensorId);
    }

    [Fact]
    public async Task A_payload_over_the_size_limit_is_dropped()
    {
        var service = new MqttBridgeService(
            new IngestionService(new ReadingRequestValidator(Options.Create(new IngestionOptions()), TimeProvider.System), _publisher, TimeProvider.System),
            Options.Create(new MqttBridgeOptions { MaxPayloadBytes = 10 }), NullLogger<MqttBridgeService>.Instance);

        Assert.Equal(MessageDisposition.Ack, await service.ProcessAsync(MqttTopics.ReadingsTopic(DeviceId), Body(SensorId), default));
        Assert.Empty(_publisher.Published);
    }

    [Fact]
    public async Task A_transient_kafka_failure_is_retried_and_then_acknowledged()
    {
        _publisher.FailTimes = 2;

        var result = await Service(retries: 3).ProcessAsync(MqttTopics.ReadingsTopic(DeviceId), Body(SensorId), default);

        Assert.Equal(MessageDisposition.Ack, result);
        Assert.Single(_publisher.Published);
    }

    [Fact]
    public async Task A_persistent_kafka_failure_is_not_acknowledged_so_the_broker_redelivers()
    {
        _publisher.FailTimes = int.MaxValue;

        var result = await Service(retries: 1).ProcessAsync(MqttTopics.ReadingsTopic(DeviceId), Body(SensorId), default);

        Assert.Equal(MessageDisposition.Redeliver, result);   // sem PUBACK: o dado continua no broker, não se perde
        Assert.Empty(_publisher.Published);
    }

    private sealed class FakePublisher : IReadingPublisher
    {
        public List<ReadingMessage> Published { get; } = [];
        public int FailTimes;

        public Task PublishAsync(IReadOnlyList<ReadingMessage> messages, CancellationToken cancellationToken)
        {
            if (FailTimes > 0) { FailTimes--; throw new IngestionUnavailableException("kafka fora do ar"); }
            Published.AddRange(messages);
            return Task.CompletedTask;
        }
    }

    private sealed class FakeRegistry : ISensorRegistry
    {
        public Task<SensorInfo?> GetAsync(Guid sensorId, CancellationToken cancellationToken) =>
            Task.FromResult<SensorInfo?>(sensorId == SensorId ? new SensorInfo(SensorId, DeviceId, MetricType.Temperature, "°C", true)
                : sensorId == OtherSensor ? new SensorInfo(OtherSensor, Guid.NewGuid(), MetricType.Temperature, "°C", true) : null);
    }
}
