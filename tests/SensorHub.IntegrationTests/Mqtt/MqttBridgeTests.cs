using MQTTnet;
using MQTTnet.Protocol;
using SensorHub.IntegrationTests.Infrastructure;

namespace SensorHub.IntegrationTests.Mqtt;

/// <summary>MQTT de ponta a ponta contra um Mosquitto real: autenticação, ACL, entrega ao Kafka e resiliência.</summary>
[Collection(PlatformCollection.Name)]
public class MqttBridgeTests(PlatformFixture platform)
{
    private static string TopicOf(MqttStack.TestDevice device) => SensorHub.MqttBridge.MqttTopics.ReadingsTopic(device.Id);

    [Fact]
    public async Task A_device_publishes_with_its_api_key_and_the_reading_reaches_kafka_keyed_by_sensor()
    {
        await using var stack = await MqttStack.StartAsync(platform);
        var device = await stack.CreateDeviceAsync();

        using var client = await stack.ConnectAsync(device.Id.ToString(), device.ApiKey);
        var result = await MqttStack.PublishAsync(client, TopicOf(device), MqttStack.Reading(device.SensorId, 23.5));

        Assert.Equal(MqttClientPublishReasonCode.Success, result.ReasonCode);
        var read = stack.ReadKafka(1, new HashSet<Guid> { device.SensorId });
        var message = Assert.Single(read);
        Assert.Equal(device.SensorId.ToString(), message.Key);   // mesma chave de partição da ingestão HTTP: a ordem por sensor se mantém
        Assert.Equal(23.5, message.Message.Value);
    }

    [Fact]
    public async Task An_array_payload_is_a_batch_and_every_reading_is_published()
    {
        await using var stack = await MqttStack.StartAsync(platform);
        var device = await stack.CreateDeviceAsync();
        var now = DateTimeOffset.UtcNow;
        var batch = "[" + string.Join(",", Enumerable.Range(1, 20).Select(i =>
            $$"""{"sensorId":"{{device.SensorId}}","timestamp":"{{now.AddSeconds(-i):O}}","value":{{20 + i}},"unit":"°C"}""")) + "]";

        using var client = await stack.ConnectAsync(device.Id.ToString(), device.ApiKey);
        await MqttStack.PublishAsync(client, TopicOf(device), batch);

        Assert.Equal(20, stack.ReadKafka(20, new HashSet<Guid> { device.SensorId }).Count);
    }

    // ------------------------------------------------------------------ autenticação

    [Fact]
    public async Task Bad_credentials_are_refused_at_connect()
    {
        await using var stack = await MqttStack.StartAsync(platform);
        var device = await stack.CreateDeviceAsync();
        var other = await stack.CreateDeviceAsync();

        async Task AssertRefused(string? user, string? password)
        {
            Exception? caught = null;
            try { using var c = await stack.ConnectAsync(user, password); }
            catch (Exception e) { caught = e; }
            Assert.True(caught is not null, $"conectou com usuario={user ?? "(nulo)"} senha={password ?? "(nula)"}{Environment.NewLine}{await stack.BrokerLogsAsync()}");
            Assert.Contains("NotAuthorized", caught!.ToString());
        }

        await AssertRefused(device.Id.ToString(), "shk_chave-errada");           // chave inexistente
        await AssertRefused(device.Id.ToString(), other.ApiKey);                 // chave válida, mas de OUTRO dispositivo
        await AssertRefused("nao-e-um-guid", device.ApiKey);                     // usuário fora do formato
        await AssertRefused(Guid.NewGuid().ToString(), device.ApiKey);           // dispositivo inexistente
        await AssertRefused(null, null);                                          // anônimo
        await AssertRefused("sensorhub-bridge", "tentando-ser-o-bridge");        // fingir ser o bridge sem a senha
    }

    // ------------------------------------------------------------------ ACL

    [Fact]
    public async Task A_device_cannot_publish_to_another_devices_topic()
    {
        await using var stack = await MqttStack.StartAsync(platform);
        var attacker = await stack.CreateDeviceAsync();
        var victim = await stack.CreateDeviceAsync();

        using var client = await stack.ConnectAsync(attacker.Id.ToString(), attacker.ApiKey);
        var result = await MqttStack.PublishAsync(client, TopicOf(victim), MqttStack.Reading(victim.SensorId, 999));

        Assert.Equal(MqttClientPublishReasonCode.NotAuthorized, result.ReasonCode);
        Assert.Empty(stack.ReadKafka(1, new HashSet<Guid> { victim.SensorId }, TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task A_device_cannot_subscribe_to_anything_so_it_never_sees_other_devices_data()
    {
        await using var stack = await MqttStack.StartAsync(platform);
        var device = await stack.CreateDeviceAsync();

        using var client = await stack.ConnectAsync(device.Id.ToString(), device.ApiKey);
        foreach (var filter in new[] { "sensorhub/devices/+/readings", "#", "$share/x/sensorhub/devices/+/readings", TopicOf(device) })
        {
            var result = await client.SubscribeAsync(new MqttClientSubscribeOptionsBuilder()
                .WithTopicFilter(f => f.WithTopic(filter)).Build());
            Assert.All(result.Items, i => Assert.Equal(MqttClientSubscribeResultCode.NotAuthorized, i.ResultCode));
        }
    }

    [Fact]
    public async Task A_device_cannot_publish_outside_the_readings_topic()
    {
        await using var stack = await MqttStack.StartAsync(platform);
        var device = await stack.CreateDeviceAsync();

        using var client = await stack.ConnectAsync(device.Id.ToString(), device.ApiKey);
        foreach (var topic in new[] { $"sensorhub/devices/{device.Id}/commands", $"sensorhub/devices/{device.Id}/readings/extra", "$SYS/x" })
        {
            var result = await MqttStack.PublishAsync(client, topic, MqttStack.Reading(device.SensorId));
            Assert.Equal(MqttClientPublishReasonCode.NotAuthorized, result.ReasonCode);
        }
    }

    // ------------------------------------------------------------------ validação (mesmas regras da ingestão HTTP)

    [Fact]
    public async Task A_device_cannot_inject_a_reading_for_a_sensor_it_does_not_own_even_on_its_own_topic()
    {
        await using var stack = await MqttStack.StartAsync(platform);
        var attacker = await stack.CreateDeviceAsync();
        var victim = await stack.CreateDeviceAsync();

        using var client = await stack.ConnectAsync(attacker.Id.ToString(), attacker.ApiKey);
        await MqttStack.PublishAsync(client, TopicOf(attacker), MqttStack.Reading(victim.SensorId, 999));  // tópico próprio, sensor alheio
        await MqttStack.PublishAsync(client, TopicOf(attacker), MqttStack.Reading(attacker.SensorId, 22)); // e uma legítima logo depois

        var read = stack.ReadKafka(1, new HashSet<Guid> { attacker.SensorId, victim.SensorId });
        Assert.Equal(attacker.SensorId, Assert.Single(read).Message.SensorId);   // só a legítima chegou
        Assert.Empty(stack.ReadKafka(1, new HashSet<Guid> { victim.SensorId }, TimeSpan.FromSeconds(3)));
    }

    [Fact]
    public async Task A_malformed_message_is_dropped_without_blocking_the_ones_behind_it()
    {
        await using var stack = await MqttStack.StartAsync(platform);
        var device = await stack.CreateDeviceAsync();

        using var client = await stack.ConnectAsync(device.Id.ToString(), device.ApiKey);
        await MqttStack.PublishAsync(client, TopicOf(device), "isto não é json");
        await MqttStack.PublishAsync(client, TopicOf(device), "[]");
        await MqttStack.PublishAsync(client, TopicOf(device), "42");
        await MqttStack.PublishAsync(client, TopicOf(device), MqttStack.Reading(device.SensorId, 24));

        // se a mensagem venenosa fosse reentregue para sempre, a válida atrás dela nunca chegaria
        Assert.Single(stack.ReadKafka(1, new HashSet<Guid> { device.SensorId }));
    }

    // ------------------------------------------------------------------ ciclo de vida do dispositivo

    [Fact]
    public async Task Deactivating_a_device_cuts_a_session_that_is_already_connected()
    {
        await using var stack = await MqttStack.StartAsync(platform);
        var device = await stack.CreateDeviceAsync();
        using var client = await stack.ConnectAsync(device.Id.ToString(), device.ApiKey);
        Assert.Equal(MqttClientPublishReasonCode.Success, (await MqttStack.PublishAsync(client, TopicOf(device), MqttStack.Reading(device.SensorId))).ReasonCode);

        var admin = await stack.Api.AdminAsync();
        await stack.Api.SendAsync(HttpMethod.Patch, $"/api/devices/{device.Id}/active", admin.AccessToken, new { active = false });
        await Task.Delay(2500); // cache de ACL do broker (1 s neste teste) + cache do dispositivo

        var result = await MqttStack.PublishAsync(client, TopicOf(device), MqttStack.Reading(device.SensorId, 30));
        Assert.Equal(MqttClientPublishReasonCode.NotAuthorized, result.ReasonCode);
    }

    [Fact]
    public async Task After_rotating_the_key_the_old_one_no_longer_connects_and_the_new_one_does()
    {
        await using var stack = await MqttStack.StartAsync(platform);
        var device = await stack.CreateDeviceAsync();
        var admin = await stack.Api.AdminAsync();

        var rotated = await SensorHub.IntegrationTests.Security.SecureApi.Json(
            await stack.Api.SendAsync(HttpMethod.Post, $"/api/devices/{device.Id}/rotate-key", admin.AccessToken));
        var newKey = rotated.GetProperty("apiKey").GetString()!;
        await Task.Delay(2500);

        await Assert.ThrowsAsync<MqttRefusedException>(async () => { using var c = await stack.ConnectAsync(device.Id.ToString(), device.ApiKey); });
        using var ok = await stack.ConnectAsync(device.Id.ToString(), newKey);
        Assert.True(ok.IsConnected);
    }

    // ------------------------------------------------------------------ resiliência

    [Fact]
    public async Task Messages_published_while_the_bridge_is_down_are_delivered_when_it_returns()
    {
        await using var stack = await MqttStack.StartAsync(platform);
        var device = await stack.CreateDeviceAsync();
        using var client = await stack.ConnectAsync(device.Id.ToString(), device.ApiKey);

        // uma mensagem com o bridge no ar (garante que a sessão persistente e a assinatura existem)
        await MqttStack.PublishAsync(client, TopicOf(device), MqttStack.Reading(device.SensorId, 20));
        Assert.Single(stack.ReadKafka(1, new HashSet<Guid> { device.SensorId }));

        await stack.StopBridgeAsync();
        var now = DateTimeOffset.UtcNow;
        for (var i = 1; i <= 25; i++)
            await MqttStack.PublishAsync(client, TopicOf(device),
                $$"""{"sensorId":"{{device.SensorId}}","timestamp":"{{now.AddSeconds(-100 - i):O}}","value":{{i}},"unit":"°C"}""");

        await stack.StartBridgeAsync();

        // o broker guardou as 25 (QoS 1 + sessão persistente) e as entregou ao bridge que voltou: 1 + 25
        var all = stack.ReadKafka(26, new HashSet<Guid> { device.SensorId }, TimeSpan.FromSeconds(45));
        Assert.Equal(26, all.Select(m => m.Message.Timestamp).Distinct().Count());
    }
}
