using System.Collections.Concurrent;
using System.Net.Http.Json;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Options;
using SensorHub.Application.Alerting;
using SensorHub.Domain.Alerts;
using SensorHub.Infrastructure.Kafka;
using SensorHub.Infrastructure.Realtime;
using SensorHub.Infrastructure.Redis;
using SensorHub.IntegrationTests.Infrastructure;
using static SensorHub.IntegrationTests.Infrastructure.TestHelpers;

namespace SensorHub.IntegrationTests.Realtime;

/// <summary>Uma API completa (Program.cs real) com o tempo real ligado, tópicos e consumer groups isolados.</summary>
internal sealed class RealtimeApi : IAsyncDisposable
{
    public required ApiFactory Factory { get; init; }
    public required KafkaOptions Kafka { get; init; }
    public required string ReadingsGroupId { get; init; }
    public required Guid DeviceId { get; init; }
    private readonly List<HubConnection> _connections = [];

    public static async Task<RealtimeApi> StartAsync(
        PlatformFixture platform, KafkaOptions? kafka = null, string? readingsGroup = null, Dictionary<string, string?>? extra = null)
    {
        kafka ??= await platform.CreateIsolatedTopicsAsync();
        readingsGroup ??= $"rt-{Guid.NewGuid():N}";
        var overrides = new Dictionary<string, string?>
        {
            ["Realtime:Enabled"] = "true",
            ["Realtime:FlushMs"] = "100",
            ["Realtime:DirectoryCacheSeconds"] = "1",
            ["Realtime:ReadingsGroupId"] = readingsGroup,
            ["Realtime:AlertsGroupId"] = $"rta-{Guid.NewGuid():N}",
            ["Kafka:ProvisionTopics"] = "false",
            ["Kafka:ReadingsTopic"] = kafka.ReadingsTopic,
            ["Kafka:ReadingsDlqTopic"] = kafka.ReadingsDlqTopic,
            ["Kafka:AlertsTopic"] = kafka.AlertsTopic
        };
        if (extra is not null) foreach (var (k, v) in extra) overrides[k] = v;

        var factory = new ApiFactory(platform.BootstrapServers, overrides, platform.ConnectionString, platform.RedisConnectionString);
        _ = factory.Server; // sobe o host (e os consumers)
        return new RealtimeApi { Factory = factory, Kafka = kafka, ReadingsGroupId = readingsGroup, DeviceId = platform.SharedDeviceId };
    }

    public async Task<HubConnection> ConnectAsync()
    {
        var connection = new HubConnectionBuilder()
            .WithUrl(new Uri(Factory.Server.BaseAddress, "hubs/telemetry"), options =>
            {
                options.HttpMessageHandlerFactory = _ => Factory.Server.CreateHandler();
                options.Transports = HttpTransportType.LongPolling; // o TestServer atende long polling sem configuração extra
            })
            .Build();
        await connection.StartAsync();
        _connections.Add(connection);
        return connection;
    }

    public async Task<Guid> CreateSensorAsync(string group)
    {
        using var http = Factory.CreateClient();
        var response = await http.PostAsJsonAsync("/api/sensors",
            new { deviceId = DeviceId, name = $"rt-{Guid.NewGuid():N}"[..16], metric = "Temperature", unit = "°C", group });
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        return doc.RootElement.GetProperty("id").GetGuid();
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var c in _connections) await c.DisposeAsync();
        await Factory.DisposeAsync();
    }
}

[Collection(PlatformCollection.Name)]
public class RealtimeTests(PlatformFixture platform)
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    /// <summary>Publica leituras repetidamente até condição: o consumer do tempo real começa do fim do log (Latest) e pode ainda estar entrando no grupo.</summary>
    private async Task PublishUntilAsync(KafkaOptions kafka, Func<List<Application.Contracts.ReadingMessage>> next, Func<bool> done, TimeSpan? timeout = null)
    {
        var clock = System.Diagnostics.Stopwatch.StartNew();
        while (!done() && clock.Elapsed < (timeout ?? Wait))
        {
            await PublishAsync(kafka, next());
            await Task.Delay(250);
        }
    }

    private static Application.Contracts.ReadingMessage M(Guid sensor, double value) => Message(sensor, DateTimeOffset.UtcNow, value);

    [Fact]
    public async Task A_group_subscriber_receives_the_latest_reading_of_each_sensor_in_the_group()
    {
        await using var api = await RealtimeApi.StartAsync(platform);
        var group = $"planta-{Guid.NewGuid():N}"[..14];
        var sensors = new[] { await api.CreateSensorAsync(group), await api.CreateSensorAsync(group) };
        var other = await api.CreateSensorAsync("outra-planta");
        var received = new ConcurrentDictionary<Guid, double>();
        var connection = await api.ConnectAsync();
        connection.On<List<ReadingPushDto>>("readings", list => { foreach (var r in list) received[r.SensorId] = r.Value; });
        await connection.InvokeAsync("SubscribeGroup", group);

        var n = 0;
        await PublishUntilAsync(api.Kafka, () => [.. sensors.Select(s => M(s, ++n)), M(other, 999)],
            () => sensors.All(s => received.ContainsKey(s)));

        Assert.All(sensors, s => Assert.True(received.ContainsKey(s)));
        Assert.DoesNotContain(other, received.Keys); // sensor de OUTRO grupo não vaza para quem assinou este
    }

    [Fact]
    public async Task Coalescing_caps_the_push_rate_and_the_final_value_is_always_delivered()
    {
        await using var api = await RealtimeApi.StartAsync(platform);
        var group = $"planta-{Guid.NewGuid():N}"[..14];
        var sensor = await api.CreateSensorAsync(group);
        var messages = new ConcurrentQueue<ReadingPushDto>();
        var connection = await api.ConnectAsync();
        connection.On<List<ReadingPushDto>>("readings", list => { foreach (var r in list) messages.Enqueue(r); });
        await connection.InvokeAsync("SubscribeGroup", group);

        // espera o consumer estar no grupo (aquecimento) antes da rajada
        await PublishUntilAsync(api.Kafka, () => [M(sensor, -1)], () => !messages.IsEmpty);
        messages.Clear();

        // 3000 leituras do MESMO sensor o mais rápido possível: o dashboard não deve receber 3000 mensagens
        var burstStart = DateTimeOffset.UtcNow;
        await PublishAsync(api.Kafka, Enumerable.Range(0, 3000).Select(i => Message(sensor, burstStart.AddMilliseconds(i), i)).ToList());
        await EventuallyAsync(() => Task.FromResult(messages.Any(m => m.Value == 2999)), "o último valor da rajada chegou");
        await Task.Delay(500);

        Assert.True(messages.Count < 100, $"{messages.Count} mensagens para 3000 leituras: o coalescing deveria limitar a taxa");
        Assert.Equal(2999, messages.Max(m => m.Value));
    }

    [Fact]
    public async Task A_sensor_subscription_delivers_only_that_sensor_and_stops_after_unsubscribe()
    {
        await using var api = await RealtimeApi.StartAsync(platform);
        var group = $"planta-{Guid.NewGuid():N}"[..14];
        var watched = await api.CreateSensorAsync(group);
        var ignored = await api.CreateSensorAsync(group);
        var seen = new ConcurrentBag<Guid>();
        var connection = await api.ConnectAsync();
        connection.On<ReadingPushDto>("reading", r => seen.Add(r.SensorId));
        await connection.InvokeAsync("SubscribeSensor", watched);

        var n = 0;
        await PublishUntilAsync(api.Kafka, () => [M(watched, ++n), M(ignored, ++n)], () => !seen.IsEmpty);

        Assert.NotEmpty(seen);
        Assert.All(seen, id => Assert.Equal(watched, id)); // só o sensor assistido

        await connection.InvokeAsync("UnsubscribeSensor", watched);
        await Task.Delay(1500); // deixa o registro e o cache de assinaturas (1 s) se atualizarem
        seen.Clear();
        await PublishAsync(api.Kafka, [M(watched, 1), M(watched, 2)]);
        await Task.Delay(1500);

        Assert.Empty(seen);
    }

    [Fact]
    public async Task Alerts_are_pushed_to_subscribers_and_republished_duplicates_are_dropped()
    {
        await using var api = await RealtimeApi.StartAsync(platform);
        var pushes = new ConcurrentQueue<AlertPushDto>();
        var connection = await api.ConnectAsync();
        connection.On<AlertPushDto>("alert", a => pushes.Enqueue(a));
        await connection.InvokeAsync("SubscribeAlerts");

        var alertId = Guid.NewGuid();
        var fired = new AlertEvent(1, alertId, Guid.NewGuid(), Guid.NewGuid(), AlertEventKind.Fired, Severity.Critical, DateTimeOffset.UtcNow, 95, "Temp alta");
        using var producer = new ProducerBuilder<string, byte[]>(new ProducerConfig { BootstrapServers = platform.BootstrapServers }).Build();
        var clock = System.Diagnostics.Stopwatch.StartNew();
        // o listener começa do fim do log: repete até chegar o primeiro push (ele pode ainda estar entrando no grupo)
        while (pushes.IsEmpty && clock.Elapsed < Wait)
        {
            await producer.ProduceAsync(api.Kafka.AlertsTopic, new Message<string, byte[]> { Key = fired.SensorId.ToString(), Value = AlertEventSerializer.Serialize(fired) });
            await Task.Delay(400);
        }

        // republica o MESMO evento várias vezes (o motor faz isso após uma queda) e depois o Resolved
        for (var i = 0; i < 3; i++)
            await producer.ProduceAsync(api.Kafka.AlertsTopic, new Message<string, byte[]> { Key = fired.SensorId.ToString(), Value = AlertEventSerializer.Serialize(fired) });
        var resolved = fired with { Kind = AlertEventKind.Resolved, Value = 70 };
        await producer.ProduceAsync(api.Kafka.AlertsTopic, new Message<string, byte[]> { Key = fired.SensorId.ToString(), Value = AlertEventSerializer.Serialize(resolved) });
        await EventuallyAsync(() => Task.FromResult(pushes.Any(p => p.Kind == "Resolved")), "resolved chegou");
        await Task.Delay(500);

        Assert.Equal(1, pushes.Count(p => p.Kind == "Fired"));    // 4 publicações do Fired => 1 push
        Assert.Equal(1, pushes.Count(p => p.Kind == "Resolved"));
        Assert.All(pushes, p => Assert.Equal(alertId, p.AlertId));
        Assert.Equal("Critical", pushes.First().Severity);
    }

    [Fact]
    public async Task Two_api_instances_share_the_stream_and_the_redis_backplane_delivers_to_a_client_on_either()
    {
        // Duas instâncias da API no MESMO consumer group: o Kafka reparte as partições entre elas, então cada leitura é
        // tratada por UMA delas. O cliente está conectado só na instância A, mas precisa ver as leituras das duas.
        var kafka = await platform.CreateIsolatedTopicsAsync(partitions: 6);
        var groupId = $"rt-{Guid.NewGuid():N}";
        await using var apiA = await RealtimeApi.StartAsync(platform, kafka, groupId);
        await using var apiB = await RealtimeApi.StartAsync(platform, kafka, groupId);

        var group = $"planta-{Guid.NewGuid():N}"[..14];
        var sensors = new List<Guid>();
        for (var i = 0; i < 40; i++) sensors.Add(await apiA.CreateSensorAsync(group));
        var received = new ConcurrentDictionary<Guid, double>();
        var connection = await apiA.ConnectAsync(); // conectado APENAS na instância A
        connection.On<List<ReadingPushDto>>("readings", list => { foreach (var r in list) received[r.SensorId] = r.Value; });
        await connection.InvokeAsync("SubscribeGroup", group);

        var n = 0;
        await PublishUntilAsync(kafka, () => sensors.Select(s => M(s, ++n)).ToList(),
            () => sensors.All(received.ContainsKey), TimeSpan.FromSeconds(60));

        Assert.Equal(40, received.Count); // todas as leituras, inclusive as consumidas pela instância B

        // as duas instâncias estão de fato no grupo, dividindo as partições
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = platform.BootstrapServers }).Build();
        var description = (await admin.DescribeConsumerGroupsAsync([groupId])).ConsumerGroupDescriptions.Single();
        Assert.Equal(2, description.Members.Count);
    }

    [Fact]
    public async Task Invalid_subscriptions_and_per_connection_limits_are_rejected()
    {
        await using var api = await RealtimeApi.StartAsync(platform, extra: new()
        {
            ["Realtime:MaxSensorSubscriptionsPerConnection"] = "2",
            ["Realtime:MaxGroupSubscriptionsPerConnection"] = "2"
        });
        var connection = await api.ConnectAsync();

        await Assert.ThrowsAsync<HubException>(() => connection.InvokeAsync("SubscribeGroup", "  "));
        await Assert.ThrowsAsync<HubException>(() => connection.InvokeAsync("SubscribeGroup", new string('x', 101)));

        await connection.InvokeAsync("SubscribeGroup", "g1");
        await connection.InvokeAsync("SubscribeGroup", "g2");
        await connection.InvokeAsync("SubscribeGroup", "g1"); // repetir a mesma assinatura não conta
        await Assert.ThrowsAsync<HubException>(() => connection.InvokeAsync("SubscribeGroup", "g3"));

        await connection.InvokeAsync("SubscribeSensor", Guid.NewGuid());
        await connection.InvokeAsync("SubscribeSensor", Guid.NewGuid());
        await Assert.ThrowsAsync<HubException>(() => connection.InvokeAsync("SubscribeSensor", Guid.NewGuid()));
    }

    [Fact]
    public async Task Disconnecting_releases_the_clusterwide_subscription_count()
    {
        await using var api = await RealtimeApi.StartAsync(platform);
        var registry = new RedisSubscriptionRegistry(platform.Redis, Options.Create(new RedisOptions { KeyPrefix = "test:" }));
        var sensor = Guid.NewGuid();
        var connection = await api.ConnectAsync();
        await connection.InvokeAsync("SubscribeSensor", sensor);
        Assert.Contains(sensor, await registry.GetSubscribedSensorsAsync(default));

        await connection.StopAsync();

        await EventuallyAsync(async () => !(await registry.GetSubscribedSensorsAsync(default)).Contains(sensor),
            "assinatura liberada ao desconectar");
    }

    private sealed record ReadingPushDto(Guid SensorId, DateTimeOffset Timestamp, double Value);
    private sealed record AlertPushDto(Guid AlertId, Guid RuleId, Guid SensorId, string Kind, string Severity, DateTimeOffset At, double? Value, string Message);
}

[Collection(PlatformCollection.Name)]
public class RealtimeStoresTests(PlatformFixture platform)
{
    private RedisSubscriptionRegistry Registry() => new(platform.Redis, Options.Create(new RedisOptions { KeyPrefix = "test:" }));

    [Fact]
    public async Task Registry_counts_subscribers_and_forgets_a_sensor_when_the_last_one_leaves()
    {
        var registry = Registry();
        var sensor = Guid.NewGuid();

        await registry.AddAsync(sensor, default);
        await registry.AddAsync(sensor, default); // dois dashboards assistem o mesmo sensor
        await registry.RemoveAsync(sensor, default);
        Assert.Contains(sensor, await registry.GetSubscribedSensorsAsync(default)); // ainda resta um

        await registry.RemoveAsync(sensor, default);
        Assert.DoesNotContain(sensor, await registry.GetSubscribedSensorsAsync(default));
    }

    [Fact]
    public async Task Registry_never_goes_negative_when_removing_more_than_was_added()
    {
        var registry = Registry();
        var sensor = Guid.NewGuid();

        await registry.RemoveAsync(sensor, default);
        await registry.AddAsync(sensor, default);

        Assert.Contains(sensor, await registry.GetSubscribedSensorsAsync(default)); // a contagem recomeçou de zero, não de -1
    }

    [Fact]
    public async Task Sensor_directory_maps_sensors_to_groups_caches_and_refreshes()
    {
        var clock = new Microsoft.Extensions.Time.Testing.FakeTimeProvider(DateTimeOffset.UtcNow);
        var directory = new CachedSensorDirectory(platform.DataSource, clock, cacheSeconds: 10);
        var first = await Alerting.AlertTestData.SaveSensorAsync(platform, group: "grupo-a");
        Assert.Equal("grupo-a", await directory.GetGroupAsync(first.Id, default));
        Assert.Null(await directory.GetGroupAsync(Guid.NewGuid(), default)); // desconhecido

        var late = await Alerting.AlertTestData.SaveSensorAsync(platform, group: "grupo-b");
        Assert.Null(await directory.GetGroupAsync(late.Id, default));   // ainda dentro do TTL: cache antigo
        clock.Advance(TimeSpan.FromSeconds(11));
        Assert.Equal("grupo-b", await directory.GetGroupAsync(late.Id, default)); // TTL vencido: enxerga o novo
    }
}
