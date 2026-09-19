using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Options;
using SensorHub.Application.Realtime;

namespace SensorHub.Api.Realtime;

public sealed class RealtimeOptions
{
    public const string SectionName = "Realtime";

    /// <summary>Liga o fanout de tempo real (consumers + flush). Desligável para processos que só ingerem.</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>Período do flush: o teto de atualizações por sensor é 1/período (250 ms = 4 por segundo).</summary>
    public int FlushMs { get; set; } = 250;

    public int MaxSensorSubscriptionsPerConnection { get; set; } = 50;
    public int MaxGroupSubscriptionsPerConnection { get; set; } = 10;

    /// <summary>Consumer group do fanout. TODAS as instâncias da API usam o mesmo: cada partição é lida por uma só.</summary>
    public string ReadingsGroupId { get; set; } = "sensorhub.realtime";
    public string AlertsGroupId { get; set; } = "sensorhub.realtime.alerts";

    public int DirectoryCacheSeconds { get; set; } = 15;
}

/// <summary>
/// Hub de telemetria. O cliente escolhe o que quer receber assinando grupos:
/// <list type="bullet">
/// <item><c>group:{nome}</c>: leituras coalescidas de todos os sensores de um grupo (visão geral do painel).</item>
/// <item><c>sensor:{id}</c>: leituras de UM sensor (visão de detalhe); só é alimentado enquanto há assinantes.</item>
/// <item><c>alerts</c>: disparos e resoluções de alertas.</item>
/// </list>
/// Mensagens do servidor: <c>readings</c> (lista), <c>reading</c> (uma) e <c>alert</c>.
/// </summary>
public sealed class TelemetryHub(ISubscriptionRegistry registry, IOptions<RealtimeOptions> options) : Hub
{
    private const string SensorsKey = "sensors";
    private const string GroupsKey = "groups";
    private const int MaxGroupNameLength = 100;

    public static string SensorGroup(Guid sensorId) => $"sensor:{sensorId:D}";
    public static string ReadingGroup(string name) => $"group:{name}";
    public const string AlertsGroup = "alerts";

    public async Task SubscribeGroup(string group)
    {
        if (string.IsNullOrWhiteSpace(group) || group.Length > MaxGroupNameLength)
            throw new HubException("Nome de grupo inválido.");

        var subscribed = Track<string>(GroupsKey);
        if (!subscribed.Contains(group) && subscribed.Count >= options.Value.MaxGroupSubscriptionsPerConnection)
            throw new HubException($"Limite de {options.Value.MaxGroupSubscriptionsPerConnection} grupos por conexão.");

        if (subscribed.Add(group))
            await Groups.AddToGroupAsync(Context.ConnectionId, ReadingGroup(group));
    }

    public async Task UnsubscribeGroup(string group)
    {
        if (Track<string>(GroupsKey).Remove(group))
            await Groups.RemoveFromGroupAsync(Context.ConnectionId, ReadingGroup(group));
    }

    public async Task SubscribeSensor(Guid sensorId)
    {
        var subscribed = Track<Guid>(SensorsKey);
        if (!subscribed.Contains(sensorId) && subscribed.Count >= options.Value.MaxSensorSubscriptionsPerConnection)
            throw new HubException($"Limite de {options.Value.MaxSensorSubscriptionsPerConnection} sensores por conexão.");

        if (!subscribed.Add(sensorId)) return;

        await Groups.AddToGroupAsync(Context.ConnectionId, SensorGroup(sensorId));
        await registry.AddAsync(sensorId, Context.ConnectionAborted); // avisa o cluster inteiro que alguém assiste este sensor
    }

    public async Task UnsubscribeSensor(Guid sensorId)
    {
        if (!Track<Guid>(SensorsKey).Remove(sensorId)) return;

        await Groups.RemoveFromGroupAsync(Context.ConnectionId, SensorGroup(sensorId));
        await registry.RemoveAsync(sensorId, CancellationToken.None);
    }

    public Task SubscribeAlerts() => Groups.AddToGroupAsync(Context.ConnectionId, AlertsGroup);

    public Task UnsubscribeAlerts() => Groups.RemoveFromGroupAsync(Context.ConnectionId, AlertsGroup);

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        // O SignalR já remove a conexão dos grupos; o que ele NÃO sabe é a contagem de assinantes no registro do cluster.
        foreach (var sensor in Track<Guid>(SensorsKey).ToList())
            await registry.RemoveAsync(sensor, CancellationToken.None);

        await base.OnDisconnectedAsync(exception);
    }

    private HashSet<T> Track<T>(string key)
    {
        if (Context.Items.TryGetValue(key, out var existing)) return (HashSet<T>)existing!;

        var created = new HashSet<T>();
        Context.Items[key] = created;
        return created;
    }
}
