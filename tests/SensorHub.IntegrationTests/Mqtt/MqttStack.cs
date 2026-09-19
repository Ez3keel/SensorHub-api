using System.Net;
using System.Net.Sockets;
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using MQTTnet;
using MQTTnet.Formatter;
using MQTTnet.Protocol;
using SensorHub.Infrastructure.Kafka;
using SensorHub.IntegrationTests.Infrastructure;
using SensorHub.IntegrationTests.Security;
using SensorHub.MqttBridge;

namespace SensorHub.IntegrationTests.Mqtt;

/// <summary>O broker respondeu ao CONNECT com um código de recusa (no MQTTnet 5 isso não vira exceção sozinho).</summary>
internal sealed class MqttRefusedException(MqttClientConnectResultCode code) : Exception($"Conexão recusada pelo broker: {code}.")
{
    public MqttClientConnectResultCode Code { get; } = code;
}

/// <summary>
/// O caminho MQTT de verdade: um Mosquitto (com o plugin go-auth) num container. Sua autenticação aponta para um host "auth" e o consumidor MQTT -> Kafka
/// roda como um segundo host "bridge" (papéis separados, como em produção), ambos no processo do teste, com Kestrel real e um tópico Kafka isolado.
/// O arquivo de configuração do broker é o MESMO do Compose (só troca host/porta e o cache).
/// </summary>
internal sealed class MqttStack : IAsyncDisposable
{
    public const string BridgePassword = "senha-do-bridge-para-testes-123456";

    private readonly IContainer _broker;
    private readonly PlatformFixture _platform;
    private readonly int _authPort;
    private readonly Dictionary<string, string> _bridgeArgs;
    private WebApplication? _bridge;
    private WebApplication? _authHost;

    public KafkaOptions Kafka { get; }
    public int BrokerPort { get; }
    public SecureApi Api { get; }

    private MqttStack(IContainer broker, PlatformFixture platform, KafkaOptions kafka, SecureApi api, int authPort, Dictionary<string, string> bridgeArgs)
    {
        _broker = broker;
        _platform = platform;
        Kafka = kafka;
        Api = api;
        _authPort = authPort;
        _bridgeArgs = bridgeArgs;
        BrokerPort = broker.GetMappedPublicPort(1883);
    }

    public static async Task<MqttStack> StartAsync(PlatformFixture platform, Dictionary<string, string>? bridgeSettings = null)
    {
        var kafka = await platform.CreateIsolatedTopicsAsync();
        var api = new SecureApi(platform);
        var authPort = FreePort();

        // O container alcança o bridge (no host) por host.docker.internal; o cache cai para 1 s para os testes de rotação/desativação.
        var conf = (await File.ReadAllTextAsync(FindRepoFile("docker/mosquitto/mosquitto.conf")))
            .Replace("auth_opt_http_host mqtt-auth", "auth_opt_http_host host.docker.internal")
            .Replace("auth_opt_http_port 8080", $"auth_opt_http_port {authPort}")
            .Replace("auth_opt_auth_cache_seconds 30", "auth_opt_auth_cache_seconds 1")
            .Replace("auth_opt_acl_cache_seconds 30", "auth_opt_acl_cache_seconds 1")
            .Replace("auth_opt_log_level warn", "auth_opt_log_level debug");

        var broker = new ContainerBuilder("iegomez/mosquitto-go-auth:latest")
            .WithPortBinding(1883, assignRandomHostPort: true)
            .WithExtraHost("host.docker.internal", "host-gateway")
            .WithResourceMapping(Encoding.UTF8.GetBytes(conf), "/etc/mosquitto/mosquitto.conf")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("running"))
            .Build();
        await broker.StartAsync();

        var args = new Dictionary<string, string>
        {
            ["environment"] = "Testing",
            ["Mqtt:Host"] = "localhost",
            ["Mqtt:Port"] = broker.GetMappedPublicPort(1883).ToString(),
            ["Mqtt:BridgePassword"] = BridgePassword,
            ["Kafka:BootstrapServers"] = kafka.BootstrapServers,
            ["Kafka:ReadingsTopic"] = kafka.ReadingsTopic,
            ["Kafka:ReadingsDlqTopic"] = kafka.ReadingsDlqTopic,
            ["Kafka:AlertsTopic"] = kafka.AlertsTopic,
            ["ConnectionStrings:Postgres"] = platform.ConnectionString,
            ["Persistence:MigrateOnStartup"] = "false",
            ["Security:DeviceCacheSeconds"] = "1",
            ["Security:SensorCacheSeconds"] = "1"
        };
        if (bridgeSettings is not null) foreach (var (k, v) in bridgeSettings) args[k] = v;

        var stack = new MqttStack(broker, platform, kafka, api, authPort, args);
        await stack.StartAuthAsync();   // primeiro: o próprio bridge se autentica no broker por ele
        await stack.StartBridgeAsync();
        return stack;
    }

    private async Task StartAuthAsync()
    {
        var args = new Dictionary<string, string>(_bridgeArgs) { ["urls"] = $"http://0.0.0.0:{_authPort}", ["Mqtt:Roles:0"] = "auth" };
        _authHost = MqttBridgeHost.Build(args.Select(kv => $"--{kv.Key}={kv.Value}").ToArray());
        await _authHost.StartAsync();
    }

    public async Task StartBridgeAsync()
    {
        // porta 0 = efêmera: o papel "bridge" só precisa do Kestrel para /metrics e /health
        var args = new Dictionary<string, string>(_bridgeArgs) { ["urls"] = "http://127.0.0.1:0", ["Mqtt:Roles:0"] = "bridge" };
        _bridge = MqttBridgeHost.Build(args.Select(kv => $"--{kv.Key}={kv.Value}").ToArray());
        await _bridge.StartAsync();
        await TestHelpers.EventuallyAsync(() => Task.FromResult(MqttMetrics.IsConnected), "o bridge conectou ao broker", TimeSpan.FromSeconds(30));
    }

    /// <summary>Derruba o bridge (o broker continua no ar): simula uma queda ou um deploy.</summary>
    public async Task StopBridgeAsync()
    {
        if (_bridge is null) return;
        await _bridge.StopAsync();
        await _bridge.DisposeAsync();
        _bridge = null;
    }

    public async Task<string> BrokerLogsAsync()
    {
        var (stdout, stderr) = await _broker.GetLogsAsync();
        return string.Join(Environment.NewLine, (stdout + stderr).Split('\n').Where(l => !l.Contains("to auth record")).TakeLast(25));
    }

    public MqttBridgeService Service => _bridge!.Services.GetRequiredService<MqttBridgeService>();

    // ------------------------------------------------------------------ cliente MQTT de teste

    public async Task<IMqttClient> ConnectAsync(string? username, string? password, bool cleanSession = true)
    {
        var client = new MqttClientFactory().CreateMqttClient();
        var builder = new MqttClientOptionsBuilder()
            .WithTcpServer("localhost", BrokerPort)
            .WithClientId($"teste-{Guid.NewGuid():N}")
            .WithProtocolVersion(MqttProtocolVersion.V500) // v5: o broker devolve o MOTIVO da recusa (PUBACK/SUBACK com códigos)
            .WithCleanSession(cleanSession);
        if (username is not null) builder.WithCredentials(username, password);
        var result = await client.ConnectAsync(builder.Build());
        if (result.ResultCode != MqttClientConnectResultCode.Success)
        {
            client.Dispose();
            throw new MqttRefusedException(result.ResultCode);
        }
        return client;
    }

    public static async Task<MqttClientPublishResult> PublishAsync(IMqttClient client, string topic, string json) =>
        await client.PublishAsync(new MqttApplicationMessageBuilder()
            .WithTopic(topic)
            .WithPayload(Encoding.UTF8.GetBytes(json))
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .Build());

    // ------------------------------------------------------------------ dispositivos

    public sealed record TestDevice(Guid Id, string ApiKey, Guid SensorId);

    /// <summary>Cria (como administrador, pela API) um dispositivo com um sensor de temperatura e devolve a chave em texto puro.</summary>
    public async Task<TestDevice> CreateDeviceAsync()
    {
        var admin = await Api.AdminAsync();
        var created = await SecureApi.Json(await Api.SendAsync(HttpMethod.Post, "/api/devices", admin.AccessToken, new { name = $"mqtt-{Guid.NewGuid():N}"[..12] }));
        var deviceId = created.GetProperty("device").GetProperty("id").GetGuid();
        var sensor = await SecureApi.Json(await Api.SendAsync(HttpMethod.Post, "/api/sensors", admin.AccessToken,
            new { deviceId, name = $"t-{Guid.NewGuid():N}"[..12], metric = "Temperature", unit = "°C", group = "mqtt" }));
        return new TestDevice(deviceId, created.GetProperty("apiKey").GetString()!, sensor.GetProperty("id").GetGuid());
    }

    public static string Reading(Guid sensorId, double value = 21.5) =>
        $$"""{"sensorId":"{{sensorId}}","timestamp":"{{DateTimeOffset.UtcNow.AddSeconds(-1):O}}","value":{{value.ToString(System.Globalization.CultureInfo.InvariantCulture)}},"unit":"°C"}""";

    public List<TopicReading> ReadKafka(int expected, ISet<Guid> sensors, TimeSpan? timeout = null) =>
        TopicReader.Read(Kafka.BootstrapServers, Kafka.ReadingsTopic, expected, sensors, timeout ?? TimeSpan.FromSeconds(30));

    // ------------------------------------------------------------------ infra

    private static int FreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static string FindRepoFile(string relative)
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, relative);
            if (File.Exists(candidate)) return candidate;
        }
        throw new FileNotFoundException($"Não achei {relative} subindo a partir de {AppContext.BaseDirectory}.");
    }

    public async ValueTask DisposeAsync()
    {
        await StopBridgeAsync();
        if (_authHost is not null) { await _authHost.StopAsync(); await _authHost.DisposeAsync(); }
        await _broker.DisposeAsync();
        await Api.DisposeAsync();
    }
}
