using SensorHub.Simulator;
using SensorHub.Simulator.Load;

SimulatorSettings settings;
try
{
    if (args.Contains("--help") || args.Contains("-h"))
    {
        Console.WriteLine(SimulatorSettings.Usage);
        return 0;
    }

    settings = SimulatorSettings.Parse(args);
}
catch (Exception ex) when (ex is ArgumentException or FormatException)
{
    Console.Error.WriteLine(ex.Message);
    Console.Error.WriteLine();
    Console.Error.WriteLine(SimulatorSettings.Usage);
    return 2;
}

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

// A frota inteira é UM gateway: uma chave de API por dispositivo e todos os sensores pertencem a ele.
var fleet = new SensorFleet(settings.Sensors, sensorsPerDevice: settings.Sensors);
var factory = new ReadingFactory(fleet, new ReadingFactoryOptions(settings.DuplicateProbability, settings.HotSensorFactor));

using var httpClient = new HttpClient(new SocketsHttpHandler
{
    MaxConnectionsPerServer = Math.Max(4, settings.Workers * 2),
    PooledConnectionLifetime = TimeSpan.FromMinutes(5)
})
{
    BaseAddress = new Uri(settings.Url.TrimEnd('/') + "/"),
    Timeout = TimeSpan.FromSeconds(30)
};

if (settings.Mode == "register")
{
    var rules = settings.Rules.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .Select(r => r.ToLowerInvariant()).ToHashSet();
    AdminCredentials? admin = settings.AdminEmail.Length > 0 ? new(settings.AdminEmail, settings.AdminPassword) : null;
    RegistrationReport registration;
    try
    {
        registration = await new FleetRegistrar(httpClient, admin).RegisterAsync(
            fleet, rules.Contains("threshold"), rules.Contains("nodata"), settings.NoDataSeconds, cancellationToken: cts.Token);
    }
    catch (HttpRequestException ex)
    {
        Console.Error.WriteLine($"Cadastro interrompido: {ex.Message}");
        return 1;
    }
    Console.WriteLine($"Cadastro: {registration.DevicesCreated} dispositivo(s) criado(s), {registration.SensorsCreated} sensores criados, " +
                      $"{registration.SensorsAlreadyExisted} já existiam, {registration.RulesCreated} regras criadas, {registration.Failures} falhas.");
    foreach (var (deviceId, key) in registration.ApiKeys ?? new Dictionary<Guid, string>())
    {
        if (settings.KeyOut.Length > 0) { await File.WriteAllTextAsync(settings.KeyOut, key, cts.Token); Console.WriteLine($"Chave de API do dispositivo {deviceId} gravada em {settings.KeyOut}."); }
        else Console.WriteLine($"Chave de API do dispositivo {deviceId} (exibida só agora): {key}");
    }
    if (registration.DevicesAlreadyExisted > 0)
        Console.WriteLine("Dispositivo já existia: a chave original não é recuperável. Use a que você guardou ou rotacione em POST /api/devices/{id}/rotate-key.");
    return registration.Failures > 0 ? 1 : 0;
}

IReadingSink sink = settings.Mode switch
{
    "dry-run" => new CountingSink(),
    "http" => new HttpReadingSink(httpClient, settings.ApiKey),
    "mqtt" => new MqttReadingSink(settings.MqttHost, settings.MqttPort, fleet.Sensors[0].DeviceId, settings.ApiKey),
    _ => throw new NotSupportedException($"Modo '{settings.Mode}' não suportado (use dry-run, http ou mqtt).")
};

Console.WriteLine($"Simulador: {settings.Sensors} sensores, {settings.Rate} leituras/s, {settings.DurationSeconds}s, modo {settings.Mode}");

var report = await new LoadRunner().RunAsync(
    factory, sink,
    new LoadOptions(settings.Rate, TimeSpan.FromSeconds(settings.DurationSeconds), settings.BatchSize, settings.Workers,
        settings.SilenceAfterSeconds > 0 ? TimeSpan.FromSeconds(settings.SilenceAfterSeconds) : null, settings.SilenceFraction),
    cts.Token);

Console.WriteLine($"Enviadas: {report.Sent:N0}  Falhas: {report.Failed:N0}  Tempo: {report.Elapsed.TotalSeconds:F1}s  Taxa: {report.AchievedRate:N0}/s");
if (report.FirstError is not null)
    Console.Error.WriteLine($"Primeira falha: {report.FirstError}");
if (sink is HttpReadingSink http)
    Console.WriteLine($"Reenvios por backpressure/rede: {http.Retries:N0}");
if (sink is MqttReadingSink mqtt)
{
    Console.WriteLine($"Reenvios por reconexão: {mqtt.Retries:N0}");
    await mqtt.DisposeAsync();
}
return report.Failed > 0 ? 1 : 0;
