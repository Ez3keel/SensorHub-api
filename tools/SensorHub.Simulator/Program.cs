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

var fleet = new SensorFleet(settings.Sensors);
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
    var registration = await new FleetRegistrar(httpClient).RegisterAsync(
        fleet, rules.Contains("threshold"), rules.Contains("nodata"), settings.NoDataSeconds, cancellationToken: cts.Token);
    Console.WriteLine($"Cadastro: {registration.SensorsCreated} sensores criados, {registration.SensorsAlreadyExisted} já existiam, " +
                      $"{registration.RulesCreated} regras criadas, {registration.Failures} falhas.");
    return registration.Failures > 0 ? 1 : 0;
}

IReadingSink sink = settings.Mode switch
{
    "dry-run" => new CountingSink(),
    "http" => new HttpReadingSink(httpClient, settings.ApiKey),
    _ => throw new NotSupportedException($"Modo '{settings.Mode}' não suportado (use dry-run ou http).")
};

Console.WriteLine($"Simulador: {settings.Sensors} sensores, {settings.Rate} leituras/s, {settings.DurationSeconds}s, modo {settings.Mode}");

var report = await new LoadRunner().RunAsync(
    factory, sink,
    new LoadOptions(settings.Rate, TimeSpan.FromSeconds(settings.DurationSeconds), settings.BatchSize, settings.Workers,
        settings.SilenceAfterSeconds > 0 ? TimeSpan.FromSeconds(settings.SilenceAfterSeconds) : null, settings.SilenceFraction),
    cts.Token);

Console.WriteLine($"Enviadas: {report.Sent:N0}  Falhas: {report.Failed:N0}  Tempo: {report.Elapsed.TotalSeconds:F1}s  Taxa: {report.AchievedRate:N0}/s");
if (sink is HttpReadingSink http)
    Console.WriteLine($"Reenvios por backpressure/rede: {http.Retries:N0}");
return report.Failed > 0 ? 1 : 0;
