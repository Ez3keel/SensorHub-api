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

IReadingSink sink = settings.Mode switch
{
    "dry-run" => new CountingSink(),
    _ => throw new NotSupportedException($"Modo '{settings.Mode}' ainda não suportado.")
};

Console.WriteLine($"Simulador: {settings.Sensors} sensores, {settings.Rate} leituras/s, {settings.DurationSeconds}s, modo {settings.Mode}");

var report = await new LoadRunner().RunAsync(
    factory, sink,
    new LoadOptions(settings.Rate, TimeSpan.FromSeconds(settings.DurationSeconds), settings.BatchSize, settings.Workers),
    cts.Token);

Console.WriteLine($"Enviadas: {report.Sent:N0}  Falhas: {report.Failed:N0}  Tempo: {report.Elapsed.TotalSeconds:F1}s  Taxa: {report.AchievedRate:N0}/s");
return report.Failed > 0 ? 1 : 0;
