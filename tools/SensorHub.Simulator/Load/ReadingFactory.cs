using SensorHub.Simulator.Signal;

namespace SensorHub.Simulator.Load;

public readonly record struct SimulatedReading(Guid SensorId, DateTimeOffset Timestamp, double Value, string Unit);

public sealed record ReadingFactoryOptions(
    /// <summary>Probabilidade de reemitir uma leitura idêntica (mesmo sensor/instante/valor): exercita a idempotência.</summary>
    double DuplicateProbability = 0,
    /// <summary>Quantas vezes mais o sensor #0 emite que os demais (>1 cria uma "hot key" na partição).</summary>
    int HotSensorFactor = 1,
    int Seed = 42);

/// <summary>Produz lotes de leituras distribuídas entre os sensores da frota. NÃO é thread-safe.</summary>
public sealed class ReadingFactory
{
    private readonly SensorFleet _fleet;
    private readonly ReadingFactoryOptions _options;
    private readonly SignalGenerator[] _generators;
    private readonly long[] _lastTicks;
    private readonly HashSet<int> _silenced = [];
    private int[] _schedule;
    private readonly Random _random;
    private readonly Queue<SimulatedReading> _recent = new();
    private int _cursor;

    public ReadingFactory(SensorFleet fleet, ReadingFactoryOptions? options = null)
    {
        _fleet = fleet;
        _options = options ?? new ReadingFactoryOptions();
        if (_options.HotSensorFactor < 1) throw new ArgumentOutOfRangeException(nameof(options), "HotSensorFactor deve ser >= 1.");
        if (_options.DuplicateProbability is < 0 or > 1) throw new ArgumentOutOfRangeException(nameof(options), "DuplicateProbability deve estar em [0,1].");

        _random = new Random(_options.Seed);
        _generators = fleet.Sensors
            .Select((s, i) => new SignalGenerator(s.Profile, _options.Seed + i))
            .ToArray();
        _lastTicks = new long[fleet.Sensors.Count];

        // Agenda round-robin: cada sensor uma vez por ciclo; o sensor 0 aparece HotSensorFactor vezes.
        _schedule = Enumerable.Range(0, fleet.Sensors.Count)
            .SelectMany(i => Enumerable.Repeat(i, i == 0 ? _options.HotSensorFactor : 1))
            .ToArray();
    }

    public SensorFleet Fleet => _fleet;

    /// <summary>Quantos sensores estão "mortos" (não emitem mais).</summary>
    public int SilencedCount => _silenced.Count;

    /// <summary>
    /// Silencia uma fração dos sensores (a partir do fim da lista, para não afetar o sensor #0 "quente"): eles param de
    /// emitir. Simula sensores que morrem, para exercitar os alertas de "sem dados". Retorna os índices silenciados.
    /// </summary>
    public IReadOnlyList<int> SilenceFraction(double fraction)
    {
        if (fraction is < 0 or > 1) throw new ArgumentOutOfRangeException(nameof(fraction));

        var count = (int)Math.Round(_fleet.Sensors.Count * fraction);
        var victims = Enumerable.Range(_fleet.Sensors.Count - count, count).Where(i => i > 0).ToList();
        foreach (var index in victims) _silenced.Add(index);

        _schedule = _schedule.Where(i => !_silenced.Contains(i)).ToArray();
        _cursor = 0;
        return victims;
    }

    public List<SimulatedReading> NextBatch(int count, DateTimeOffset now)
    {
        var batch = new List<SimulatedReading>(count);
        for (var i = 0; i < count; i++)
        {
            if (_options.DuplicateProbability > 0 && _recent.Count > 0 && _random.NextDouble() < _options.DuplicateProbability)
            {
                batch.Add(_recent.ToArray()[_random.Next(_recent.Count)]); // reenvio idêntico
                continue;
            }

            if (_schedule.Length == 0) break; // todos silenciados: nada a emitir
            var index = _schedule[_cursor++ % _schedule.Length];
            var sensor = _fleet.Sensors[index];

            // Timestamp estritamente crescente por sensor (a chave natural é sensor+instante).
            // Microssegundos: a precisão do banco.
            var ticks = Math.Max(now.UtcTicks, _lastTicks[index] + 10);
            ticks -= ticks % 10;
            _lastTicks[index] = ticks;
            var timestamp = new DateTimeOffset(ticks, TimeSpan.Zero);

            var reading = new SimulatedReading(sensor.Id, timestamp, _generators[index].Next(timestamp), sensor.Unit);
            batch.Add(reading);

            if (_options.DuplicateProbability > 0)
            {
                _recent.Enqueue(reading);
                if (_recent.Count > 64) _recent.Dequeue();
            }
        }

        return batch;
    }
}
