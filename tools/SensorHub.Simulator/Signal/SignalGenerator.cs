using SensorHub.Domain.Sensors;

namespace SensorHub.Simulator.Signal;

/// <summary>
/// Gera o valor de um sensor a cada instante. Determinístico dado o seed: a mesma execução do
/// simulador reproduz a mesma carga, o que torna comparações de benchmark justas.
/// </summary>
public sealed class SignalGenerator
{
    private readonly SensorProfile _profile;
    private readonly Random _random;
    private readonly double _phaseOffset;

    public SignalGenerator(SensorProfile profile, int seed)
    {
        _profile = profile;
        _random = new Random(seed);
        _phaseOffset = _random.NextDouble() * 2 * Math.PI; // sensores não oscilam todos em fase
    }

    public double Next(DateTimeOffset timestamp)
    {
        var periodMs = _profile.Period.TotalMilliseconds;
        var position = timestamp.ToUnixTimeMilliseconds() % (long)periodMs / periodMs;
        var value = _profile.Baseline
                    + _profile.Amplitude * Math.Sin(2 * Math.PI * position + _phaseOffset)
                    + Gaussian() * _profile.NoiseStdDev;

        if (_random.NextDouble() < _profile.SpikeProbability)
            value += _profile.SpikeMagnitude * (_random.Next(2) == 0 ? -1 : 1);

        // Nunca gera valor fisicamente impossível: o simulador não deve ser rejeitado pela validação.
        var (min, max) = _profile.Metric.PlausibleRange();
        return Math.Round(Math.Clamp(value, min, max), 3);
    }

    /// <summary>Box-Muller: ruído gaussiano padrão.</summary>
    private double Gaussian()
    {
        var u1 = 1.0 - _random.NextDouble();
        var u2 = _random.NextDouble();
        return Math.Sqrt(-2.0 * Math.Log(u1)) * Math.Cos(2.0 * Math.PI * u2);
    }
}
