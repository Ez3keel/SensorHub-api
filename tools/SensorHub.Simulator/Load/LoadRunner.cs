using System.Diagnostics;

namespace SensorHub.Simulator.Load;

/// <summary>Destino das leituras geradas (HTTP, MQTT, contador em memória...).</summary>
public interface IReadingSink
{
    Task SendAsync(IReadOnlyList<SimulatedReading> batch, CancellationToken cancellationToken);
}

public sealed record LoadOptions(
    int RatePerSecond, TimeSpan Duration, int BatchSize = 100, int Workers = 1,
    /// <summary>Depois deste tempo, <see cref="SilenceFraction"/> dos sensores param de emitir (cenário "sensor morreu").</summary>
    TimeSpan? SilenceAfter = null, double SilenceFraction = 0);

public sealed record LoadReport(long Sent, long Failed, TimeSpan Elapsed)
{
    public double AchievedRate => Elapsed.TotalSeconds > 0 ? Sent / Elapsed.TotalSeconds : 0;
}

/// <summary>
/// Gera carga em taxa constante (leituras/s) durante um período. Se o destino não acompanha,
/// a taxa alcançada fica abaixo da pedida e o relatório mostra isso, em vez de mascarar.
/// </summary>
public sealed class LoadRunner
{
    private readonly object _factoryLock = new();

    public async Task<LoadReport> RunAsync(
        ReadingFactory factory, IReadingSink sink, LoadOptions options, CancellationToken cancellationToken = default)
    {
        if (options.RatePerSecond <= 0) throw new ArgumentOutOfRangeException(nameof(options), "RatePerSecond deve ser > 0.");
        if (options.BatchSize <= 0) throw new ArgumentOutOfRangeException(nameof(options), "BatchSize deve ser > 0.");
        if (options.Workers <= 0) throw new ArgumentOutOfRangeException(nameof(options), "Workers deve ser > 0.");

        long sent = 0, failed = 0;
        var silenced = false;
        var clock = Stopwatch.StartNew();
        var perWorkerRate = (double)options.RatePerSecond / options.Workers;

        async Task Worker()
        {
            long workerSent = 0;
            while (clock.Elapsed < options.Duration && !cancellationToken.IsCancellationRequested)
            {
                var due = (long)(clock.Elapsed.TotalSeconds * perWorkerRate);
                var deficit = due - workerSent;
                if (deficit < 1)
                {
                    await Task.Delay(1, CancellationToken.None);
                    continue;
                }

                var size = (int)Math.Min(deficit, options.BatchSize);
                List<SimulatedReading> batch;
                lock (_factoryLock)
                {
                    if (!silenced && options.SilenceAfter is { } after && clock.Elapsed >= after)
                    {
                        silenced = true;
                        factory.SilenceFraction(options.SilenceFraction);
                    }

                    batch = factory.NextBatch(size, DateTimeOffset.UtcNow);
                }
                workerSent += size;

                try
                {
                    await sink.SendAsync(batch, cancellationToken);
                    Interlocked.Add(ref sent, size);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception)
                {
                    Interlocked.Add(ref failed, size);
                }
            }
        }

        await Task.WhenAll(Enumerable.Range(0, options.Workers).Select(_ => Worker()));
        clock.Stop();
        return new LoadReport(Interlocked.Read(ref sent), Interlocked.Read(ref failed), clock.Elapsed);
    }
}

/// <summary>Sink de "dry-run": só conta. Mede o teto do próprio gerador.</summary>
public sealed class CountingSink : IReadingSink
{
    private long _count;
    public long Count => Interlocked.Read(ref _count);

    public Task SendAsync(IReadOnlyList<SimulatedReading> batch, CancellationToken cancellationToken)
    {
        Interlocked.Add(ref _count, batch.Count);
        return Task.CompletedTask;
    }
}
