using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SensorHub.Application.Alerting;
using SensorHub.Infrastructure.Persistence;

namespace SensorHub.Infrastructure.Kafka;

/// <summary>
/// Varredura periódica das regras "sem dados". Um sensor que para de reportar não gera evento nenhum no
/// Kafka, então a detecção precisa de um relógio. Com várias instâncias do Worker, um lease no Redis garante que só
/// UMA varre por vez (a idempotência dos alertas seria a segunda linha de defesa, mas evitar a corrida é mais barato).
/// </summary>
public sealed class NoDataSweeper(
    AlertEngine engine,
    IDistributedLease lease,
    IOptions<AlertingOptions> options,
    ILogger<NoDataSweeper> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var interval = TimeSpan.FromSeconds(options.Value.NoDataSweepSeconds);
        using var timer = new PeriodicTimer(interval);

        while (await SafeWaitAsync(timer, stoppingToken))
        {
            try
            {
                // O lease dura um pouco menos que o intervalo: expira sozinho se esta instância morrer no meio.
                await using var held = await lease.TryAcquireAsync("nodata-sweep", interval * 0.9, stoppingToken);
                if (held is null) continue; // outra instância está varrendo

                var transitions = await engine.SweepNoDataAsync(TimeSpan.FromSeconds(options.Value.MaxProcessingDelaySeconds), stoppingToken);
                if (transitions > 0) logger.LogInformation("Varredura 'sem dados': {Count} transição(ões).", transitions);
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                // Falha da varredura (Redis/banco/Kafka fora) não derruba o processo: tenta de novo no próximo tick.
                logger.LogWarning(ex, "Falha na varredura 'sem dados'; nova tentativa no próximo ciclo.");
            }
        }
    }

    private static async Task<bool> SafeWaitAsync(PeriodicTimer timer, CancellationToken cancellationToken)
    {
        try { return await timer.WaitForNextTickAsync(cancellationToken); }
        catch (OperationCanceledException) { return false; }
    }
}
