using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace SensorHub.Infrastructure.Persistence;

/// <summary>Aplica as migrations pendentes antes de qualquer consumer/endpoint usar o banco. O EF protege a execução concorrente com advisory lock.</summary>
public sealed class DatabaseMigrator(
    IServiceScopeFactory scopes,
    IOptions<PersistenceOptions> options,
    ILogger<DatabaseMigrator> logger) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!options.Value.MigrateOnStartup) return;

        // Banco pode estar subindo (docker compose): tenta por ~60s.
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await using var scope = scopes.CreateAsyncScope();
                var db = scope.ServiceProvider.GetRequiredService<SensorHubDbContext>();
                await db.Database.MigrateAsync(cancellationToken);
                logger.LogInformation("Migrations aplicadas.");
                return;
            }
            catch (Exception ex) when (attempt < 12 && !cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("Banco indisponível para migrar (tentativa {Attempt}/12): {Reason}", attempt, ex.Message);
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
            }
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
