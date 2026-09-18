using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace SensorHub.Infrastructure.Persistence;

/// <summary>
/// Dono do esquema do banco (migrations). As leituras NÃO passam pelo EF: o caminho quente usa
/// COPY/unnest do Npgsql direto (ver <see cref="PostgresReadingStore"/>), porque o change tracker do EF
/// custaria ordens de grandeza de vazão para inserts em massa.
/// </summary>
public sealed class SensorHubDbContext(DbContextOptions<SensorHubDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
    }
}

/// <summary>Usada só pela ferramenta <c>dotnet ef</c> (design-time).</summary>
public sealed class SensorHubDbContextFactory : IDesignTimeDbContextFactory<SensorHubDbContext>
{
    public SensorHubDbContext CreateDbContext(string[] args)
    {
        var connection = Environment.GetEnvironmentVariable("SENSORHUB_CONNECTION")
                         ?? "Host=localhost;Port=5433;Database=sensorhub;Username=sensorhub;Password=sensorhub";

        var options = new DbContextOptionsBuilder<SensorHubDbContext>().UseNpgsql(connection).Options;
        return new SensorHubDbContext(options);
    }
}
