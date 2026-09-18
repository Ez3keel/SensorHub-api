namespace SensorHub.Infrastructure.Persistence;

public enum BulkInsertStrategy
{
    /// <summary>COPY binário para uma tabela temporária + INSERT ... ON CONFLICT DO NOTHING.</summary>
    CopyStaging = 0,

    /// <summary>Um único INSERT ... SELECT FROM unnest(arrays) ON CONFLICT DO NOTHING.</summary>
    Unnest = 1
}

public sealed class PersistenceOptions
{
    public const string SectionName = "Persistence";
    public const string ConnectionStringName = "Postgres";

    public BulkInsertStrategy Strategy { get; set; } = BulkInsertStrategy.Unnest;

    /// <summary>Aplicar as migrations ao subir (API/Worker). Em produção costuma ser um passo de deploy separado.</summary>
    public bool MigrateOnStartup { get; set; } = true;

    public int CommandTimeoutSeconds { get; set; } = 30;
}
