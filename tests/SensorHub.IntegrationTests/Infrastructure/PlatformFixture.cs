using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Npgsql;
using SensorHub.Infrastructure.Kafka;
using SensorHub.Infrastructure.Persistence;
using Testcontainers.Kafka;
using StackExchange.Redis;
using Testcontainers.PostgreSql;
using Testcontainers.Redis;

namespace SensorHub.IntegrationTests.Infrastructure;

/// <summary>
/// A "plataforma" real de testes: broker Kafka e TimescaleDB em containers, compartilhados por toda a
/// coleção (subir custa segundos, então paga-se uma vez). Cada teste isola seus dados com sensorIds e
/// tópicos próprios, sem precisar limpar nada.
/// </summary>
public sealed class PlatformFixture : IAsyncLifetime
{
    private readonly KafkaContainer _kafka = new KafkaBuilder("confluentinc/cp-kafka:7.6.1").Build();
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("timescale/timescaledb:latest-pg16")
        .WithDatabase("sensorhub").WithUsername("sensorhub").WithPassword("sensorhub").Build();

    private readonly RedisContainer _redis = new RedisBuilder("redis:7-alpine").Build();

    public string BootstrapServers => _kafka.GetBootstrapAddress();
    public string RedisConnectionString => _redis.GetConnectionString();
    public IConnectionMultiplexer Redis { get; private set; } = null!;
    public string ConnectionString => _postgres.GetConnectionString();
    public NpgsqlDataSource DataSource { get; private set; } = null!;
    public ApiFactory Api { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        await Task.WhenAll(_kafka.StartAsync(), _postgres.StartAsync(), _redis.StartAsync());
        Redis = await ConnectionMultiplexer.ConnectAsync(RedisConnectionString);

        DataSource = NpgsqlDataSource.Create(ConnectionString);
        await using (var db = new SensorHubDbContext(new DbContextOptionsBuilder<SensorHubDbContext>().UseNpgsql(DataSource).Options))
            await db.Database.MigrateAsync();

        Api = new ApiFactory(BootstrapServers, postgresConnection: ConnectionString, redisConnection: RedisConnectionString);
        _ = Api.Server; // força o host a subir (e o provisionamento dos tópicos) antes do primeiro teste
    }

    public async Task DisposeAsync()
    {
        await Api.DisposeAsync();
        await DataSource.DisposeAsync();
        await _kafka.DisposeAsync();
        await Redis.DisposeAsync();
        await _postgres.DisposeAsync();
        await _redis.DisposeAsync();
    }

    /// <summary>Cria um tópico de leituras (e sua DLQ) exclusivos de um teste, para que não enxerguem dados uns dos outros.</summary>
    public async Task<KafkaOptions> CreateIsolatedTopicsAsync(int partitions = 6)
    {
        var id = Guid.NewGuid().ToString("N")[..10];
        var options = new KafkaOptions
        {
            BootstrapServers = BootstrapServers,
            ReadingsTopic = $"readings-{id}",
            ReadingsDlqTopic = $"readings-{id}.dlq",
            ReadingsPartitions = partitions
        };

        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = BootstrapServers }).Build();
        await admin.CreateTopicsAsync(
        [
            new TopicSpecification { Name = options.ReadingsTopic, NumPartitions = partitions, ReplicationFactor = 1 },
            new TopicSpecification { Name = options.ReadingsDlqTopic, NumPartitions = 1, ReplicationFactor = 1 }
        ]);
        return options;
    }

    public SensorHub.Infrastructure.Redis.RedisLastValueStore CreateLastValueStore(int ttlDays = 7) =>
        new(Redis, Microsoft.Extensions.Options.Options.Create(new SensorHub.Infrastructure.Redis.RedisOptions { KeyPrefix = "test:", LastValueTtlDays = ttlDays }));

    public PostgresReadingStore CreateStore(BulkInsertStrategy strategy = BulkInsertStrategy.Unnest) =>
        new(DataSource, Microsoft.Extensions.Options.Options.Create(new PersistenceOptions { Strategy = strategy }));

    public async Task<long> CountReadingsAsync(IEnumerable<Guid> sensors)
    {
        await using var command = DataSource.CreateCommand("SELECT count(*) FROM readings WHERE sensor_id = ANY(@ids)");
        command.Parameters.AddWithValue("ids", sensors.ToArray());
        return (long)(await command.ExecuteScalarAsync())!;
    }
}

[CollectionDefinition(Name)]
public sealed class PlatformCollection : ICollectionFixture<PlatformFixture>
{
    public const string Name = "platform";
}

/// <summary>A API real (mesmo Program.cs de produção) apontando para o Kafka do container.</summary>
public sealed class ApiFactory(string bootstrapServers, Dictionary<string, string?>? overrides = null, string? postgresConnection = null, string? redisConnection = null)
    : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Testing");
        builder.ConfigureAppConfiguration((_, config) =>
        {
            var settings = new Dictionary<string, string?>
            {
                ["Kafka:BootstrapServers"] = bootstrapServers,
                // O banco de teste já é migrado pela fixture; a API não precisa (nem deve) migrar de novo.
                ["Persistence:MigrateOnStartup"] = "false",
                ["Redis:KeyPrefix"] = "test:",
                ["Redis:ConnectionString"] = redisConnection ?? "localhost:1,abortConnect=false,connectTimeout=500",
                ["ConnectionStrings:Postgres"] = postgresConnection ?? "Host=localhost;Port=1;Database=none;Username=none;Password=none;Timeout=2"
            };
            if (overrides is not null)
                foreach (var (key, value) in overrides) settings[key] = value;
            config.AddInMemoryCollection(settings);
        });
    }
}
