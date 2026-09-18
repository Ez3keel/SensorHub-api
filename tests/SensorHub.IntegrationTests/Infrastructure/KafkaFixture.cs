using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Testcontainers.Kafka;

namespace SensorHub.IntegrationTests.Infrastructure;

/// <summary>
/// UM broker Kafka real (container) compartilhado por todos os testes da coleção: subir Kafka custa
/// segundos, então paga-se uma vez. Cada teste isola os seus dados usando sensorIds próprios.
/// </summary>
public sealed class KafkaFixture : IAsyncLifetime
{
    private readonly KafkaContainer _container = new KafkaBuilder("confluentinc/cp-kafka:7.6.1").Build();

    public string BootstrapServers => _container.GetBootstrapAddress();
    public ApiFactory Api { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        Api = new ApiFactory(BootstrapServers);
        _ = Api.Server; // força o host a subir (e o provisionamento dos tópicos) antes do primeiro teste
    }

    public async Task DisposeAsync()
    {
        await Api.DisposeAsync();
        await _container.DisposeAsync();
    }
}

[CollectionDefinition(Name)]
public sealed class KafkaCollection : ICollectionFixture<KafkaFixture>
{
    public const string Name = "kafka";
}

/// <summary>A API real (mesmo Program.cs de produção) apontando para o Kafka do container.</summary>
public sealed class ApiFactory(string bootstrapServers, Dictionary<string, string?>? overrides = null)
    : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Testing");
        builder.ConfigureAppConfiguration((_, config) =>
        {
            var settings = new Dictionary<string, string?> { ["Kafka:BootstrapServers"] = bootstrapServers };
            if (overrides is not null)
                foreach (var (key, value) in overrides) settings[key] = value;
            config.AddInMemoryCollection(settings);
        });
    }
}
