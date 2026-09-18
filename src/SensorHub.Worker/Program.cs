using SensorHub.Infrastructure;

var builder = Host.CreateApplicationBuilder(args);

// Cada consumer é um "papel" com o seu consumer group. Um processo pode rodar vários (dev) ou um só
// (produção: escalar cada papel de forma independente). Ex.: Worker:Consumers=persistence,lastvalue
var roles = (builder.Configuration.GetSection("Worker:Consumers").Get<string[]>() ?? ["persistence", "lastvalue"])
    .Select(r => r.Trim().ToLowerInvariant()).ToHashSet();

// Ordem importa: hosted services iniciam na ordem do registro (tópicos e migrations antes dos consumers).
builder.Services.AddKafkaMessaging(builder.Configuration);

if (roles.Contains("persistence"))
{
    builder.Services.AddPostgresPersistence(builder.Configuration);
    builder.Services.AddReadingPersistenceConsumer(builder.Configuration);
}

if (roles.Contains("lastvalue"))
{
    builder.Services.AddRedisState(builder.Configuration);
    builder.Services.AddLastValueConsumer(builder.Configuration);
}

builder.Build().Run();