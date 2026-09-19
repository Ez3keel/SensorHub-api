using SensorHub.Infrastructure;

var builder = Host.CreateApplicationBuilder(args);

// Cada consumer é um "papel" com o seu consumer group. Um processo pode rodar vários (dev) ou um só
// (produção: escalar cada papel de forma independente). Ex.: Worker:Consumers=persistence,lastvalue,alerts
var roles = (builder.Configuration.GetSection("Worker:Consumers").Get<string[]>() ?? ["persistence", "lastvalue", "alerts"])
    .Select(r => r.Trim().ToLowerInvariant()).ToHashSet();

var needsPostgres = roles.Contains("persistence") || roles.Contains("alerts");
var needsRedis = roles.Contains("lastvalue") || roles.Contains("alerts");

// Ordem importa: hosted services iniciam na ordem do registro (tópicos e migrations antes dos consumers).
builder.Services.AddKafkaMessaging(builder.Configuration);
if (needsPostgres) builder.Services.AddPostgresPersistence(builder.Configuration);
if (needsRedis) builder.Services.AddRedisState(builder.Configuration);

if (roles.Contains("persistence")) builder.Services.AddReadingPersistenceConsumer(builder.Configuration);
if (roles.Contains("lastvalue")) builder.Services.AddLastValueConsumer(builder.Configuration);
if (roles.Contains("alerts")) builder.Services.AddAlertEngine(builder.Configuration);

builder.Build().Run();