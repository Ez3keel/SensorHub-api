using OpenTelemetry.Metrics;
using SensorHub.Infrastructure;
using SensorHub.Infrastructure.Kafka;
using SensorHub.Infrastructure.Observability;
using SensorHub.Infrastructure.Persistence;
using SensorHub.Infrastructure.Redis;

// O Worker é um host web MÍNIMO: os consumers rodam como hosted services, e o Kestrel só serve /metrics (Prometheus) e
// /health/* (probes de container). Sem isso, um processo de fundo seria invisível para o monitoramento.
var builder = WebApplication.CreateBuilder(args);

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

builder.Services.AddSensorHubObservability(builder.Configuration, "sensorhub-worker",
    metrics: m => m.AddPrometheusExporter());

var health = builder.Services.AddHealthChecks().AddCheck<KafkaHealthCheck>("kafka", tags: ["ready"]);
if (needsPostgres) health.AddCheck<PostgresHealthCheck>("postgres", tags: ["ready"]);
if (needsRedis) health.AddCheck<RedisHealthCheck>("redis", tags: ["ready"]);

var app = builder.Build();

app.MapPrometheusScrapingEndpoint(); // GET /metrics
app.MapHealthChecks("/health/live", new() { Predicate = _ => false });
app.MapHealthChecks("/health/ready", new() { Predicate = check => check.Tags.Contains("ready") });

app.Run();
