using SensorHub.Api.Infrastructure;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using SensorHub.Api.Realtime;
using SensorHub.Infrastructure.Observability;
using SensorHub.Application;
using SensorHub.Infrastructure;
using SensorHub.Infrastructure.Kafka;
using SensorHub.Infrastructure.Persistence;
using SensorHub.Infrastructure.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers().AddJsonOptions(options =>
    // enums como texto ("Temperature", "GreaterThan"): legível e estável (não quebra se a ordem do enum mudar)
    options.JsonSerializerOptions.Converters.Add(new System.Text.Json.Serialization.JsonStringEnumConverter()));
builder.Services.AddOpenApi();
builder.Services.AddProblemDetails();
builder.Services.AddExceptionHandler<GlobalExceptionHandler>();

builder.Services.AddIngestion(builder.Configuration);
builder.Services.AddKafkaMessaging(builder.Configuration);
builder.Services.AddKafkaReadingPublisher();
builder.Services.AddPostgresPersistence(builder.Configuration);
builder.Services.AddReadingQueries();
builder.Services.AddManagementRepositories();
builder.Services.AddManagementServices();
builder.Services.AddRealtime(builder.Configuration);
builder.Services.AddSensorHubObservability(builder.Configuration, "sensorhub-api",
    metrics: m => m.AddAspNetCoreInstrumentation().AddHttpClientInstrumentation().AddPrometheusExporter(),
    tracing: t => t.AddHttpClientInstrumentation().AddAspNetCoreInstrumentation(o =>
        // health checks e o próprio scrape de métricas não são tráfego de negócio: sem o filtro poluiriam todos os traces
        o.Filter = ctx => !ctx.Request.Path.StartsWithSegments("/health") && !ctx.Request.Path.StartsWithSegments("/metrics")));
builder.Services.AddRedisState(builder.Configuration);

builder.Services.AddHealthChecks()
    .AddCheck<KafkaHealthCheck>("kafka", tags: ["ready"])
    .AddCheck<PostgresHealthCheck>("postgres", tags: ["ready"])
    .AddCheck<RedisHealthCheck>("redis", tags: ["ready"]);

var app = builder.Build();

app.UseExceptionHandler();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseCors(RealtimeExtensions.CorsPolicy);

app.MapControllers();
app.MapHub<TelemetryHub>("/hubs/telemetry");
app.MapPrometheusScrapingEndpoint(); // GET /metrics (formato Prometheus)

// liveness: o processo está de pé. readiness: as dependências respondem.
app.MapHealthChecks("/health/live", new() { Predicate = _ => false });
app.MapHealthChecks("/health/ready", new() { Predicate = check => check.Tags.Contains("ready") });

app.Run();

// Necessário para WebApplicationFactory<Program> nos testes de integração.
public partial class Program;
