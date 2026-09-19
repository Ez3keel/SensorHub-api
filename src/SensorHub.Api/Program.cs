using SensorHub.Api.Infrastructure;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using SensorHub.Api.Realtime;
using SensorHub.Api.Security;
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
builder.Services.AddSecurityInfrastructure(builder.Configuration);
builder.Services.AddSecurityServices();
builder.Services.AddApiSecurity(builder.Configuration);
builder.Services.AddHostedService<BootstrapAdminService>(); // depois do migrator (registrado por AddPostgresPersistence)
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

// Atrás de um proxy (nginx do Compose) o IP de origem vem em X-Forwarded-For; sem isso o limiter de login enxergaria SEMPRE o IP do
// proxy e todos os usuários dividiriam um único balde. Só se confia no cabeçalho quando o operador declara que a API NÃO é
// alcançável diretamente: um cliente qualquer poderia forjá-lo e escapar do limite por IP.
// (lido de forma preguiçosa, via options, para respeitar sobreposições de configuração feitas depois do CreateBuilder)
builder.Services.AddOptions<Microsoft.AspNetCore.Builder.ForwardedHeadersOptions>().Configure<IConfiguration>((o, configuration) =>
{
    if (!configuration.GetValue<bool>("Security:TrustForwardedHeaders")) return; // padrão: ForwardedHeaders.None (o middleware não faz nada)
    o.ForwardedHeaders = Microsoft.AspNetCore.HttpOverrides.ForwardedHeaders.XForwardedFor | Microsoft.AspNetCore.HttpOverrides.ForwardedHeaders.XForwardedProto;
    o.KnownIPNetworks.Clear();
    o.KnownProxies.Clear();
});

var app = builder.Build();

app.UseForwardedHeaders();

app.Services.ValidateSecurityConfiguration(); // segredo do JWT fraco/ausente derruba a subida, não a primeira requisição

app.UseExceptionHandler();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseCors(RealtimeExtensions.CorsPolicy);
app.UseAuthentication();
app.UseAuthorization();
// Depois da autorização, de propósito: os esquemas são por política (JWT x chave de API), então a identidade do
// dispositivo só existe em HttpContext.User depois que a política roda. Antes disso o limiter via "anônimo" e agrupava tudo por IP.
app.UseRateLimiter();

app.MapControllers();
app.MapHub<TelemetryHub>("/hubs/telemetry");
app.MapPrometheusScrapingEndpoint(); // GET /metrics (formato Prometheus)

// liveness: o processo está de pé. readiness: as dependências respondem.
app.MapHealthChecks("/health/live", new() { Predicate = _ => false });
app.MapHealthChecks("/health/ready", new() { Predicate = check => check.Tags.Contains("ready") });

app.Run();

// Necessário para WebApplicationFactory<Program> nos testes de integração.
public partial class Program;
