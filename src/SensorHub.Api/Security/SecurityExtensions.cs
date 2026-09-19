using System.Security.Claims;
using System.Text.Encodings.Web;
using System.Threading.RateLimiting;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;
using SensorHub.Application.Security;
using SensorHub.Domain.Security;
using SensorHub.Infrastructure.Security;

namespace SensorHub.Api.Security;

public static class Policies
{
    public const string Viewer = "Viewer";
    public const string Operator = "Operator";
    public const string Admin = "Admin";
    public const string Device = "Device";

    public const string ApiKeyScheme = "ApiKey";
    public const string DeviceIdClaim = "device_id";
    public const string DeviceNameClaim = "device_name";

    public const string IngestionLimiter = "ingestion";
    public const string AuthLimiter = "auth";
    public const string ApiLimiter = "api";
}

public sealed class RateLimitOptions
{
    public const string SectionName = "Security:RateLimits";

    /// <summary>Requisições por segundo sustentadas POR DISPOSITIVO (cada requisição pode ter até 1000 leituras).</summary>
    public int IngestionPerSecond { get; set; } = 50;

    /// <summary>Rajada tolerada: o balde de tokens acumula até este total (um dispositivo que ficou offline reenvia o buffer de uma vez).</summary>
    public int IngestionBurst { get; set; } = 200;

    /// <summary>Tentativas de login/refresh por minuto POR IP: freia força bruta e credential stuffing.</summary>
    public int AuthPerMinute { get; set; } = 10;

    /// <summary>Requisições por minuto por usuário (ou por IP, se anônimo) nos endpoints de leitura e administração.</summary>
    public int ApiPerMinute { get; set; } = 600;
}

// ============================================================================ autorização por papel

/// <summary>"Este papel ou superior": Admin ⊃ Operator ⊃ Viewer.</summary>
public sealed class MinimumRoleRequirement(UserRole minimum) : IAuthorizationRequirement
{
    public UserRole Minimum { get; } = minimum;
}

public sealed class DeviceRequirement : IAuthorizationRequirement;

/// <summary>
/// Com <c>Security:Enabled=false</c> (testes e quick-start local) todo requisito é satisfeito. Lido a cada requisição, não no
/// registro, para que a configuração do host de teste valha.
/// </summary>
public sealed class MinimumRoleHandler(IOptions<SecurityOptions> options) : AuthorizationHandler<MinimumRoleRequirement>
{
    protected override Task HandleRequirementAsync(AuthorizationHandlerContext context, MinimumRoleRequirement requirement)
    {
        if (!options.Value.Enabled) { context.Succeed(requirement); return Task.CompletedTask; }

        var role = context.User.FindFirstValue("role");
        if (Enum.TryParse<UserRole>(role, out var actual) && actual >= requirement.Minimum)
            context.Succeed(requirement);

        return Task.CompletedTask;
    }
}

public sealed class DeviceHandler(IOptions<SecurityOptions> options) : AuthorizationHandler<DeviceRequirement>
{
    protected override Task HandleRequirementAsync(AuthorizationHandlerContext context, DeviceRequirement requirement)
    {
        if (!options.Value.Enabled || context.User.HasClaim(c => c.Type == Policies.DeviceIdClaim))
            context.Succeed(requirement);

        return Task.CompletedTask;
    }
}

// ============================================================================ chave de API (dispositivos)

/// <summary>
/// Autentica dispositivos pelo header <c>X-Api-Key</c>. Sem o header, "não há credencial" (NoResult): a política decide o
/// que fazer (401). Com chave inválida, rotacionada ou de dispositivo desativado, falha.
/// </summary>
public sealed class ApiKeyAuthenticationHandler(
    IOptionsMonitor<AuthenticationSchemeOptions> options, ILoggerFactory logger, UrlEncoder encoder, IDeviceAuthenticator devices)
    : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
{
    public const string HeaderName = "X-Api-Key";

    protected override async Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        if (!Request.Headers.TryGetValue(HeaderName, out var header) || string.IsNullOrWhiteSpace(header))
            return AuthenticateResult.NoResult();

        var device = await devices.AuthenticateAsync(header.ToString(), Context.RequestAborted);
        if (device is null) return AuthenticateResult.Fail("Chave de API inválida.");

        var identity = new ClaimsIdentity(
        [
            new Claim(Policies.DeviceIdClaim, device.Id.ToString()),
            new Claim(Policies.DeviceNameClaim, device.Name)
        ], Policies.ApiKeyScheme);
        return AuthenticateResult.Success(new AuthenticationTicket(new ClaimsPrincipal(identity), Policies.ApiKeyScheme));
    }
}

public static class SecurityExtensions
{
    public static IServiceCollection AddApiSecurity(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<RateLimitOptions>(configuration.GetSection(RateLimitOptions.SectionName));

        services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
            .AddJwtBearer()
            .AddScheme<AuthenticationSchemeOptions, ApiKeyAuthenticationHandler>(Policies.ApiKeyScheme, _ => { });

        // Configuração PREGUIÇOSA do JWT: lida quando o esquema é usado, com toda a configuração já montada.
        services.AddOptions<JwtBearerOptions>(JwtBearerDefaults.AuthenticationScheme)
            .Configure<IOptions<SecurityOptions>>((jwt, security) =>
            {
                var settings = security.Value;
                if (!settings.Enabled) return; // sem segurança ligada não se exige chave

                jwt.TokenValidationParameters = new TokenValidationParameters
                {
                    ValidateIssuer = true, ValidIssuer = settings.JwtIssuer,
                    ValidateAudience = true, ValidAudience = settings.JwtAudience,
                    ValidateIssuerSigningKey = true, IssuerSigningKey = JwtAccessTokenIssuer.SigningKey(settings),
                    ValidateLifetime = true,
                    ClockSkew = TimeSpan.FromSeconds(30),    // tolerância curta para relógios ligeiramente diferentes
                    ValidAlgorithms = [SecurityAlgorithms.HmacSha256], // nunca aceita "alg: none" nem troca de algoritmo
                    RoleClaimType = "role",
                    NameClaimType = "sub"
                };
                jwt.MapInboundClaims = false; // mantém os nomes de claim do JWT (sub, email) em vez de mapeá-los para URIs da Microsoft

                // WebSockets do navegador não conseguem enviar o header Authorization: o SignalR manda o token na query string.
                jwt.Events = new JwtBearerEvents
                {
                    OnMessageReceived = ctx =>
                    {
                        var token = ctx.Request.Query["access_token"];
                        if (!string.IsNullOrEmpty(token) && ctx.HttpContext.Request.Path.StartsWithSegments("/hubs"))
                            ctx.Token = token;
                        return Task.CompletedTask;
                    }
                };
            });

        services.AddSingleton<IAuthorizationHandler, MinimumRoleHandler>();
        services.AddSingleton<IAuthorizationHandler, DeviceHandler>();
        services.AddAuthorizationBuilder()
            .AddPolicy(Policies.Viewer, p => p.AddAuthenticationSchemes(JwtBearerDefaults.AuthenticationScheme).AddRequirements(new MinimumRoleRequirement(UserRole.Viewer)))
            .AddPolicy(Policies.Operator, p => p.AddAuthenticationSchemes(JwtBearerDefaults.AuthenticationScheme).AddRequirements(new MinimumRoleRequirement(UserRole.Operator)))
            .AddPolicy(Policies.Admin, p => p.AddAuthenticationSchemes(JwtBearerDefaults.AuthenticationScheme).AddRequirements(new MinimumRoleRequirement(UserRole.Admin)))
            .AddPolicy(Policies.Device, p => p.AddAuthenticationSchemes(Policies.ApiKeyScheme).AddRequirements(new DeviceRequirement()));

        services.AddRateLimiter(limiter =>
        {
            limiter.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
            limiter.OnRejected = async (context, cancellationToken) =>
            {
                if (context.Lease.TryGetMetadata(MetadataName.RetryAfter, out var retryAfter))
                    context.HttpContext.Response.Headers.RetryAfter = ((int)Math.Ceiling(retryAfter.TotalSeconds)).ToString();
                else
                    // A janela deslizante não informa o tempo de espera. Um segmento dura Window/SegmentsPerWindow = 10 s: só depois dele
                    // um permit volta a existir. Dizer "1" faria os clientes bem-comportados insistirem inutilmente (medido no cadastro da frota).
                    context.HttpContext.Response.Headers.RetryAfter = "10";

                // WriteAsJsonAsync sobrescreve o Content-Type se não o receber explicitamente.
                await context.HttpContext.Response.WriteAsJsonAsync(new
                {
                    status = 429, title = "Muitas requisições", detail = "Limite de requisições excedido. Tente novamente após o intervalo indicado em Retry-After."
                }, options: null, contentType: "application/problem+json", cancellationToken);
            };

            // Ingestão: um balde de tokens POR DISPOSITIVO. Um dispositivo com defeito (laço infinito) esgota o SEU balde,
            // não a capacidade dos outros. Sem autenticação (segurança desligada) a partição é o IP.
            limiter.AddPolicy(Policies.IngestionLimiter, context =>
            {
                var settings = context.RequestServices.GetRequiredService<IOptions<RateLimitOptions>>().Value;
                var key = context.User.FindFirstValue(Policies.DeviceIdClaim) ?? $"ip:{context.Connection.RemoteIpAddress}";
                return RateLimitPartition.GetTokenBucketLimiter(key, _ => new TokenBucketRateLimiterOptions
                {
                    TokenLimit = settings.IngestionBurst,
                    TokensPerPeriod = settings.IngestionPerSecond,
                    ReplenishmentPeriod = TimeSpan.FromSeconds(1),
                    QueueLimit = 0,          // sem fila: se não há token, responde 429 na hora (o cliente reenvia com backoff)
                    AutoReplenishment = true
                });
            });

            // Login e refresh: janela fixa POR IP (o usuário ainda não é conhecido). Freia força bruta e credential stuffing.
            limiter.AddPolicy(Policies.AuthLimiter, context =>
            {
                var settings = context.RequestServices.GetRequiredService<IOptions<RateLimitOptions>>().Value;
                return RateLimitPartition.GetFixedWindowLimiter($"ip:{context.Connection.RemoteIpAddress}", _ => new FixedWindowRateLimiterOptions
                {
                    PermitLimit = settings.AuthPerMinute, Window = TimeSpan.FromMinutes(1), QueueLimit = 0
                });
            });

            // Demais endpoints: por usuário (sub do JWT), ou por IP se anônimo.
            limiter.AddPolicy(Policies.ApiLimiter, context =>
            {
                var settings = context.RequestServices.GetRequiredService<IOptions<RateLimitOptions>>().Value;
                var key = context.User.FindFirstValue("sub") ?? $"ip:{context.Connection.RemoteIpAddress}";
                return RateLimitPartition.GetSlidingWindowLimiter(key, _ => new SlidingWindowRateLimiterOptions
                {
                    PermitLimit = settings.ApiPerMinute, Window = TimeSpan.FromMinutes(1), SegmentsPerWindow = 6, QueueLimit = 0
                });
            });
        });

        return services;
    }

    /// <summary>Falha na subida (não na 1ª requisição) se a segurança está ligada mas o segredo do JWT é fraco ou ausente.</summary>
    public static void ValidateSecurityConfiguration(this IServiceProvider services)
    {
        var settings = services.GetRequiredService<IOptions<SecurityOptions>>().Value;
        if (!settings.Enabled) return;

        _ = JwtAccessTokenIssuer.SigningKey(settings);

        // A chave de desenvolvimento está no repositório: qualquer pessoa poderia forjar tokens de administrador. Em Production, recusa.
        if (services.GetRequiredService<IHostEnvironment>().IsProduction() && settings.JwtSigningKey.Contains("dev-only", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Security:JwtSigningKey é a chave de DESENVOLVIMENTO (pública no repositório). Defina uma chave própria por variável de ambiente.");
    }
}

/// <summary>Cria o primeiro administrador em uma instalação nova (sem usuários), depois das migrations.</summary>
public sealed class BootstrapAdminService(IServiceScopeFactory scopes, IOptions<SecurityOptions> options, ILogger<BootstrapAdminService> logger) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var settings = options.Value;
        if (!settings.Enabled || string.IsNullOrWhiteSpace(settings.BootstrapAdmin.Email)) return;

        try
        {
            await using var scope = scopes.CreateAsyncScope();
            await scope.ServiceProvider.GetRequiredService<UserService>()
                .EnsureBootstrapAdminAsync(settings.BootstrapAdmin.Email, settings.BootstrapAdmin.Password, cancellationToken);
        }
        catch (Exception ex)
        {
            // Senha do bootstrap fora da política é erro de configuração: falha alto e claro, em vez de subir sem ninguém para entrar.
            logger.LogCritical(ex, "Não foi possível criar o administrador inicial. Verifique Security:BootstrapAdmin (a senha segue a política).");
            throw;
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
