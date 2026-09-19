using Microsoft.AspNetCore.Mvc;
using SensorHub.Application.Security;

namespace SensorHub.MqttBridge;

/// <summary>
/// Backend HTTP do plugin de autenticação do Mosquitto (modo "status": 200 = permitido, 401/403 = negado).
/// Estes endpoints NÃO são públicos: só a rede interna do broker os alcança (o bridge não publica a porta no host).
/// </summary>
public static class MqttAuthEndpoints
{
    public sealed record AuthRequest(string? Username, string? Password, string? Clientid);
    public sealed record SuperuserRequest(string? Username);
    public sealed record AclRequest(string? Username, string? Topic, int Acc, string? Clientid);

    public static IEndpointRouteBuilder MapMqttAuth(this IEndpointRouteBuilder app)
    {
        app.MapPost("/mqtt/auth", async ([FromBody] AuthRequest request, MqttAccessPolicy policy, CancellationToken ct) =>
            await policy.AuthenticateAsync(request.Username, request.Password, ct) ? Results.Ok() : Results.StatusCode(StatusCodes.Status401Unauthorized));

        app.MapPost("/mqtt/superuser", ([FromBody] SuperuserRequest request, MqttAccessPolicy policy) =>
            policy.IsBridge(request.Username) ? Results.Ok() : Results.StatusCode(StatusCodes.Status403Forbidden));

        app.MapPost("/mqtt/acl", async ([FromBody] AclRequest request, MqttAccessPolicy policy, IServiceScopeFactory scopes, CancellationToken ct) =>
        {
            if (string.IsNullOrEmpty(request.Username) || string.IsNullOrEmpty(request.Topic))
                return Results.StatusCode(StatusCodes.Status403Forbidden);
            if (!MqttAccessPolicy.DeviceMayAccess(request.Username, request.Topic, request.Acc))
                return Results.StatusCode(StatusCodes.Status403Forbidden);

            // O broker só autentica no CONNECT; um dispositivo desativado depois continuaria conectado. A ACL é consultada a cada
            // publicação (com o cache curto do plugin), então é aqui que "desativar o dispositivo" passa a valer.
            await using var scope = scopes.CreateAsyncScope();
            var device = await scope.ServiceProvider.GetRequiredService<IDeviceRepository>().GetAsync(Guid.Parse(request.Username), ct);
            return device is { Active: true } ? Results.Ok() : Results.StatusCode(StatusCodes.Status403Forbidden);
        });

        return app;
    }
}
