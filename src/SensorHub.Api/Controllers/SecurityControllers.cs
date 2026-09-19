using System.Security.Claims;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.RateLimiting;
using SensorHub.Api.Security;
using SensorHub.Application.Security;
using SensorHub.Domain.Security;

namespace SensorHub.Api.Controllers;

public sealed record UserResponse(Guid Id, string Email, UserRole Role, bool Active, DateTimeOffset CreatedAt)
{
    public static UserResponse From(User u) => new(u.Id, u.Email, u.Role, u.Active, u.CreatedAt);
}

public sealed record DeviceResponse(Guid Id, string Name, string ApiKeyHint, bool Active, DateTimeOffset CreatedAt, DateTimeOffset? KeyRotatedAt)
{
    public static DeviceResponse From(Device d) => new(d.Id, d.Name, d.ApiKeyHint, d.Active, d.CreatedAt, d.KeyRotatedAt);
}

/// <summary>Login, refresh rotativo e logout. Limitado por IP: é o alvo natural de força bruta.</summary>
[ApiController]
[Route("api/auth")]
public sealed class AuthController(AuthService auth) : ControllerBase
{
    public sealed record LoginRequest(string? Email, string? Password);
    public sealed record RefreshRequest(string? RefreshToken);
    public sealed record SessionResponse(
        string AccessToken, DateTimeOffset AccessTokenExpiresAt, string RefreshToken, DateTimeOffset RefreshTokenExpiresAt, UserResponse User);

    [HttpPost("login")]
    [AllowAnonymous]
    [EnableRateLimiting(Policies.AuthLimiter)]
    [ProducesResponseType(typeof(SessionResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status401Unauthorized)]
    public async Task<IActionResult> Login([FromBody] LoginRequest request, CancellationToken cancellationToken)
    {
        var (tokens, user) = await auth.LoginAsync(request.Email, request.Password, cancellationToken);
        return Session(tokens, user);
    }

    /// <summary>Troca o refresh token por um NOVO par de tokens e revoga o usado. Reapresentar um token já usado revoga a sessão inteira.</summary>
    [HttpPost("refresh")]
    [AllowAnonymous]
    [EnableRateLimiting(Policies.AuthLimiter)]
    public async Task<IActionResult> Refresh([FromBody] RefreshRequest request, CancellationToken cancellationToken)
    {
        var (tokens, user) = await auth.RefreshAsync(request.RefreshToken, cancellationToken);
        return Session(tokens, user);
    }

    [HttpPost("logout")]
    [AllowAnonymous]
    [EnableRateLimiting(Policies.AuthLimiter)]
    public async Task<IActionResult> Logout([FromBody] RefreshRequest request, CancellationToken cancellationToken)
    {
        await auth.LogoutAsync(request.RefreshToken, cancellationToken);
        return NoContent();
    }

    /// <summary>Quem sou eu, a partir do token (sem consultar o banco).</summary>
    [HttpGet("me")]
    [Authorize(Policy = Policies.Viewer)]
    [EnableRateLimiting(Policies.ApiLimiter)]
    public IActionResult Me() => Ok(new
    {
        id = User.FindFirstValue("sub"),
        email = User.FindFirstValue("email"),
        role = User.FindFirstValue("role")
    });

    private OkObjectResult Session(TokenPair tokens, User user)
    {
        Response.Headers.CacheControl = "no-store"; // respostas com token nunca devem ficar em cache de navegador/proxy
        return Ok(new SessionResponse(tokens.AccessToken, tokens.AccessTokenExpiresAt, tokens.RefreshToken, tokens.RefreshTokenExpiresAt, UserResponse.From(user)));
    }
}

/// <summary>Dispositivos e suas chaves de API. A chave em texto puro aparece UMA vez (criação/rotação) e não pode ser recuperada depois.</summary>
[ApiController]
[Route("api/devices")]
[Authorize(Policy = Policies.Admin)]
[EnableRateLimiting(Policies.ApiLimiter)]
public sealed class DevicesController(DeviceService devices) : ControllerBase
{
    /// <param name="Id">Opcional: preserva a identidade de um dispositivo já existente (importação, simulador).</param>
    public sealed record CreateDeviceRequest(string? Name, Guid? Id = null);
    public sealed record SetActiveRequest(bool Active);
    public sealed record DeviceWithKeyResponse(DeviceResponse Device, string ApiKey, string Notice);

    private const string ShowOnceNotice = "Guarde esta chave agora: ela não poderá ser exibida novamente (apenas rotacionada).";

    [HttpPost]
    [ProducesResponseType(typeof(DeviceWithKeyResponse), StatusCodes.Status201Created)]
    public async Task<IActionResult> Create([FromBody] CreateDeviceRequest request, CancellationToken cancellationToken)
    {
        var (device, key) = await devices.CreateAsync(request.Name ?? "", cancellationToken, request.Id);
        Response.Headers.CacheControl = "no-store";
        return CreatedAtAction(nameof(GetById), new { id = device.Id }, new DeviceWithKeyResponse(DeviceResponse.From(device), key, ShowOnceNotice));
    }

    [HttpGet]
    public async Task<IActionResult> List(CancellationToken cancellationToken) =>
        Ok((await devices.ListAsync(cancellationToken)).Select(DeviceResponse.From).ToList());

    [HttpGet("{id:guid}")]
    public async Task<IActionResult> GetById(Guid id, CancellationToken cancellationToken) =>
        Ok(DeviceResponse.From(await devices.GetAsync(id, cancellationToken)));

    /// <summary>Gera uma chave nova e invalida a anterior (vale em até <c>Security:DeviceCacheSeconds</c> em cada instância da API).</summary>
    [HttpPost("{id:guid}/rotate-key")]
    public async Task<IActionResult> RotateKey(Guid id, CancellationToken cancellationToken)
    {
        var (device, key) = await devices.RotateKeyAsync(id, cancellationToken);
        Response.Headers.CacheControl = "no-store";
        return Ok(new DeviceWithKeyResponse(DeviceResponse.From(device), key, ShowOnceNotice));
    }

    [HttpPatch("{id:guid}/active")]
    public async Task<IActionResult> SetActive(Guid id, [FromBody] SetActiveRequest request, CancellationToken cancellationToken) =>
        Ok(DeviceResponse.From(await devices.SetActiveAsync(id, request.Active, cancellationToken)));
}

[ApiController]
[Route("api/users")]
[Authorize(Policy = Policies.Admin)]
[EnableRateLimiting(Policies.ApiLimiter)]
public sealed class UsersController(UserService users) : ControllerBase
{
    public sealed record CreateUserRequest(string? Email, string? Password, UserRole Role);
    public sealed record ChangeRoleRequest(UserRole Role);
    public sealed record SetActiveRequest(bool Active);

    [HttpPost]
    [ProducesResponseType(typeof(UserResponse), StatusCodes.Status201Created)]
    public async Task<IActionResult> Create([FromBody] CreateUserRequest request, CancellationToken cancellationToken)
    {
        var user = await users.CreateAsync(request.Email ?? "", request.Password ?? "", request.Role, cancellationToken);
        return CreatedAtAction(nameof(List), null, UserResponse.From(user));
    }

    [HttpGet]
    public async Task<IActionResult> List(CancellationToken cancellationToken) =>
        Ok((await users.ListAsync(cancellationToken)).Select(UserResponse.From).ToList());

    [HttpPatch("{id:guid}/role")]
    public async Task<IActionResult> ChangeRole(Guid id, [FromBody] ChangeRoleRequest request, CancellationToken cancellationToken) =>
        Ok(UserResponse.From(await users.ChangeRoleAsync(id, request.Role, cancellationToken)));

    [HttpPatch("{id:guid}/active")]
    public async Task<IActionResult> SetActive(Guid id, [FromBody] SetActiveRequest request, CancellationToken cancellationToken) =>
        Ok(UserResponse.From(await users.SetActiveAsync(id, request.Active, cancellationToken)));
}
