using Microsoft.Extensions.Logging;
using SensorHub.Application.Management;
using SensorHub.Domain.Common;
using SensorHub.Domain.Security;
using SensorHub.Domain.Sensors;

namespace SensorHub.Application.Security;

// ============================================================================ exceções

/// <summary>Credencial inválida ou sessão inválida (vira HTTP 401). A mensagem é GENÉRICA de propósito: não revela se o usuário existe.</summary>
public sealed class AuthenticationFailedException(string message = "Credenciais inválidas.") : Exception(message);

// ============================================================================ portas

public enum PasswordVerification
{
    Failed = 0,
    Success = 1,

    /// <summary>Senha correta, mas o hash foi gerado com parâmetros antigos: deve ser regravado com o custo atual.</summary>
    SuccessRehashNeeded = 2
}

public interface IPasswordHasher
{
    string Hash(string password);
    PasswordVerification Verify(string hash, string password);

    /// <summary>
    /// Gasta o MESMO tempo de uma verificação real, sem resultado útil. Chamado quando o e-mail não existe, para que o tempo de
    /// resposta não revele quais e-mails estão cadastrados (enumeração de usuários por timing).
    /// </summary>
    void SimulateVerify(string password);
}

public sealed record AccessToken(string Token, DateTimeOffset ExpiresAt);

public interface IAccessTokenIssuer
{
    AccessToken Issue(User user, DateTimeOffset now);
}

public interface IUserRepository
{
    Task<User?> GetByEmailAsync(string normalizedEmail, CancellationToken cancellationToken);
    Task<User?> GetAsync(Guid id, CancellationToken cancellationToken);
    Task<bool> AnyAsync(CancellationToken cancellationToken);
    Task AddAsync(User user, CancellationToken cancellationToken);
    Task<IReadOnlyList<User>> ListAsync(CancellationToken cancellationToken);
}

public interface IRefreshTokenRepository
{
    Task AddAsync(RefreshToken token, CancellationToken cancellationToken);
    Task<RefreshToken?> FindByHashAsync(string tokenHash, CancellationToken cancellationToken);

    /// <summary>Revoga todos os tokens ainda válidos de uma família (a sessão inteira).</summary>
    Task RevokeFamilyAsync(Guid familyId, DateTimeOffset now, CancellationToken cancellationToken);
}

public interface IDeviceRepository
{
    Task AddAsync(Device device, CancellationToken cancellationToken);
    Task<Device?> GetAsync(Guid id, CancellationToken cancellationToken);
    Task<IReadOnlyList<Device>> ListAsync(CancellationToken cancellationToken);
}

/// <summary>Um dispositivo autenticado pela chave de API.</summary>
public sealed record DeviceIdentity(Guid Id, string Name);

public interface IDeviceAuthenticator
{
    /// <summary>Autentica pela chave. Retorna null se a chave é desconhecida, foi rotacionada ou o dispositivo está desativado.</summary>
    Task<DeviceIdentity?> AuthenticateAsync(string apiKey, CancellationToken cancellationToken);
}

/// <summary>O que a ingestão precisa saber de um sensor para validar uma leitura, sem tocar o banco a cada leitura.</summary>
public sealed record SensorInfo(Guid Id, Guid DeviceId, MetricType Metric, string Unit, bool Active);

public interface ISensorRegistry
{
    Task<SensorInfo?> GetAsync(Guid sensorId, CancellationToken cancellationToken);
}

// ============================================================================ política de senha

public static class PasswordPolicy
{
    public const int MinLength = 10;
    public const int MaxLength = 128; // evita usar o hash como vetor de DoS (PBKDF2 sobre megabytes de entrada)

    private static readonly HashSet<string> Common = new(StringComparer.OrdinalIgnoreCase)
    {
        "password12", "1234567890", "qwerty1234", "admin12345", "sensorhub1", "senha12345", "changeme123"
    };

    public static void Validate(string? password)
    {
        if (string.IsNullOrEmpty(password) || password.Length < MinLength)
            throw new DomainException($"A senha deve ter pelo menos {MinLength} caracteres.");
        if (password.Length > MaxLength)
            throw new DomainException($"A senha deve ter no máximo {MaxLength} caracteres.");
        if (!password.Any(char.IsLetter) || !password.Any(char.IsDigit))
            throw new DomainException("A senha deve conter letras e números.");
        if (Common.Contains(password) || password.Distinct().Count() < 4)
            throw new DomainException("Senha fraca demais.");
    }
}

// ============================================================================ autenticação

public sealed class SecurityOptions
{
    public const string SectionName = "Security";

    /// <summary>Liga a autenticação/autorização. Desligada apenas em testes e em quick-start local.</summary>
    public bool Enabled { get; set; } = true;

    public TimeSpan AccessTokenLifetime { get; set; } = TimeSpan.FromMinutes(15);
    public TimeSpan RefreshTokenLifetime { get; set; } = TimeSpan.FromDays(7);

    /// <summary>Segredo de assinatura do JWT (HS256). Em produção vem de variável de ambiente/secret manager, NUNCA do repositório.</summary>
    public string JwtSigningKey { get; set; } = "";
    public string JwtIssuer { get; set; } = "sensorhub";
    public string JwtAudience { get; set; } = "sensorhub-clients";

    /// <summary>Quanto tempo o resultado de "esta chave de API é válida?" pode ficar em memória (janela para uma revogação valer em todas as instâncias).</summary>
    public int DeviceCacheSeconds { get; set; } = 30;
    public int SensorCacheSeconds { get; set; } = 30;

    public BootstrapAdminOptions BootstrapAdmin { get; set; } = new();
}

public sealed class BootstrapAdminOptions
{
    public string Email { get; set; } = "";
    public string Password { get; set; } = "";
}

public sealed record TokenPair(string AccessToken, DateTimeOffset AccessTokenExpiresAt, string RefreshToken, DateTimeOffset RefreshTokenExpiresAt);

/// <summary>
/// Login, refresh e logout. O refresh token é opaco e ROTATIVO: cada uso emite um novo e revoga o anterior. Um token já usado
/// que reaparece indica cópia/roubo, e a sessão inteira (família) é revogada.
/// </summary>
public sealed class AuthService(
    IUserRepository users,
    IRefreshTokenRepository refreshTokens,
    IPasswordHasher hasher,
    IAccessTokenIssuer accessTokens,
    IUnitOfWork unitOfWork,
    Microsoft.Extensions.Options.IOptions<SecurityOptions> options,
    TimeProvider clock,
    ILogger<AuthService> logger)
{
    public async Task<(TokenPair Tokens, User User)> LoginAsync(string? email, string? password, CancellationToken cancellationToken)
    {
        var normalized = TryNormalize(email);
        var user = normalized is null ? null : await users.GetByEmailAsync(normalized, cancellationToken);

        if (user is null || string.IsNullOrEmpty(password))
        {
            hasher.SimulateVerify(password ?? "");
            throw new AuthenticationFailedException();
        }

        var verification = hasher.Verify(user.PasswordHash, password);
        if (verification == PasswordVerification.Failed || !user.Active)
        {
            logger.LogWarning("Login recusado para {User} (senha incorreta ou usuário inativo).", user.Id);
            throw new AuthenticationFailedException();
        }

        if (verification == PasswordVerification.SuccessRehashNeeded)
            user.ChangePasswordHash(hasher.Hash(password)); // atualiza o hash para os parâmetros atuais de forma transparente

        var tokens = await IssueAsync(user, Guid.NewGuid(), cancellationToken);
        return (tokens, user);
    }

    public async Task<(TokenPair Tokens, User User)> RefreshAsync(string? refreshToken, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(refreshToken)) throw new AuthenticationFailedException("Sessão inválida.");

        var now = clock.GetUtcNow();
        var stored = await refreshTokens.FindByHashAsync(RefreshToken.Hash(refreshToken), cancellationToken)
                     ?? throw new AuthenticationFailedException("Sessão inválida.");

        if (stored.IsRevoked)
        {
            if (stored.WasRotated)
            {
                // Um token que JÁ FOI TROCADO por outro está sendo apresentado de novo: quem tem este token não é (só) o dono legítimo.
                logger.LogWarning("REUSO de refresh token detectado (usuário {User}, família {Family}): revogando a sessão inteira.", stored.UserId, stored.FamilyId);
                await refreshTokens.RevokeFamilyAsync(stored.FamilyId, now, cancellationToken);
                await unitOfWork.SaveChangesAsync(cancellationToken);
            }

            throw new AuthenticationFailedException("Sessão inválida.");
        }

        if (stored.IsExpired(now)) throw new AuthenticationFailedException("Sessão expirada.");

        var user = await users.GetAsync(stored.UserId, cancellationToken);
        if (user is null || !user.Active) throw new AuthenticationFailedException("Sessão inválida.");

        var (next, plain) = RefreshToken.Issue(user.Id, stored.FamilyId, options.Value.RefreshTokenLifetime, now);
        stored.Revoke(now, replacedBy: next.Id);
        await refreshTokens.AddAsync(next, cancellationToken);

        var access = accessTokens.Issue(user, now);
        await unitOfWork.SaveChangesAsync(cancellationToken);
        return (new TokenPair(access.Token, access.ExpiresAt, plain, next.ExpiresAt), user);
    }

    /// <summary>Encerra a sessão (revoga a família). Idempotente: token desconhecido também "funciona", para não revelar nada.</summary>
    public async Task LogoutAsync(string? refreshToken, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(refreshToken)) return;

        var stored = await refreshTokens.FindByHashAsync(RefreshToken.Hash(refreshToken), cancellationToken);
        if (stored is null) return;

        await refreshTokens.RevokeFamilyAsync(stored.FamilyId, clock.GetUtcNow(), cancellationToken);
        await unitOfWork.SaveChangesAsync(cancellationToken);
    }

    private async Task<TokenPair> IssueAsync(User user, Guid familyId, CancellationToken cancellationToken)
    {
        var now = clock.GetUtcNow();
        var (token, plain) = RefreshToken.Issue(user.Id, familyId, options.Value.RefreshTokenLifetime, now);
        await refreshTokens.AddAsync(token, cancellationToken);

        var access = accessTokens.Issue(user, now);
        await unitOfWork.SaveChangesAsync(cancellationToken);
        return new TokenPair(access.Token, access.ExpiresAt, plain, token.ExpiresAt);
    }

    private static string? TryNormalize(string? email)
    {
        try { return User.NormalizeEmail(email); }
        catch (DomainException) { return null; }
    }
}

// ============================================================================ usuários e dispositivos

public sealed class UserService(IUserRepository users, IPasswordHasher hasher, IUnitOfWork unitOfWork, TimeProvider clock, ILogger<UserService> logger)
{
    public async Task<User> CreateAsync(string email, string password, UserRole role, CancellationToken cancellationToken)
    {
        PasswordPolicy.Validate(password);
        var normalized = User.NormalizeEmail(email);
        if (await users.GetByEmailAsync(normalized, cancellationToken) is not null)
            throw new ConflictException("Já existe um usuário com este e-mail.");

        var user = User.Create(normalized, hasher.Hash(password), role, clock.GetUtcNow());
        await users.AddAsync(user, cancellationToken);
        await unitOfWork.SaveChangesAsync(cancellationToken);
        return user;
    }

    public Task<IReadOnlyList<User>> ListAsync(CancellationToken cancellationToken) => users.ListAsync(cancellationToken);

    public async Task<User> ChangeRoleAsync(Guid id, UserRole role, CancellationToken cancellationToken)
    {
        var user = await users.GetAsync(id, cancellationToken) ?? throw new NotFoundException("Usuário", id);
        user.ChangeRole(role);
        await unitOfWork.SaveChangesAsync(cancellationToken);
        return user;
    }

    public async Task<User> SetActiveAsync(Guid id, bool active, CancellationToken cancellationToken)
    {
        var user = await users.GetAsync(id, cancellationToken) ?? throw new NotFoundException("Usuário", id);
        if (active) user.Activate(); else user.Deactivate();
        await unitOfWork.SaveChangesAsync(cancellationToken);
        return user;
    }

    /// <summary>
    /// Cria o primeiro administrador quando NÃO existe nenhum usuário (bootstrap de uma instalação nova). Depois disso é no-op:
    /// mudar a configuração não cria nem altera administradores em um sistema já em uso.
    /// </summary>
    public async Task<bool> EnsureBootstrapAdminAsync(string email, string password, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(email) || string.IsNullOrWhiteSpace(password)) return false;
        if (await users.AnyAsync(cancellationToken)) return false;

        await CreateAsync(email, password, UserRole.Admin, cancellationToken);
        logger.LogInformation("Administrador inicial criado: {Email}", User.NormalizeEmail(email));
        return true;
    }
}

public sealed class DeviceService(IDeviceRepository devices, IUnitOfWork unitOfWork, TimeProvider clock)
{
    /// <summary>Cria o dispositivo. A chave em texto puro é devolvida UMA vez e não pode ser recuperada depois (só rotacionada).</summary>
    public async Task<(Device Device, string ApiKey)> CreateAsync(string name, CancellationToken cancellationToken, Guid? id = null)
    {
        if (id is { } wanted && wanted != Guid.Empty && await devices.GetAsync(wanted, cancellationToken) is not null)
            throw new ConflictException($"Já existe um dispositivo com o id '{wanted}'.");

        var (device, key) = Device.Create(name, clock.GetUtcNow(), id);
        await devices.AddAsync(device, cancellationToken);
        await unitOfWork.SaveChangesAsync(cancellationToken);
        return (device, key);
    }

    public Task<IReadOnlyList<Device>> ListAsync(CancellationToken cancellationToken) => devices.ListAsync(cancellationToken);

    public async Task<Device> GetAsync(Guid id, CancellationToken cancellationToken) =>
        await devices.GetAsync(id, cancellationToken) ?? throw new NotFoundException("Dispositivo", id);

    public async Task<(Device Device, string ApiKey)> RotateKeyAsync(Guid id, CancellationToken cancellationToken)
    {
        var device = await GetAsync(id, cancellationToken);
        var key = device.RotateKey(clock.GetUtcNow());
        await unitOfWork.SaveChangesAsync(cancellationToken);
        return (device, key);
    }

    public async Task<Device> SetActiveAsync(Guid id, bool active, CancellationToken cancellationToken)
    {
        var device = await GetAsync(id, cancellationToken);
        if (active) device.Activate(); else device.Deactivate();
        await unitOfWork.SaveChangesAsync(cancellationToken);
        return device;
    }
}
