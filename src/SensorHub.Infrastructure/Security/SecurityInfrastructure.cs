using System.Security.Claims;
using System.Text;
using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;
using SensorHub.Application.Security;
using SensorHub.Domain.Security;
using SensorHub.Domain.Sensors;
using SensorHub.Infrastructure.Persistence;

namespace SensorHub.Infrastructure.Security;

// ============================================================================ mapeamento EF

internal sealed class DeviceConfiguration : IEntityTypeConfiguration<Device>
{
    public void Configure(EntityTypeBuilder<Device> b)
    {
        b.ToTable("devices");
        b.HasKey(d => d.Id);
        b.Property(d => d.Id).HasColumnName("id");
        b.Property(d => d.Name).HasColumnName("name").HasMaxLength(100).IsRequired();
        b.Property(d => d.ApiKeyHash).HasColumnName("api_key_hash").HasMaxLength(64).IsRequired();
        b.Property(d => d.ApiKeyHint).HasColumnName("api_key_hint").HasMaxLength(20).IsRequired();
        b.Property(d => d.Active).HasColumnName("active");
        b.Property(d => d.CreatedAt).HasColumnName("created_at");
        b.Property(d => d.KeyRotatedAt).HasColumnName("key_rotated_at");

        // A autenticação acha o dispositivo PELO HASH da chave: o índice único é o que a torna O(log n) e impede dois dispositivos com a mesma chave.
        b.HasIndex(d => d.ApiKeyHash).IsUnique().HasDatabaseName("ux_devices_api_key_hash");
    }
}

internal sealed class UserConfiguration : IEntityTypeConfiguration<User>
{
    public void Configure(EntityTypeBuilder<User> b)
    {
        b.ToTable("users");
        b.HasKey(u => u.Id);
        b.Property(u => u.Id).HasColumnName("id");
        b.Property(u => u.Email).HasColumnName("email").HasMaxLength(200).IsRequired();
        b.Property(u => u.PasswordHash).HasColumnName("password_hash").HasMaxLength(500).IsRequired();
        b.Property(u => u.Role).HasColumnName("role").HasConversion<string>().HasMaxLength(20);
        b.Property(u => u.Active).HasColumnName("active");
        b.Property(u => u.CreatedAt).HasColumnName("created_at");
        b.HasIndex(u => u.Email).IsUnique().HasDatabaseName("ux_users_email");
    }
}

internal sealed class RefreshTokenConfiguration : IEntityTypeConfiguration<RefreshToken>
{
    public void Configure(EntityTypeBuilder<RefreshToken> b)
    {
        b.ToTable("refresh_tokens");
        b.HasKey(t => t.Id);
        b.Property(t => t.Id).HasColumnName("id");
        b.Property(t => t.UserId).HasColumnName("user_id");
        b.Property(t => t.FamilyId).HasColumnName("family_id");
        b.Property(t => t.TokenHash).HasColumnName("token_hash").HasMaxLength(64).IsRequired();
        b.Property(t => t.CreatedAt).HasColumnName("created_at");
        b.Property(t => t.ExpiresAt).HasColumnName("expires_at");
        b.Property(t => t.RevokedAt).HasColumnName("revoked_at");
        b.Property(t => t.ReplacedById).HasColumnName("replaced_by_id");

        b.HasOne<User>().WithMany().HasForeignKey(t => t.UserId).OnDelete(DeleteBehavior.Cascade);
        b.HasIndex(t => t.TokenHash).IsUnique().HasDatabaseName("ux_refresh_tokens_hash");
        b.HasIndex(t => t.FamilyId).HasDatabaseName("ix_refresh_tokens_family");
    }
}

/// <summary>Todo sensor pertence a um dispositivo cadastrado: a chave estrangeira garante que a "propriedade" checada na ingestão exista.</summary>
internal sealed class SensorDeviceRelation : IEntityTypeConfiguration<Sensor>
{
    public void Configure(EntityTypeBuilder<Sensor> b) =>
        b.HasOne<Device>().WithMany().HasForeignKey(s => s.DeviceId).OnDelete(DeleteBehavior.Restrict);
}

// ============================================================================ repositórios

internal sealed class UserRepository(SensorHubDbContext db) : IUserRepository
{
    public Task<User?> GetByEmailAsync(string normalizedEmail, CancellationToken cancellationToken) =>
        db.Set<User>().FirstOrDefaultAsync(u => u.Email == normalizedEmail, cancellationToken);

    public Task<User?> GetAsync(Guid id, CancellationToken cancellationToken) =>
        db.Set<User>().FirstOrDefaultAsync(u => u.Id == id, cancellationToken);

    public Task<bool> AnyAsync(CancellationToken cancellationToken) => db.Set<User>().AnyAsync(cancellationToken);

    public async Task AddAsync(User user, CancellationToken cancellationToken) => await db.Set<User>().AddAsync(user, cancellationToken);

    public async Task<IReadOnlyList<User>> ListAsync(CancellationToken cancellationToken) =>
        await db.Set<User>().AsNoTracking().OrderBy(u => u.Email).ToListAsync(cancellationToken);
}

internal sealed class RefreshTokenRepository(SensorHubDbContext db) : IRefreshTokenRepository
{
    public async Task AddAsync(RefreshToken token, CancellationToken cancellationToken) =>
        await db.Set<RefreshToken>().AddAsync(token, cancellationToken);

    public Task<RefreshToken?> FindByHashAsync(string tokenHash, CancellationToken cancellationToken) =>
        db.Set<RefreshToken>().FirstOrDefaultAsync(t => t.TokenHash == tokenHash, cancellationToken);

    public Task RevokeFamilyAsync(Guid familyId, DateTimeOffset now, CancellationToken cancellationToken) =>
        // UPDATE direto: revogar uma família não deve depender de carregar (e disputar) cada token na memória
        db.Set<RefreshToken>().Where(t => t.FamilyId == familyId && t.RevokedAt == null)
            .ExecuteUpdateAsync(s => s.SetProperty(t => t.RevokedAt, now), cancellationToken);
}

internal sealed class DeviceRepository(SensorHubDbContext db) : IDeviceRepository
{
    public async Task AddAsync(Device device, CancellationToken cancellationToken) => await db.Set<Device>().AddAsync(device, cancellationToken);

    public Task<Device?> GetAsync(Guid id, CancellationToken cancellationToken) =>
        db.Set<Device>().FirstOrDefaultAsync(d => d.Id == id, cancellationToken);

    public async Task<IReadOnlyList<Device>> ListAsync(CancellationToken cancellationToken) =>
        await db.Set<Device>().AsNoTracking().OrderBy(d => d.Name).ToListAsync(cancellationToken);
}

// ============================================================================ senha e JWT

/// <summary>
/// Hash de senha do ASP.NET Core Identity: PBKDF2-HMAC-SHA512 com sal aleatório por senha e contagem de iterações embutida no próprio
/// hash (por isso dá para detectar hashes antigos e regravá-los quando o custo sobe, sem forçar ninguém a trocar de senha).
/// </summary>
public sealed class IdentityPasswordHasher : IPasswordHasher
{
    private static readonly User Nobody = User.Create("nobody@sensorhub.invalid", "x", UserRole.Viewer, DateTimeOffset.UnixEpoch);
    private readonly PasswordHasher<User> _hasher = new();
    private readonly Lazy<string> _decoy;

    public IdentityPasswordHasher() => _decoy = new Lazy<string>(() => _hasher.HashPassword(Nobody, Guid.NewGuid().ToString("N")));

    public string Hash(string password) => _hasher.HashPassword(Nobody, password);

    public PasswordVerification Verify(string hash, string password) =>
        _hasher.VerifyHashedPassword(Nobody, hash, password) switch
        {
            PasswordVerificationResult.Success => PasswordVerification.Success,
            PasswordVerificationResult.SuccessRehashNeeded => PasswordVerification.SuccessRehashNeeded,
            _ => PasswordVerification.Failed
        };

    public void SimulateVerify(string password) => _hasher.VerifyHashedPassword(Nobody, _decoy.Value, password);
}

public sealed class JwtAccessTokenIssuer(IOptions<SecurityOptions> options) : IAccessTokenIssuer
{
    private readonly JsonWebTokenHandler _handler = new();

    public AccessToken Issue(User user, DateTimeOffset now)
    {
        var settings = options.Value;
        var expires = now + settings.AccessTokenLifetime;

        var descriptor = new SecurityTokenDescriptor
        {
            Issuer = settings.JwtIssuer,
            Audience = settings.JwtAudience,
            IssuedAt = now.UtcDateTime,
            NotBefore = now.UtcDateTime,
            Expires = expires.UtcDateTime,
            Subject = new ClaimsIdentity(
            [
                new Claim(JwtRegisteredClaimNames.Sub, user.Id.ToString()),
                new Claim(JwtRegisteredClaimNames.Email, user.Email),
                new Claim("role", user.Role.ToString()),
                new Claim(JwtRegisteredClaimNames.Jti, Guid.NewGuid().ToString("N"))
            ]),
            SigningCredentials = new SigningCredentials(SigningKey(settings), SecurityAlgorithms.HmacSha256)
        };

        return new AccessToken(_handler.CreateToken(descriptor), expires);
    }

    public static SymmetricSecurityKey SigningKey(SecurityOptions settings)
    {
        // HS256 exige chave de pelo menos 256 bits; uma chave curta enfraquece a assinatura e é rejeitada logo na subida.
        if (string.IsNullOrWhiteSpace(settings.JwtSigningKey) || Encoding.UTF8.GetByteCount(settings.JwtSigningKey) < 32)
            throw new InvalidOperationException("Security:JwtSigningKey deve ter pelo menos 32 bytes (defina por variável de ambiente, nunca no repositório).");
        return new SymmetricSecurityKey(Encoding.UTF8.GetBytes(settings.JwtSigningKey));
    }
}

// ============================================================================ autenticação de dispositivo e registro de sensores

/// <summary>
/// Autentica a chave de API pelo HASH. O resultado fica em memória por <c>DeviceCacheSeconds</c> (a ingestão é o caminho mais quente
/// do sistema: uma consulta ao banco por requisição seria o gargalo). Preço assumido: revogar ou rotacionar uma chave leva até
/// esse tempo para valer em cada instância. Chaves DESCONHECIDAS também ficam em cache por poucos segundos, para que um atacante
/// tentando chaves aleatórias não transforme cada tentativa numa consulta ao banco.
/// </summary>
public sealed class CachedDeviceAuthenticator(
    IDbContextFactory<SensorHubDbContext> contexts, IMemoryCache cache, IOptions<SecurityOptions> options) : IDeviceAuthenticator
{
    private static readonly TimeSpan NegativeTtl = TimeSpan.FromSeconds(5);

    public async Task<DeviceIdentity?> AuthenticateAsync(string apiKey, CancellationToken cancellationToken)
    {
        if (!ApiKey.LooksValid(apiKey)) return null; // lixo óbvio nem chega ao cache nem ao banco

        var hash = ApiKey.Hash(apiKey);
        var cacheKey = $"device:{hash}";
        if (cache.TryGetValue(cacheKey, out DeviceIdentity? cached)) return cached;

        await using var db = await contexts.CreateDbContextAsync(cancellationToken);
        var device = await db.Set<Device>().AsNoTracking().FirstOrDefaultAsync(d => d.ApiKeyHash == hash, cancellationToken);

        var identity = device is { Active: true } ? new DeviceIdentity(device.Id, device.Name) : null;
        var ttl = identity is null ? NegativeTtl : TimeSpan.FromSeconds(options.Value.DeviceCacheSeconds);
        cache.Set(cacheKey, identity, ttl);
        return identity;
    }
}

/// <summary>Cache-aside por sensor: leitura por chave primária no banco na primeira vez, memória depois. Sensor inexistente também é cacheado por pouco tempo.</summary>
public sealed class CachedSensorRegistry(
    IDbContextFactory<SensorHubDbContext> contexts, IMemoryCache cache, IOptions<SecurityOptions> options) : ISensorRegistry
{
    private static readonly TimeSpan NegativeTtl = TimeSpan.FromSeconds(5);

    public async Task<SensorInfo?> GetAsync(Guid sensorId, CancellationToken cancellationToken)
    {
        var cacheKey = $"sensor:{sensorId:N}";
        if (cache.TryGetValue(cacheKey, out SensorInfo? cached)) return cached;

        await using var db = await contexts.CreateDbContextAsync(cancellationToken);
        var sensor = await db.Set<Sensor>().AsNoTracking().FirstOrDefaultAsync(s => s.Id == sensorId, cancellationToken);

        var info = sensor is null ? null : new SensorInfo(sensor.Id, sensor.DeviceId, sensor.Metric, sensor.Unit, sensor.Active);
        cache.Set(cacheKey, info, info is null ? NegativeTtl : TimeSpan.FromSeconds(options.Value.SensorCacheSeconds));
        return info;
    }
}
