using System.Security.Cryptography;
using System.Text;
using SensorHub.Domain.Common;

namespace SensorHub.Domain.Security;

public enum UserRole
{
    /// <summary>Somente leitura: o dashboard.</summary>
    Viewer = 1,

    /// <summary>Leitura + reconhecer alertas.</summary>
    Operator = 2,

    /// <summary>Tudo: cadastro de sensores, regras, dispositivos e usuários.</summary>
    Admin = 3
}

/// <summary>
/// Chave de API de um dispositivo. O texto puro só existe UMA vez, no momento da criação (e da rotação): o que se guarda é o hash
/// SHA-256. Como a chave tem 256 bits de entropia aleatória, um hash rápido é adequado (PBKDF2/bcrypt existem para SENHAS, que têm
/// pouca entropia e precisam de custo para resistir a força bruta). Isso permite achar o dispositivo por índice no hash, sem varrer.
/// </summary>
public static class ApiKey
{
    public const string Prefix = "shk_";

    /// <summary>Gera uma chave nova: <c>shk_</c> + 32 bytes aleatórios em base64url (43 caracteres).</summary>
    public static string Generate() => Prefix + Base64Url(RandomNumberGenerator.GetBytes(32));

    /// <summary>Hash em hexadecimal minúsculo (64 caracteres), o que vai para o banco.</summary>
    public static string Hash(string key) => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(key)));

    /// <summary>Trecho inicial não secreto, para o operador identificar de qual chave se fala (nunca basta para autenticar).</summary>
    public static string Hint(string key) => key.Length <= 12 ? key : key[..12] + "…";

    public static bool LooksValid(string? key) =>
        key is { Length: >= 20 and <= 200 } && key.StartsWith(Prefix, StringComparison.Ordinal);

    private static string Base64Url(byte[] bytes) =>
        Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');
}

/// <summary>
/// Dispositivo (ou gateway) que envia leituras. É a identidade da INGESTÃO: cada dispositivo tem a sua chave, revogável
/// individualmente, e só pode enviar leituras dos sensores que lhe pertencem.
/// </summary>
public sealed class Device
{
    public Guid Id { get; private set; }
    public string Name { get; private set; } = null!;
    public string ApiKeyHash { get; private set; } = null!;
    public string ApiKeyHint { get; private set; } = null!;
    public bool Active { get; private set; }
    public DateTimeOffset CreatedAt { get; private set; }
    public DateTimeOffset? KeyRotatedAt { get; private set; }

    private Device() { } // EF

    /// <summary>Cria o dispositivo e devolve a chave em TEXTO PURO, que o chamador deve mostrar uma única vez.</summary>
    public static (Device Device, string ApiKey) Create(string name, DateTimeOffset now, Guid? id = null)
    {
        if (string.IsNullOrWhiteSpace(name)) throw new DomainException("Nome do dispositivo é obrigatório.");
        if (name.Trim().Length > 100) throw new DomainException("Nome do dispositivo excede 100 caracteres.");

        var key = ApiKey.Generate();
        var device = new Device
        {
            Id = id is { } given && given != Guid.Empty ? given : Guid.NewGuid(),
            Name = name.Trim(),
            ApiKeyHash = ApiKey.Hash(key),
            ApiKeyHint = ApiKey.Hint(key),
            Active = true,
            CreatedAt = now
        };
        return (device, key);
    }

    /// <summary>Gera uma chave nova e invalida a anterior imediatamente. Devolve a nova em texto puro.</summary>
    public string RotateKey(DateTimeOffset now)
    {
        var key = ApiKey.Generate();
        ApiKeyHash = ApiKey.Hash(key);
        ApiKeyHint = ApiKey.Hint(key);
        KeyRotatedAt = now;
        return key;
    }

    public void Deactivate() => Active = false;

    public void Activate() => Active = true;
}

public sealed class User
{
    public Guid Id { get; private set; }
    public string Email { get; private set; } = null!;
    public string PasswordHash { get; private set; } = null!;
    public UserRole Role { get; private set; }
    public bool Active { get; private set; }
    public DateTimeOffset CreatedAt { get; private set; }

    private User() { } // EF

    public static User Create(string email, string passwordHash, UserRole role, DateTimeOffset now)
    {
        var normalized = NormalizeEmail(email);
        if (string.IsNullOrWhiteSpace(passwordHash)) throw new DomainException("Hash de senha é obrigatório.");
        if (!Enum.IsDefined(role)) throw new DomainException("Papel inválido.");

        return new User { Id = Guid.NewGuid(), Email = normalized, PasswordHash = passwordHash, Role = role, Active = true, CreatedAt = now };
    }

    /// <summary>E-mail é o login: normalizado (minúsculo, sem espaços) para que "Ana@x.com" e "ana@x.com" sejam a mesma pessoa.</summary>
    public static string NormalizeEmail(string? email)
    {
        var value = email?.Trim().ToLowerInvariant();
        if (string.IsNullOrEmpty(value) || value.Length > 200 || !value.Contains('@') || value.StartsWith('@') || value.EndsWith('@'))
            throw new DomainException("E-mail inválido.");
        return value;
    }

    public void ChangePasswordHash(string passwordHash)
    {
        if (string.IsNullOrWhiteSpace(passwordHash)) throw new DomainException("Hash de senha é obrigatório.");
        PasswordHash = passwordHash;
    }

    public void ChangeRole(UserRole role)
    {
        if (!Enum.IsDefined(role)) throw new DomainException("Papel inválido.");
        Role = role;
    }

    public void Deactivate() => Active = false;

    public void Activate() => Active = true;
}

/// <summary>
/// Refresh token OPACO (aleatório, sem significado) com rotação: cada uso emite um novo e revoga o anterior. Tokens de uma mesma
/// "família" (a sessão de login) são rastreados: se um token JÁ USADO for apresentado de novo, alguém o copiou, e a família inteira é
/// revogada (detecção de reuso). Só o hash é guardado.
/// </summary>
public sealed class RefreshToken
{
    public Guid Id { get; private set; }
    public Guid UserId { get; private set; }
    public Guid FamilyId { get; private set; }
    public string TokenHash { get; private set; } = null!;
    public DateTimeOffset CreatedAt { get; private set; }
    public DateTimeOffset ExpiresAt { get; private set; }
    public DateTimeOffset? RevokedAt { get; private set; }
    public Guid? ReplacedById { get; private set; }

    private RefreshToken() { } // EF

    public static (RefreshToken Token, string Plain) Issue(Guid userId, Guid familyId, TimeSpan lifetime, DateTimeOffset now)
    {
        var plain = Convert.ToBase64String(RandomNumberGenerator.GetBytes(48)).TrimEnd('=').Replace('+', '-').Replace('/', '_');
        var token = new RefreshToken
        {
            Id = Guid.NewGuid(), UserId = userId, FamilyId = familyId, TokenHash = Hash(plain),
            CreatedAt = now, ExpiresAt = now + lifetime
        };
        return (token, plain);
    }

    public static string Hash(string plain) => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(plain)));

    public bool IsExpired(DateTimeOffset now) => now >= ExpiresAt;

    public bool IsRevoked => RevokedAt is not null;

    /// <summary>Usado e substituído: apresentá-lo de novo é o sinal de reuso.</summary>
    public bool WasRotated => ReplacedById is not null;

    public void Revoke(DateTimeOffset now, Guid? replacedBy = null)
    {
        RevokedAt ??= now;
        ReplacedById ??= replacedBy;
    }
}
