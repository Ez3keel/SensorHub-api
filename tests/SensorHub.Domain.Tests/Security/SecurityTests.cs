using SensorHub.Domain.Common;
using SensorHub.Domain.Security;

namespace SensorHub.Domain.Tests.Security;

public class ApiKeyTests
{
    [Fact]
    public void Generated_keys_are_prefixed_long_and_unique()
    {
        var keys = Enumerable.Range(0, 200).Select(_ => ApiKey.Generate()).ToList();

        Assert.All(keys, k => Assert.StartsWith("shk_", k));
        Assert.All(keys, k => Assert.Equal(4 + 43, k.Length)); // prefixo + 32 bytes em base64url
        Assert.Equal(200, keys.Distinct().Count());
        Assert.All(keys, k => Assert.True(ApiKey.LooksValid(k)));
        Assert.DoesNotContain(keys, k => k.Contains('+') || k.Contains('/') || k.Contains('='));
    }

    [Fact]
    public void Hash_is_deterministic_64_hex_chars_and_never_contains_the_key()
    {
        var key = ApiKey.Generate();

        var hash = ApiKey.Hash(key);

        Assert.Equal(hash, ApiKey.Hash(key));
        Assert.Equal(64, hash.Length);
        Assert.Matches("^[0-9a-f]{64}$", hash);
        Assert.DoesNotContain(key[4..], hash);
        Assert.NotEqual(hash, ApiKey.Hash(key + "x"));
    }

    [Fact]
    public void Hint_reveals_only_a_short_prefix()
    {
        var key = ApiKey.Generate();

        var hint = ApiKey.Hint(key);

        Assert.StartsWith(key[..12], hint);
        Assert.True(hint.Length < 16);
        Assert.DoesNotContain(key[12..], hint);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("abc")]
    [InlineData("xyz_0123456789012345678901234567890")]      // prefixo errado
    [InlineData("shk_curta")]
    public void Obviously_malformed_keys_are_rejected_before_touching_the_database(string? key)
    {
        Assert.False(ApiKey.LooksValid(key));
    }
}

public class DeviceTests
{
    private static readonly DateTimeOffset Now = new(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Create_returns_the_plain_key_once_and_stores_only_its_hash()
    {
        var (device, key) = Device.Create("  Gateway 1  ", Now);

        Assert.Equal("Gateway 1", device.Name);
        Assert.Equal(ApiKey.Hash(key), device.ApiKeyHash);
        Assert.DoesNotContain(key, device.ApiKeyHash);
        Assert.StartsWith(key[..12], device.ApiKeyHint);
        Assert.True(device.Active);
    }

    [Fact]
    public void Rotate_replaces_the_key_and_invalidates_the_previous_one()
    {
        var (device, oldKey) = Device.Create("g", Now);
        var oldHash = device.ApiKeyHash;

        var newKey = device.RotateKey(Now.AddDays(1));

        Assert.NotEqual(oldKey, newKey);
        Assert.NotEqual(oldHash, device.ApiKeyHash);
        Assert.Equal(ApiKey.Hash(newKey), device.ApiKeyHash);
        Assert.NotEqual(ApiKey.Hash(oldKey), device.ApiKeyHash); // a chave antiga deixa de identificar o dispositivo
        Assert.Equal(Now.AddDays(1), device.KeyRotatedAt);
    }

    [Fact]
    public void Create_honors_an_explicit_id_and_validates_the_name()
    {
        var id = Guid.NewGuid();
        Assert.Equal(id, Device.Create("g", Now, id).Device.Id);
        Assert.Throws<DomainException>(() => Device.Create(" ", Now));
        Assert.Throws<DomainException>(() => Device.Create(new string('x', 101), Now));
    }

    [Fact]
    public void Deactivate_and_activate_toggle()
    {
        var (device, _) = Device.Create("g", Now);
        device.Deactivate();
        Assert.False(device.Active);
        device.Activate();
        Assert.True(device.Active);
    }
}

public class UserTests
{
    private static readonly DateTimeOffset Now = new(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Email_is_normalized_so_login_is_case_insensitive()
    {
        var user = User.Create("  Ana@Empresa.COM ", "hash", UserRole.Operator, Now);

        Assert.Equal("ana@empresa.com", user.Email);
        Assert.Equal(UserRole.Operator, user.Role);
        Assert.True(user.Active);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("sem-arroba")]
    [InlineData("@x.com")]
    [InlineData("x@")]
    public void Invalid_emails_are_rejected(string email)
    {
        Assert.Throws<DomainException>(() => User.Create(email, "hash", UserRole.Viewer, Now));
    }

    [Fact]
    public void Role_and_hash_are_validated_and_changeable()
    {
        Assert.Throws<DomainException>(() => User.Create("a@b.c", "", UserRole.Viewer, Now));
        Assert.Throws<DomainException>(() => User.Create("a@b.c", "h", (UserRole)99, Now));

        var user = User.Create("a@b.c", "h1", UserRole.Viewer, Now);
        user.ChangeRole(UserRole.Admin);
        user.ChangePasswordHash("h2");
        Assert.Equal(UserRole.Admin, user.Role);
        Assert.Equal("h2", user.PasswordHash);
        Assert.Throws<DomainException>(() => user.ChangePasswordHash(" "));
    }
}

public class RefreshTokenTests
{
    private static readonly DateTimeOffset Now = new(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Issued_token_is_opaque_random_and_only_the_hash_is_kept()
    {
        var (a, plainA) = RefreshToken.Issue(Guid.NewGuid(), Guid.NewGuid(), TimeSpan.FromDays(7), Now);
        var (_, plainB) = RefreshToken.Issue(Guid.NewGuid(), Guid.NewGuid(), TimeSpan.FromDays(7), Now);

        Assert.NotEqual(plainA, plainB);
        Assert.Equal(RefreshToken.Hash(plainA), a.TokenHash);
        Assert.DoesNotContain(plainA, a.TokenHash);
        Assert.Equal(Now.AddDays(7), a.ExpiresAt);
    }

    [Fact]
    public void Expiry_is_evaluated_against_the_given_clock()
    {
        var (token, _) = RefreshToken.Issue(Guid.NewGuid(), Guid.NewGuid(), TimeSpan.FromHours(1), Now);

        Assert.False(token.IsExpired(Now.AddMinutes(59)));
        Assert.True(token.IsExpired(Now.AddHours(1)));
    }

    [Fact]
    public void Revoke_marks_rotation_and_is_idempotent()
    {
        var (token, _) = RefreshToken.Issue(Guid.NewGuid(), Guid.NewGuid(), TimeSpan.FromDays(1), Now);
        var successor = Guid.NewGuid();

        token.Revoke(Now.AddMinutes(1), successor);
        token.Revoke(Now.AddMinutes(9), Guid.NewGuid()); // segunda chamada não sobrescreve

        Assert.True(token.IsRevoked);
        Assert.True(token.WasRotated);
        Assert.Equal(Now.AddMinutes(1), token.RevokedAt);
        Assert.Equal(successor, token.ReplacedById);
    }

    [Fact]
    public void Revoking_without_a_successor_is_a_plain_revocation_not_a_rotation()
    {
        var (token, _) = RefreshToken.Issue(Guid.NewGuid(), Guid.NewGuid(), TimeSpan.FromDays(1), Now);

        token.Revoke(Now);

        Assert.True(token.IsRevoked);
        Assert.False(token.WasRotated); // logout/revogação em família não conta como "token usado"
    }
}
