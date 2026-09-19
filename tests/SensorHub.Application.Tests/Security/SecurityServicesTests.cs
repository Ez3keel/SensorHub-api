using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using SensorHub.Application.Contracts;
using SensorHub.Application.Ingestion;
using SensorHub.Application.Management;
using SensorHub.Application.Security;
using SensorHub.Domain.Common;
using SensorHub.Domain.Security;
using SensorHub.Domain.Sensors;

namespace SensorHub.Application.Tests.Security;

/// <summary>Repositórios em memória compartilhados pelos testes de segurança.</summary>
internal sealed class SecurityFakes
{
    public List<User> Users { get; } = [];
    public List<RefreshToken> Tokens { get; } = [];
    public List<Device> Devices { get; } = [];
    public int SaveCalls { get; set; }

    public IUserRepository UserStore => new UserRepo(this);
    public IRefreshTokenRepository TokenStore => new TokenRepo(this);
    public IDeviceRepository DeviceStore => new DeviceRepo(this);
    public IUnitOfWork Uow => new FakeUow(this);

    private sealed class FakeUow(SecurityFakes f) : IUnitOfWork
    {
        public Task<int> SaveChangesAsync(CancellationToken ct) { f.SaveCalls++; return Task.FromResult(1); }
    }

    private sealed class UserRepo(SecurityFakes f) : IUserRepository
    {
        public Task<User?> GetByEmailAsync(string e, CancellationToken ct) => Task.FromResult(f.Users.FirstOrDefault(u => u.Email == e));
        public Task<User?> GetAsync(Guid id, CancellationToken ct) => Task.FromResult(f.Users.FirstOrDefault(u => u.Id == id));
        public Task<bool> AnyAsync(CancellationToken ct) => Task.FromResult(f.Users.Count > 0);
        public Task AddAsync(User u, CancellationToken ct) { f.Users.Add(u); return Task.CompletedTask; }
        public Task<IReadOnlyList<User>> ListAsync(CancellationToken ct) => Task.FromResult<IReadOnlyList<User>>(f.Users.ToList());
    }

    private sealed class TokenRepo(SecurityFakes f) : IRefreshTokenRepository
    {
        public Task AddAsync(RefreshToken t, CancellationToken ct) { f.Tokens.Add(t); return Task.CompletedTask; }
        public Task<RefreshToken?> FindByHashAsync(string h, CancellationToken ct) => Task.FromResult(f.Tokens.FirstOrDefault(t => t.TokenHash == h));
        public Task RevokeFamilyAsync(Guid family, DateTimeOffset now, CancellationToken ct)
        {
            foreach (var t in f.Tokens.Where(t => t.FamilyId == family)) t.Revoke(now);
            return Task.CompletedTask;
        }
    }

    private sealed class DeviceRepo(SecurityFakes f) : IDeviceRepository
    {
        public Task AddAsync(Device d, CancellationToken ct) { f.Devices.Add(d); return Task.CompletedTask; }
        public Task<Device?> GetAsync(Guid id, CancellationToken ct) => Task.FromResult(f.Devices.FirstOrDefault(d => d.Id == id));
        public Task<IReadOnlyList<Device>> ListAsync(CancellationToken ct) => Task.FromResult<IReadOnlyList<Device>>(f.Devices.ToList());
    }
}

/// <summary>Hasher de teste: "hash:" + senha. Registra quando SimulateVerify é chamado (defesa contra enumeração por timing).</summary>
internal sealed class FakeHasher : IPasswordHasher
{
    public int SimulatedVerifies { get; private set; }
    public bool ReportRehashNeeded { get; set; }

    public string Hash(string password) => "hash:" + password;

    public PasswordVerification Verify(string hash, string password) =>
        hash == "hash:" + password
            ? (ReportRehashNeeded ? PasswordVerification.SuccessRehashNeeded : PasswordVerification.Success)
            : PasswordVerification.Failed;

    public void SimulateVerify(string password) => SimulatedVerifies++;
}

internal sealed class FakeIssuer : IAccessTokenIssuer
{
    public AccessToken Issue(User user, DateTimeOffset now) => new($"jwt-for-{user.Email}-{user.Role}", now.AddMinutes(15));
}

public class AuthServiceTests
{
    private static readonly DateTimeOffset T0 = new(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly SecurityFakes _db = new();
    private readonly FakeHasher _hasher = new();
    private readonly FakeTimeProvider _clock = new(T0);
    private readonly AuthService _auth;

    public AuthServiceTests()
    {
        _db.Users.Add(User.Create("ana@empresa.com", "hash:Senha-forte-123", UserRole.Operator, T0));
        _auth = new AuthService(_db.UserStore, _db.TokenStore, _hasher, new FakeIssuer(), _db.Uow,
            Options.Create(new SecurityOptions()), _clock, NullLogger<AuthService>.Instance);
    }

    [Fact]
    public async Task Login_issues_an_access_token_and_a_refresh_token_whose_plain_text_is_never_stored()
    {
        var (tokens, user) = await _auth.LoginAsync("  ANA@empresa.com ", "Senha-forte-123", default);

        Assert.Equal("jwt-for-ana@empresa.com-Operator", tokens.AccessToken);
        Assert.Equal(T0.AddMinutes(15), tokens.AccessTokenExpiresAt);
        Assert.Equal(T0.AddDays(7), tokens.RefreshTokenExpiresAt);
        var stored = Assert.Single(_db.Tokens);
        Assert.Equal(RefreshToken.Hash(tokens.RefreshToken), stored.TokenHash);
        Assert.DoesNotContain(tokens.RefreshToken, stored.TokenHash);
        Assert.Equal(user.Id, stored.UserId);
    }

    [Theory]
    [InlineData("ana@empresa.com", "senha-errada-999")]
    [InlineData("ninguem@empresa.com", "Senha-forte-123")]
    [InlineData("nao-e-email", "Senha-forte-123")]
    [InlineData("", "")]
    [InlineData(null, null)]
    public async Task Wrong_credentials_all_fail_with_the_same_generic_message(string? email, string? password)
    {
        var ex = await Assert.ThrowsAsync<AuthenticationFailedException>(() => _auth.LoginAsync(email, password, default));

        Assert.Equal("Credenciais inválidas.", ex.Message); // não revela se foi o e-mail ou a senha
        Assert.Empty(_db.Tokens);
    }

    [Fact]
    public async Task An_unknown_email_still_spends_the_time_of_a_real_verification()
    {
        await Assert.ThrowsAsync<AuthenticationFailedException>(() => _auth.LoginAsync("ninguem@empresa.com", "qualquer-senha-1", default));

        Assert.Equal(1, _hasher.SimulatedVerifies); // anti-enumeração por timing: o tempo é o mesmo de "senha errada"
    }

    [Fact]
    public async Task An_inactive_user_cannot_log_in_even_with_the_right_password()
    {
        _db.Users[0].Deactivate();

        await Assert.ThrowsAsync<AuthenticationFailedException>(() => _auth.LoginAsync("ana@empresa.com", "Senha-forte-123", default));
    }

    [Fact]
    public async Task An_outdated_password_hash_is_upgraded_transparently_on_login()
    {
        _hasher.ReportRehashNeeded = true;

        await _auth.LoginAsync("ana@empresa.com", "Senha-forte-123", default);

        Assert.Equal("hash:Senha-forte-123", _db.Users[0].PasswordHash); // regravado (o fake gera o mesmo formato; o ponto é que foi chamado)
    }

    // ------------------------------------------------------------------ refresh rotativo

    [Fact]
    public async Task Refresh_rotates_the_token_in_the_same_family_and_revokes_the_used_one()
    {
        var (first, _) = await _auth.LoginAsync("ana@empresa.com", "Senha-forte-123", default);

        var (second, _) = await _auth.RefreshAsync(first.RefreshToken, default);

        Assert.NotEqual(first.RefreshToken, second.RefreshToken);
        Assert.Equal(2, _db.Tokens.Count);
        var old = _db.Tokens.Single(t => t.TokenHash == RefreshToken.Hash(first.RefreshToken));
        var current = _db.Tokens.Single(t => t.TokenHash == RefreshToken.Hash(second.RefreshToken));
        Assert.True(old.IsRevoked);
        Assert.Equal(current.Id, old.ReplacedById);
        Assert.Equal(old.FamilyId, current.FamilyId); // mesma sessão
        Assert.False(current.IsRevoked);
    }

    [Fact]
    public async Task Reusing_an_already_rotated_token_revokes_the_whole_session()
    {
        var (first, _) = await _auth.LoginAsync("ana@empresa.com", "Senha-forte-123", default);
        var (second, _) = await _auth.RefreshAsync(first.RefreshToken, default); // o cliente legítimo já trocou

        // alguém (que copiou o token antigo) tenta usá-lo
        await Assert.ThrowsAsync<AuthenticationFailedException>(() => _auth.RefreshAsync(first.RefreshToken, default));

        // e o token NOVO do cliente legítimo também morreu: a sessão inteira foi revogada
        Assert.All(_db.Tokens, t => Assert.True(t.IsRevoked));
        await Assert.ThrowsAsync<AuthenticationFailedException>(() => _auth.RefreshAsync(second.RefreshToken, default));
    }

    [Fact]
    public async Task An_expired_refresh_token_is_rejected()
    {
        var (tokens, _) = await _auth.LoginAsync("ana@empresa.com", "Senha-forte-123", default);
        _clock.Advance(TimeSpan.FromDays(8));

        var ex = await Assert.ThrowsAsync<AuthenticationFailedException>(() => _auth.RefreshAsync(tokens.RefreshToken, default));

        Assert.Contains("expirada", ex.Message);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("token-inexistente")]
    public async Task Unknown_or_missing_refresh_tokens_are_rejected(string? token)
    {
        await Assert.ThrowsAsync<AuthenticationFailedException>(() => _auth.RefreshAsync(token, default));
    }

    [Fact]
    public async Task A_deactivated_user_loses_the_session_at_the_next_refresh()
    {
        var (tokens, _) = await _auth.LoginAsync("ana@empresa.com", "Senha-forte-123", default);
        _db.Users[0].Deactivate();

        await Assert.ThrowsAsync<AuthenticationFailedException>(() => _auth.RefreshAsync(tokens.RefreshToken, default));
    }

    [Fact]
    public async Task Logout_revokes_the_session_and_is_idempotent_and_silent_about_unknown_tokens()
    {
        var (tokens, _) = await _auth.LoginAsync("ana@empresa.com", "Senha-forte-123", default);

        await _auth.LogoutAsync(tokens.RefreshToken, default);
        await _auth.LogoutAsync(tokens.RefreshToken, default);      // de novo: sem erro
        await _auth.LogoutAsync("nao-existe", default);              // desconhecido: sem erro e sem revelar nada
        await _auth.LogoutAsync(null, default);

        Assert.All(_db.Tokens, t => Assert.True(t.IsRevoked));
        await Assert.ThrowsAsync<AuthenticationFailedException>(() => _auth.RefreshAsync(tokens.RefreshToken, default));
    }
}

public class PasswordPolicyTests
{
    [Theory]
    [InlineData("Senha-forte-123")]
    [InlineData("umaSenhaLonga2026")]
    [InlineData("abcdefghi1")]
    public void Accepts_reasonable_passwords(string password) => PasswordPolicy.Validate(password);

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("curta1")]                 // < 10
    [InlineData("somente-letras-aqui")]    // sem número
    [InlineData("12345678901234")]         // sem letra
    [InlineData("aaaaaaaaaa1")]            // poucos caracteres distintos
    [InlineData("password12")]             // comum
    [InlineData("Admin12345")]             // comum (sem diferenciar maiúsculas)
    public void Rejects_weak_passwords(string? password)
    {
        Assert.Throws<DomainException>(() => PasswordPolicy.Validate(password));
    }

    [Fact]
    public void Rejects_absurdly_long_passwords_to_avoid_hashing_abuse()
    {
        Assert.Throws<DomainException>(() => PasswordPolicy.Validate(new string('a', 100) + "1234567890123456789012345678901"));
    }
}

public class UserAndDeviceServiceTests
{
    private static readonly DateTimeOffset T0 = new(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);
    private readonly SecurityFakes _db = new();
    private readonly FakeTimeProvider _clock = new(T0);

    private UserService Users() => new(_db.UserStore, new FakeHasher(), _db.Uow, _clock, NullLogger<UserService>.Instance);
    private DeviceService Devices() => new(_db.DeviceStore, _db.Uow, _clock);

    [Fact]
    public async Task Creating_a_user_enforces_the_password_policy_and_unique_email()
    {
        var user = await Users().CreateAsync("Bia@X.com", "Senha-forte-123", UserRole.Viewer, default);

        Assert.Equal("bia@x.com", user.Email);
        Assert.NotEqual("Senha-forte-123", user.PasswordHash); // guardado como hash
        await Assert.ThrowsAsync<ConflictException>(() => Users().CreateAsync("bia@x.com", "Outra-senha-123", UserRole.Admin, default));
        await Assert.ThrowsAsync<DomainException>(() => Users().CreateAsync("c@x.com", "fraca", UserRole.Viewer, default));
    }

    [Fact]
    public async Task Bootstrap_admin_is_created_only_on_a_fresh_installation()
    {
        Assert.True(await Users().EnsureBootstrapAdminAsync("admin@x.com", "Senha-forte-123", default));
        Assert.Equal(UserRole.Admin, Assert.Single(_db.Users).Role);

        // instalação já em uso: mudar a configuração NÃO cria nem altera administradores
        Assert.False(await Users().EnsureBootstrapAdminAsync("outro@x.com", "Senha-forte-456", default));
        Assert.Single(_db.Users);
    }

    [Theory]
    [InlineData("", "")]
    [InlineData("admin@x.com", "")]
    [InlineData("", "Senha-forte-123")]
    public async Task Bootstrap_without_complete_configuration_does_nothing(string email, string password)
    {
        Assert.False(await Users().EnsureBootstrapAdminAsync(email, password, default));
        Assert.Empty(_db.Users);
    }

    [Fact]
    public async Task Role_and_active_state_can_be_changed_and_unknown_users_are_404()
    {
        var user = await Users().CreateAsync("d@x.com", "Senha-forte-123", UserRole.Viewer, default);

        Assert.Equal(UserRole.Admin, (await Users().ChangeRoleAsync(user.Id, UserRole.Admin, default)).Role);
        Assert.False((await Users().SetActiveAsync(user.Id, false, default)).Active);
        await Assert.ThrowsAsync<NotFoundException>(() => Users().ChangeRoleAsync(Guid.NewGuid(), UserRole.Admin, default));
    }

    [Fact]
    public async Task A_device_key_is_shown_once_and_rotation_invalidates_the_old_one()
    {
        var (device, key) = await Devices().CreateAsync("Gateway", default);
        Assert.Equal(ApiKey.Hash(key), device.ApiKeyHash);

        var (_, rotated) = await Devices().RotateKeyAsync(device.Id, default);

        Assert.NotEqual(key, rotated);
        Assert.NotEqual(ApiKey.Hash(key), device.ApiKeyHash); // a chave antiga já não identifica o dispositivo
        Assert.Equal(ApiKey.Hash(rotated), device.ApiKeyHash);
    }

    [Fact]
    public async Task Device_id_can_be_preserved_but_never_duplicated()
    {
        var id = Guid.NewGuid();
        var (device, _) = await Devices().CreateAsync("Importado", default, id);

        Assert.Equal(id, device.Id);
        await Assert.ThrowsAsync<ConflictException>(() => Devices().CreateAsync("Outro", default, id));
        await Assert.ThrowsAsync<NotFoundException>(() => Devices().GetAsync(Guid.NewGuid(), default));
    }
}

public class IngestionOwnershipTests
{
    private static readonly DateTimeOffset Now = new(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);
    private static readonly DeviceIdentity Gateway = new(Guid.NewGuid(), "Gateway A");

    private readonly FakeTimeProvider _clock = new(Now);
    private readonly Registry _registry = new();
    private readonly Recorder _publisher = new();
    private readonly IngestionService _service;

    private readonly Guid _mine = Guid.NewGuid();

    public IngestionOwnershipTests()
    {
        _registry.Add(new SensorInfo(_mine, Gateway.Id, MetricType.Temperature, "°C", true));
        _service = new IngestionService(
            new ReadingRequestValidator(Options.Create(new IngestionOptions()), _clock), _publisher, _clock, _registry);
    }

    [Fact]
    public async Task A_reading_for_a_sensor_owned_by_the_device_is_accepted()
    {
        var result = await _service.IngestAsync([new(_mine, Now, 25, "°C")], Gateway, default);

        Assert.Equal(1, result.Accepted);
        Assert.Single(_publisher.Published);
    }

    [Fact]
    public async Task A_device_cannot_inject_readings_into_another_devices_sensor()
    {
        var theirs = Guid.NewGuid();
        _registry.Add(new SensorInfo(theirs, Guid.NewGuid(), MetricType.Temperature, "°C", true)); // de OUTRO dispositivo

        var result = await _service.IngestAsync([new(theirs, Now, 25, null)], Gateway, default);

        Assert.Equal(0, result.Accepted);
        Assert.Contains("não pertence", Assert.Single(result.Rejected).Errors.Single());
        Assert.Empty(_publisher.Published); // nada foi publicado
    }

    [Fact]
    public async Task Unknown_inactive_implausible_and_mismatched_unit_readings_are_rejected_with_a_reason()
    {
        var inactive = Guid.NewGuid();
        _registry.Add(new SensorInfo(inactive, Gateway.Id, MetricType.Temperature, "°C", false));

        var result = await _service.IngestAsync(
        [
            new(Guid.NewGuid(), Now, 1, null),   // desconhecido
            new(inactive, Now, 1, null),         // inativo
            new(_mine, Now, 99_999, null),       // fora da faixa (temperatura)
            new(_mine, Now, 20, "°F"),           // unidade divergente
            new(_mine, Now, 21, "°C")            // ok
        ], Gateway, default);

        Assert.Equal(1, result.Accepted);
        Assert.Equal([0, 1, 2, 3], result.Rejected.Select(r => r.Index));
        Assert.Contains("desconhecido", result.Rejected[0].Errors.Single());
        Assert.Contains("inativo", result.Rejected[1].Errors.Single());
        Assert.Contains("faixa plausível", result.Rejected[2].Errors.Single());
        Assert.Contains("unidade", result.Rejected[3].Errors.Single());
    }

    [Fact]
    public async Task Without_a_device_identity_ownership_is_not_checked()
    {
        var result = await _service.IngestAsync([new(Guid.NewGuid(), Now, 1, null)], default);

        Assert.Equal(1, result.Accepted); // segurança desligada: comportamento anterior à Fase 8
    }

    [Fact]
    public async Task Sensor_lookups_use_the_registry_once_per_reading_and_do_not_publish_invalid_ones()
    {
        var batch = Enumerable.Range(0, 300).Select(i => new ReadingRequest(_mine, Now.AddMilliseconds(i), i % 40, "°C")).ToList();

        var result = await _service.IngestAsync(batch, Gateway, default);

        Assert.Equal(300, result.Accepted);
        Assert.Equal(300, _registry.Lookups);
    }

    private sealed class Registry : ISensorRegistry
    {
        private readonly Dictionary<Guid, SensorInfo> _all = [];
        public int Lookups { get; private set; }
        public void Add(SensorInfo info) => _all[info.Id] = info;

        public Task<SensorInfo?> GetAsync(Guid sensorId, CancellationToken cancellationToken)
        {
            Lookups++;
            return Task.FromResult(_all.GetValueOrDefault(sensorId));
        }
    }

    private sealed class Recorder : IReadingPublisher
    {
        public List<ReadingMessage> Published { get; } = [];

        public Task PublishAsync(IReadOnlyList<ReadingMessage> messages, CancellationToken cancellationToken)
        {
            Published.AddRange(messages);
            return Task.CompletedTask;
        }
    }
}
