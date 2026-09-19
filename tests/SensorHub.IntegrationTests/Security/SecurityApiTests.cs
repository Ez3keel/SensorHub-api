using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Security.Claims;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;
using SensorHub.Domain.Alerts;
using SensorHub.Infrastructure.Persistence;
using SensorHub.IntegrationTests.Alerting;
using SensorHub.IntegrationTests.Infrastructure;

namespace SensorHub.IntegrationTests.Security;

/// <summary>A API real com a segurança LIGADA (JWT, chaves de API, rate limiting) sobre o Postgres e o Redis dos containers.</summary>
internal sealed class SecureApi : IAsyncDisposable
{
    public const string AdminEmail = "admin@teste.local";
    public const string AdminPassword = "Admin-de-teste-123456";
    public const string SigningKey = "chave-de-teste-com-mais-de-trinta-e-dois-bytes-0123456789";

    public ApiFactory Factory { get; }
    public HttpClient Http { get; }

    public SecureApi(PlatformFixture platform, Dictionary<string, string?>? extra = null)
    {
        var overrides = new Dictionary<string, string?>
        {
            ["Security:Enabled"] = "true",
            ["Security:JwtSigningKey"] = SigningKey,
            ["Security:BootstrapAdmin:Email"] = AdminEmail,
            ["Security:BootstrapAdmin:Password"] = AdminPassword,
            ["Security:DeviceCacheSeconds"] = "1",
            ["Security:SensorCacheSeconds"] = "1",
            // limites folgados por padrão: só os testes de rate limiting os apertam
            ["Security:RateLimits:IngestionPerSecond"] = "5000",
            ["Security:RateLimits:IngestionBurst"] = "10000",
            ["Security:RateLimits:AuthPerMinute"] = "1000",
            ["Security:RateLimits:ApiPerMinute"] = "100000"
        };
        if (extra is not null) foreach (var (k, v) in extra) overrides[k] = v;

        Factory = new ApiFactory(platform.BootstrapServers, overrides, platform.ConnectionString, platform.RedisConnectionString);
        Http = Factory.CreateClient();
    }

    public record Session(string AccessToken, string RefreshToken, string UserId);

    public async Task<Session> LoginAsync(string email, string password)
    {
        var response = await Http.PostAsJsonAsync("/api/auth/login", new { email, password });
        response.EnsureSuccessStatusCode();
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        var root = doc.RootElement;
        return new Session(root.GetProperty("accessToken").GetString()!, root.GetProperty("refreshToken").GetString()!, root.GetProperty("user").GetProperty("id").GetString()!);
    }

    public Task<Session> AdminAsync() => LoginAsync(AdminEmail, AdminPassword);

    public async Task<HttpResponseMessage> SendAsync(HttpMethod method, string url, string? bearer = null, object? body = null, string? apiKey = null)
    {
        using var request = new HttpRequestMessage(method, url);
        if (bearer is not null) request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearer);
        if (apiKey is not null) request.Headers.Add("X-Api-Key", apiKey);
        if (body is not null) request.Content = JsonContent.Create(body);
        return await Http.SendAsync(request);
    }

    public static async Task<JsonElement> Json(HttpResponseMessage response)
    {
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        return doc.RootElement.Clone();
    }

    /// <summary>Cria um usuário com o papel dado (via API de administração) e devolve o login dele.</summary>
    public async Task<Session> UserAsync(string role)
    {
        var admin = await AdminAsync();
        var email = $"{role.ToLowerInvariant()}-{Guid.NewGuid():N}@teste.local";
        var created = await SendAsync(HttpMethod.Post, "/api/users", admin.AccessToken, new { email, password = "Senha-de-teste-123", role });
        Assert.Equal(HttpStatusCode.Created, created.StatusCode);
        return await LoginAsync(email, "Senha-de-teste-123");
    }

    public async ValueTask DisposeAsync()
    {
        Http.Dispose();
        await Factory.DisposeAsync();
    }
}

[Collection(PlatformCollection.Name)]
public class AuthenticationTests(PlatformFixture platform)
{
    [Theory]
    [InlineData("GET", "/api/sensors")]
    [InlineData("GET", "/api/alerts")]
    [InlineData("GET", "/api/alert-rules")]
    [InlineData("GET", "/api/devices")]
    [InlineData("GET", "/api/users")]
    [InlineData("GET", "/api/auth/me")]
    [InlineData("GET", "/api/sensors/latest?ids=00000000-0000-0000-0000-000000000001")]
    [InlineData("POST", "/api/readings")]
    [InlineData("POST", "/api/readings/batch")]
    public async Task Every_protected_endpoint_rejects_anonymous_requests_with_401(string method, string url)
    {
        await using var api = new SecureApi(platform);

        var response = await api.SendAsync(new HttpMethod(method), url, body: method == "POST" ? new { } : null);

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task Health_and_metrics_stay_open_for_probes_and_scrapers()
    {
        await using var api = new SecureApi(platform);

        Assert.Equal(HttpStatusCode.OK, (await api.Http.GetAsync("/health/live")).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await api.Http.GetAsync("/metrics")).StatusCode);
    }

    [Fact]
    public async Task Login_returns_a_session_with_no_store_cache_control_and_the_users_role()
    {
        await using var api = new SecureApi(platform);

        var response = await api.Http.PostAsJsonAsync("/api/auth/login", new { email = SecureApi.AdminEmail, password = SecureApi.AdminPassword });

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains("no-store", response.Headers.CacheControl?.ToString());
        var body = await SecureApi.Json(response);
        Assert.Equal("Admin", body.GetProperty("user").GetProperty("role").GetString());
        Assert.False(body.GetProperty("user").TryGetProperty("passwordHash", out _)); // o hash nunca sai da API
        Assert.True(body.GetProperty("accessTokenExpiresAt").GetDateTimeOffset() > DateTimeOffset.UtcNow);
    }

    [Theory]
    [InlineData(SecureApi.AdminEmail, "senha-errada-1234")]
    [InlineData("ninguem@teste.local", "Admin-de-teste-123456")]
    [InlineData("", "")]
    public async Task Wrong_credentials_are_401_with_an_identical_generic_message(string email, string password)
    {
        await using var api = new SecureApi(platform);

        var response = await api.Http.PostAsJsonAsync("/api/auth/login", new { email, password });

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        Assert.Equal("Credenciais inválidas.", (await SecureApi.Json(response)).GetProperty("detail").GetString());
    }

    [Fact]
    public async Task The_access_token_opens_the_api_and_me_reports_who_you_are()
    {
        await using var api = new SecureApi(platform);
        var admin = await api.AdminAsync();

        var list = await api.SendAsync(HttpMethod.Get, "/api/sensors", admin.AccessToken);
        var me = await SecureApi.Json(await api.SendAsync(HttpMethod.Get, "/api/auth/me", admin.AccessToken));

        Assert.Equal(HttpStatusCode.OK, list.StatusCode);
        Assert.Equal(SecureApi.AdminEmail, me.GetProperty("email").GetString());
        Assert.Equal("Admin", me.GetProperty("role").GetString());
    }

    [Fact]
    public async Task Passwords_are_stored_as_salted_hashes_never_in_plain_text()
    {
        await using var api = new SecureApi(platform);
        await api.AdminAsync();

        await using var command = platform.DataSource.CreateCommand("SELECT password_hash FROM users WHERE email = @e");
        command.Parameters.AddWithValue("e", SecureApi.AdminEmail);
        var hash = (string)(await command.ExecuteScalarAsync())!;

        Assert.DoesNotContain(SecureApi.AdminPassword, hash);
        Assert.StartsWith("AQAAAA", hash);   // formato v3 do Identity: PBKDF2 com sal e iterações embutidos
        Assert.True(hash.Length > 60);
    }

    // ------------------------------------------------------------------ JWT forjado / inválido

    private static string Forge(string? key = null, string audience = "sensorhub-clients", string issuer = "sensorhub",
        DateTime? expires = null, string role = "Admin", string algorithm = SecurityAlgorithms.HmacSha256)
    {
        var handler = new JsonWebTokenHandler();
        return handler.CreateToken(new SecurityTokenDescriptor
        {
            Issuer = issuer, Audience = audience,
            Subject = new ClaimsIdentity([new Claim("sub", Guid.NewGuid().ToString()), new Claim("role", role)]),
            Expires = expires ?? DateTime.UtcNow.AddMinutes(10),
            NotBefore = (expires ?? DateTime.UtcNow.AddMinutes(10)).AddMinutes(-20),
            SigningCredentials = new SigningCredentials(new SymmetricSecurityKey(Encoding.UTF8.GetBytes(key ?? SecureApi.SigningKey)), algorithm)
        });
    }

    [Fact]
    public async Task A_well_formed_token_signed_with_the_right_key_is_the_only_thing_accepted()
    {
        await using var api = new SecureApi(platform);

        Assert.Equal(HttpStatusCode.OK, (await api.SendAsync(HttpMethod.Get, "/api/sensors", Forge())).StatusCode); // controle: o forjador acerta a chave
        Assert.Equal(HttpStatusCode.Unauthorized, (await api.SendAsync(HttpMethod.Get, "/api/sensors", Forge(key: "outra-chave-qualquer-com-mais-de-32-bytes-abcdef"))).StatusCode);
        Assert.Equal(HttpStatusCode.Unauthorized, (await api.SendAsync(HttpMethod.Get, "/api/sensors", Forge(audience: "outro-publico"))).StatusCode);
        Assert.Equal(HttpStatusCode.Unauthorized, (await api.SendAsync(HttpMethod.Get, "/api/sensors", Forge(issuer: "outro-emissor"))).StatusCode);
    }

    [Fact]
    public async Task An_expired_token_is_rejected_even_when_the_signature_is_valid()
    {
        await using var api = new SecureApi(platform);

        var expired = Forge(expires: DateTime.UtcNow.AddMinutes(-5));

        Assert.Equal(HttpStatusCode.Unauthorized, (await api.SendAsync(HttpMethod.Get, "/api/sensors", expired)).StatusCode);
    }

    [Fact]
    public async Task A_token_with_alg_none_is_rejected()
    {
        await using var api = new SecureApi(platform);
        static string B64(string s) => Convert.ToBase64String(Encoding.UTF8.GetBytes(s)).TrimEnd('=').Replace('+', '-').Replace('/', '_');
        var unsigned = $"{B64("""{"alg":"none","typ":"JWT"}""")}.{B64($$"""{"sub":"x","role":"Admin","iss":"sensorhub","aud":"sensorhub-clients","exp":{{DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeSeconds()}}}""")}.";

        Assert.Equal(HttpStatusCode.Unauthorized, (await api.SendAsync(HttpMethod.Get, "/api/sensors", unsigned)).StatusCode);
    }

    [Fact]
    public async Task Tampering_with_the_role_inside_a_valid_token_breaks_the_signature()
    {
        await using var api = new SecureApi(platform);
        var viewer = await api.UserAsync("Viewer");
        var parts = viewer.AccessToken.Split('.');
        var payload = Encoding.UTF8.GetString(Convert.FromBase64String(parts[1].Replace('-', '+').Replace('_', '/').PadRight((parts[1].Length + 3) / 4 * 4, '=')));
        var forgedPayload = Convert.ToBase64String(Encoding.UTF8.GetBytes(payload.Replace("\"Viewer\"", "\"Admin\""))).TrimEnd('=').Replace('+', '-').Replace('/', '_');
        var tampered = $"{parts[0]}.{forgedPayload}.{parts[2]}"; // a assinatura é a do payload original

        Assert.NotEqual(viewer.AccessToken, tampered);
        Assert.Equal(HttpStatusCode.Unauthorized, (await api.SendAsync(HttpMethod.Get, "/api/users", tampered)).StatusCode);
    }

    // ------------------------------------------------------------------ papéis

    [Fact]
    public async Task The_role_matrix_is_enforced_viewer_reads_operator_acknowledges_admin_manages()
    {
        await using var api = new SecureApi(platform);
        var viewer = await api.UserAsync("Viewer");
        var op = await api.UserAsync("Operator");
        var admin = await api.AdminAsync();
        var alert = Alert.Fire(AlertTestData.Rule(), DateTimeOffset.UtcNow, 91);
        await new PostgresAlertStore(platform.ContextFactory).InsertFiredAsync(alert, default);
        var ackUrl = $"/api/alerts/{alert.Id}/acknowledge";
        var newSensor = new { deviceId = platform.SharedDeviceId, name = "s-matriz", metric = "Temperature", unit = "°C", group = "g" };

        // leitura: os três papéis
        foreach (var token in new[] { viewer, op, admin })
            Assert.Equal(HttpStatusCode.OK, (await api.SendAsync(HttpMethod.Get, "/api/sensors", token.AccessToken)).StatusCode);

        // reconhecer alerta: Operator e acima. Viewer recebe 403 (autenticado, sem permissão), NÃO 401.
        Assert.Equal(HttpStatusCode.Forbidden, (await api.SendAsync(HttpMethod.Post, ackUrl, viewer.AccessToken, new { user = "v" })).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await api.SendAsync(HttpMethod.Post, ackUrl, op.AccessToken, new { user = "op" })).StatusCode);

        // administração: só Admin
        Assert.Equal(HttpStatusCode.Forbidden, (await api.SendAsync(HttpMethod.Post, "/api/sensors", viewer.AccessToken, newSensor)).StatusCode);
        Assert.Equal(HttpStatusCode.Forbidden, (await api.SendAsync(HttpMethod.Post, "/api/sensors", op.AccessToken, newSensor)).StatusCode);
        Assert.Equal(HttpStatusCode.Created, (await api.SendAsync(HttpMethod.Post, "/api/sensors", admin.AccessToken, newSensor)).StatusCode);
        Assert.Equal(HttpStatusCode.Forbidden, (await api.SendAsync(HttpMethod.Get, "/api/users", op.AccessToken)).StatusCode);
        Assert.Equal(HttpStatusCode.Forbidden, (await api.SendAsync(HttpMethod.Get, "/api/devices", viewer.AccessToken)).StatusCode);
        Assert.Equal(HttpStatusCode.Forbidden, (await api.SendAsync(HttpMethod.Delete, $"/api/alert-rules/{Guid.NewGuid()}", op.AccessToken)).StatusCode);
    }

    [Fact]
    public async Task User_creation_enforces_the_password_policy_and_unique_email_and_never_returns_the_hash()
    {
        await using var api = new SecureApi(platform);
        var admin = await api.AdminAsync();

        var weak = await api.SendAsync(HttpMethod.Post, "/api/users", admin.AccessToken, new { email = "fraco@teste.local", password = "curta", role = "Viewer" });
        var ok = await api.SendAsync(HttpMethod.Post, "/api/users", admin.AccessToken, new { email = "Duplicado@Teste.Local", password = "Senha-forte-1234", role = "Viewer" });
        var dupe = await api.SendAsync(HttpMethod.Post, "/api/users", admin.AccessToken, new { email = "duplicado@teste.local", password = "Senha-forte-1234", role = "Admin" });

        Assert.Equal(HttpStatusCode.BadRequest, weak.StatusCode);
        Assert.Equal(HttpStatusCode.Created, ok.StatusCode);
        Assert.DoesNotContain("password", (await ok.Content.ReadAsStringAsync()), StringComparison.OrdinalIgnoreCase);
        Assert.Equal(HttpStatusCode.Conflict, dupe.StatusCode);
    }

    [Fact]
    public async Task A_deactivated_user_cannot_log_in_and_loses_the_session_at_refresh()
    {
        await using var api = new SecureApi(platform);
        var user = await api.UserAsync("Viewer");
        var admin = await api.AdminAsync();

        await api.SendAsync(HttpMethod.Patch, $"/api/users/{user.UserId}/active", admin.AccessToken, new { active = false });

        var refresh = await api.Http.PostAsJsonAsync("/api/auth/refresh", new { refreshToken = user.RefreshToken });
        Assert.Equal(HttpStatusCode.Unauthorized, refresh.StatusCode);
    }

    // ------------------------------------------------------------------ refresh rotativo

    [Fact]
    public async Task Refresh_rotation_works_and_the_old_token_stops_working()
    {
        await using var api = new SecureApi(platform);
        var first = await api.AdminAsync();

        var refreshed = await api.Http.PostAsJsonAsync("/api/auth/refresh", new { refreshToken = first.RefreshToken });
        Assert.Equal(HttpStatusCode.OK, refreshed.StatusCode);
        var second = await SecureApi.Json(refreshed);
        Assert.NotEqual(first.RefreshToken, second.GetProperty("refreshToken").GetString());

        // o token antigo já foi trocado
        Assert.Equal(HttpStatusCode.Unauthorized, (await api.Http.PostAsJsonAsync("/api/auth/refresh", new { refreshToken = first.RefreshToken })).StatusCode);
    }

    [Fact]
    public async Task Reusing_a_rotated_refresh_token_kills_the_whole_session_including_the_new_token()
    {
        await using var api = new SecureApi(platform);
        var first = await api.AdminAsync();
        var second = await SecureApi.Json(await api.Http.PostAsJsonAsync("/api/auth/refresh", new { refreshToken = first.RefreshToken }));
        var newRefresh = second.GetProperty("refreshToken").GetString();

        // um atacante que copiou o token ANTIGO tenta usá-lo
        Assert.Equal(HttpStatusCode.Unauthorized, (await api.Http.PostAsJsonAsync("/api/auth/refresh", new { refreshToken = first.RefreshToken })).StatusCode);

        // e o token NOVO do usuário legítimo também foi revogado: a família inteira caiu
        Assert.Equal(HttpStatusCode.Unauthorized, (await api.Http.PostAsJsonAsync("/api/auth/refresh", new { refreshToken = newRefresh })).StatusCode);
    }

    [Fact]
    public async Task Logout_revokes_the_session_and_is_safe_to_repeat()
    {
        await using var api = new SecureApi(platform);
        var session = await api.AdminAsync();

        Assert.Equal(HttpStatusCode.NoContent, (await api.Http.PostAsJsonAsync("/api/auth/logout", new { refreshToken = session.RefreshToken })).StatusCode);
        Assert.Equal(HttpStatusCode.NoContent, (await api.Http.PostAsJsonAsync("/api/auth/logout", new { refreshToken = session.RefreshToken })).StatusCode);
        Assert.Equal(HttpStatusCode.NoContent, (await api.Http.PostAsJsonAsync("/api/auth/logout", new { refreshToken = "desconhecido" })).StatusCode);

        Assert.Equal(HttpStatusCode.Unauthorized, (await api.Http.PostAsJsonAsync("/api/auth/refresh", new { refreshToken = session.RefreshToken })).StatusCode);
    }

    // ------------------------------------------------------------------ SignalR autenticado

    [Fact]
    public async Task The_realtime_hub_requires_a_valid_token_passed_by_query_string_for_websockets()
    {
        await using var api = new SecureApi(platform, new() { ["Realtime:Enabled"] = "false" });
        var admin = await api.AdminAsync();

        HubConnection Connect(Func<Task<string?>>? token) => new HubConnectionBuilder()
            .WithUrl(new Uri(api.Factory.Server.BaseAddress, "hubs/telemetry"), o =>
            {
                o.HttpMessageHandlerFactory = _ => api.Factory.Server.CreateHandler();
                o.Transports = HttpTransportType.LongPolling;
                if (token is not null) o.AccessTokenProvider = token;
            }).Build();

        await using var anonymous = Connect(null);
        await Assert.ThrowsAnyAsync<Exception>(() => anonymous.StartAsync()); // 401

        await using var authenticated = Connect(() => Task.FromResult<string?>(admin.AccessToken));
        await authenticated.StartAsync();
        Assert.Equal(HubConnectionState.Connected, authenticated.State);
    }

    // ------------------------------------------------------------------ configuração segura

    [Fact]
    public void The_api_refuses_to_start_with_security_on_and_a_weak_or_missing_signing_key()
    {
        // O host sobe dentro do construtor (CreateClient): a falha aparece já na subida, não na primeira requisição.
        Assert.ThrowsAny<Exception>(() => new SecureApi(platform, new() { ["Security:JwtSigningKey"] = "curta" }));
        Assert.ThrowsAny<Exception>(() => new SecureApi(platform, new() { ["Security:JwtSigningKey"] = "" }));
    }
}
