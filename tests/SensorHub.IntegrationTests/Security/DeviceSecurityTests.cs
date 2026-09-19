using System.Net;
using System.Net.Http.Json;
using SensorHub.IntegrationTests.Infrastructure;

namespace SensorHub.IntegrationTests.Security;

[Collection(PlatformCollection.Name)]
public class DeviceSecurityTests(PlatformFixture platform)
{
    private sealed record TestDevice(Guid Id, string ApiKey, Guid SensorId);

    /// <summary>Cria (como administrador) um dispositivo com um sensor de temperatura e devolve a chave em texto puro.</summary>
    private static async Task<TestDevice> CreateDeviceAsync(SecureApi api, string adminToken, string unit = "°C", bool activeSensor = true)
    {
        var created = await SecureApi.Json(await api.SendAsync(HttpMethod.Post, "/api/devices", adminToken, new { name = $"gw-{Guid.NewGuid():N}"[..12] }));
        var deviceId = created.GetProperty("device").GetProperty("id").GetGuid();
        var key = created.GetProperty("apiKey").GetString()!;

        var sensor = await SecureApi.Json(await api.SendAsync(HttpMethod.Post, "/api/sensors", adminToken,
            new { deviceId, name = $"t-{Guid.NewGuid():N}"[..12], metric = "Temperature", unit, group = "seguranca" }));
        var sensorId = sensor.GetProperty("id").GetGuid();
        if (!activeSensor) await api.SendAsync(HttpMethod.Patch, $"/api/sensors/{sensorId}/active", adminToken, new { active = false });
        return new TestDevice(deviceId, key, sensorId);
    }

    private static object Reading(Guid sensor, double value = 21.5, string? unit = "°C") =>
        new { sensorId = sensor, timestamp = DateTimeOffset.UtcNow.AddSeconds(-1), value, unit };

    [Fact]
    public async Task A_device_authenticates_with_its_key_and_can_ingest_readings_of_its_own_sensors()
    {
        await using var api = new SecureApi(platform);
        var admin = await api.AdminAsync();
        var device = await CreateDeviceAsync(api, admin.AccessToken);

        var response = await api.SendAsync(HttpMethod.Post, "/api/readings/batch", body: new[] { Reading(device.SensorId), Reading(device.SensorId, 22) }, apiKey: device.ApiKey);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Equal(2, (await SecureApi.Json(response)).GetProperty("accepted").GetInt32());
    }

    [Fact]
    public async Task The_plain_key_is_shown_once_and_never_returned_by_reads()
    {
        await using var api = new SecureApi(platform);
        var admin = await api.AdminAsync();

        var created = await api.SendAsync(HttpMethod.Post, "/api/devices", admin.AccessToken, new { name = "Gateway Unico" });
        var body = await SecureApi.Json(created);
        var key = body.GetProperty("apiKey").GetString()!;
        var id = body.GetProperty("device").GetProperty("id").GetGuid();

        Assert.StartsWith("shk_", key);
        Assert.Contains("no-store", created.Headers.CacheControl?.ToString());
        Assert.Contains("não poderá ser exibida novamente", body.GetProperty("notice").GetString());

        var fetched = await (await api.SendAsync(HttpMethod.Get, $"/api/devices/{id}", admin.AccessToken)).Content.ReadAsStringAsync();
        var listed = await (await api.SendAsync(HttpMethod.Get, "/api/devices", admin.AccessToken)).Content.ReadAsStringAsync();
        Assert.DoesNotContain(key, fetched);
        Assert.DoesNotContain(key, listed);
        Assert.Contains(key[..12], fetched); // só o "hint" não secreto, para o operador identificar a chave

        await using var command = platform.DataSource.CreateCommand("SELECT api_key_hash FROM devices WHERE id = @id");
        command.Parameters.AddWithValue("id", id);
        var stored = (string)(await command.ExecuteScalarAsync())!;
        Assert.DoesNotContain(key, stored);  // no banco só existe o hash
        Assert.Equal(64, stored.Length);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("lixo")]
    [InlineData("shk_chave-bem-formada-mas-inexistente-000000000000")]
    public async Task Missing_malformed_or_unknown_keys_are_401(string? key)
    {
        await using var api = new SecureApi(platform);

        var response = await api.SendAsync(HttpMethod.Post, "/api/readings", body: Reading(Guid.NewGuid()), apiKey: key);

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task Credentials_are_not_interchangeable_a_device_key_is_not_a_user_and_a_user_token_is_not_a_device()
    {
        await using var api = new SecureApi(platform);
        var admin = await api.AdminAsync();
        var device = await CreateDeviceAsync(api, admin.AccessToken);

        // a chave do dispositivo não abre a API de leitura/administração
        Assert.Equal(HttpStatusCode.Unauthorized, (await api.SendAsync(HttpMethod.Get, "/api/sensors", apiKey: device.ApiKey)).StatusCode);
        // e o token de ADMIN não serve para ingerir leituras (a ingestão só aceita identidade de dispositivo)
        Assert.Equal(HttpStatusCode.Unauthorized, (await api.SendAsync(HttpMethod.Post, "/api/readings", admin.AccessToken, Reading(device.SensorId))).StatusCode);
    }

    [Fact]
    public async Task A_device_cannot_inject_readings_into_the_sensor_of_another_device()
    {
        await using var api = new SecureApi(platform);
        var admin = await api.AdminAsync();
        var attacker = await CreateDeviceAsync(api, admin.AccessToken);
        var victim = await CreateDeviceAsync(api, admin.AccessToken);

        var response = await api.SendAsync(HttpMethod.Post, "/api/readings/batch", body: new[] { Reading(victim.SensorId, 999) }, apiKey: attacker.ApiKey);

        Assert.Equal(HttpStatusCode.UnprocessableEntity, response.StatusCode); // tudo rejeitado
        var body = await SecureApi.Json(response);
        Assert.Equal(0, body.GetProperty("accepted").GetInt32());
        Assert.Contains("não pertence", body.GetProperty("rejected")[0].GetProperty("errors")[0].GetString());
    }

    [Fact]
    public async Task Unknown_inactive_implausible_and_wrong_unit_readings_are_rejected_with_reasons_and_valid_ones_still_pass()
    {
        await using var api = new SecureApi(platform);
        var admin = await api.AdminAsync();
        var device = await CreateDeviceAsync(api, admin.AccessToken);
        var inactive = await CreateDeviceAsync(api, admin.AccessToken, activeSensor: false);
        // um sensor inativo DO MESMO dispositivo
        var mineInactive = await SecureApi.Json(await api.SendAsync(HttpMethod.Post, "/api/sensors", admin.AccessToken,
            new { deviceId = device.Id, name = "inativo", metric = "Temperature", unit = "°C", group = "seguranca" }));
        var mineInactiveId = mineInactive.GetProperty("id").GetGuid();
        await api.SendAsync(HttpMethod.Patch, $"/api/sensors/{mineInactiveId}/active", admin.AccessToken, new { active = false });
        _ = inactive;

        await Task.Delay(1200); // o cache do registro de sensores expira (1 s neste teste)
        var response = await api.SendAsync(HttpMethod.Post, "/api/readings/batch", body: new object[]
        {
            Reading(Guid.NewGuid()),                 // 0 desconhecido
            Reading(mineInactiveId),                 // 1 inativo
            Reading(device.SensorId, 99_999),        // 2 fora da faixa
            Reading(device.SensorId, 20, "°F"),      // 3 unidade divergente
            Reading(device.SensorId, 21)             // 4 ok
        }, apiKey: device.ApiKey);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        var body = await SecureApi.Json(response);
        Assert.Equal(1, body.GetProperty("accepted").GetInt32());
        var reasons = body.GetProperty("rejected").EnumerateArray().Select(r => (r.GetProperty("index").GetInt32(), r.GetProperty("errors")[0].GetString()!)).ToList();
        Assert.Equal([0, 1, 2, 3], reasons.Select(r => r.Item1));
        Assert.Contains("desconhecido", reasons[0].Item2);
        Assert.Contains("inativo", reasons[1].Item2);
        Assert.Contains("faixa plausível", reasons[2].Item2);
        Assert.Contains("unidade", reasons[3].Item2);
    }

    [Fact]
    public async Task Rotating_the_key_invalidates_the_old_one_and_the_new_one_works()
    {
        await using var api = new SecureApi(platform);
        var admin = await api.AdminAsync();
        var device = await CreateDeviceAsync(api, admin.AccessToken);
        Assert.Equal(HttpStatusCode.Accepted, (await api.SendAsync(HttpMethod.Post, "/api/readings", body: Reading(device.SensorId), apiKey: device.ApiKey)).StatusCode);

        var rotated = await SecureApi.Json(await api.SendAsync(HttpMethod.Post, $"/api/devices/{device.Id}/rotate-key", admin.AccessToken));
        var newKey = rotated.GetProperty("apiKey").GetString()!;
        await Task.Delay(1200); // janela do cache de autenticação (Security:DeviceCacheSeconds = 1)

        Assert.NotEqual(device.ApiKey, newKey);
        Assert.Equal(HttpStatusCode.Unauthorized, (await api.SendAsync(HttpMethod.Post, "/api/readings", body: Reading(device.SensorId), apiKey: device.ApiKey)).StatusCode);
        Assert.Equal(HttpStatusCode.Accepted, (await api.SendAsync(HttpMethod.Post, "/api/readings", body: Reading(device.SensorId), apiKey: newKey)).StatusCode);
    }

    [Fact]
    public async Task Deactivating_a_device_cuts_its_ingestion()
    {
        await using var api = new SecureApi(platform);
        var admin = await api.AdminAsync();
        var device = await CreateDeviceAsync(api, admin.AccessToken);
        Assert.Equal(HttpStatusCode.Accepted, (await api.SendAsync(HttpMethod.Post, "/api/readings", body: Reading(device.SensorId), apiKey: device.ApiKey)).StatusCode);

        await api.SendAsync(HttpMethod.Patch, $"/api/devices/{device.Id}/active", admin.AccessToken, new { active = false });
        await Task.Delay(1200);

        Assert.Equal(HttpStatusCode.Unauthorized, (await api.SendAsync(HttpMethod.Post, "/api/readings", body: Reading(device.SensorId), apiKey: device.ApiKey)).StatusCode);
    }

    // ------------------------------------------------------------------ rate limiting

    [Fact]
    public async Task Ingestion_is_rate_limited_per_device_and_one_noisy_device_does_not_starve_the_others()
    {
        await using var api = new SecureApi(platform, new()
        {
            ["Security:RateLimits:IngestionPerSecond"] = "1",
            ["Security:RateLimits:IngestionBurst"] = "3"
        });
        var admin = await api.AdminAsync();
        var noisy = await CreateDeviceAsync(api, admin.AccessToken);
        var quiet = await CreateDeviceAsync(api, admin.AccessToken);

        var statuses = new List<HttpStatusCode>();
        HttpResponseMessage? rejected = null;
        for (var i = 0; i < 12; i++)
        {
            var r = await api.SendAsync(HttpMethod.Post, "/api/readings", body: Reading(noisy.SensorId), apiKey: noisy.ApiKey);
            statuses.Add(r.StatusCode);
            if (r.StatusCode == HttpStatusCode.TooManyRequests) rejected ??= r;
        }

        Assert.Contains(HttpStatusCode.Accepted, statuses);              // o começo passa (o balde tem 3 tokens)
        Assert.Contains(HttpStatusCode.TooManyRequests, statuses);       // depois é freado
        Assert.NotNull(rejected!.Headers.RetryAfter);                    // o cliente sabe QUANDO tentar de novo
        Assert.Equal("application/problem+json", rejected.Content.Headers.ContentType?.MediaType);

        // outro dispositivo tem o SEU balde e não é afetado
        var other = await api.SendAsync(HttpMethod.Post, "/api/readings", body: Reading(quiet.SensorId), apiKey: quiet.ApiKey);
        Assert.Equal(HttpStatusCode.Accepted, other.StatusCode);
    }

    [Fact]
    public async Task Login_is_rate_limited_to_slow_down_brute_force()
    {
        await using var api = new SecureApi(platform, new() { ["Security:RateLimits:AuthPerMinute"] = "3" });

        var statuses = new List<HttpStatusCode>();
        for (var i = 0; i < 6; i++)
            statuses.Add((await api.Http.PostAsJsonAsync("/api/auth/login", new { email = SecureApi.AdminEmail, password = $"tentativa-errada-{i}" })).StatusCode);

        Assert.Equal(3, statuses.Count(s => s == HttpStatusCode.Unauthorized));       // as 3 primeiras chegam a ser avaliadas
        Assert.Equal(3, statuses.Count(s => s == HttpStatusCode.TooManyRequests));    // as demais nem chegam ao hash de senha (que é caro)
    }

    [Fact]
    public async Task The_general_api_is_limited_per_user_and_tells_the_truth_about_when_to_retry()
    {
        await using var api = new SecureApi(platform, new() { ["Security:RateLimits:ApiPerMinute"] = "3" });
        var admin = await api.AdminAsync();

        var responses = new List<HttpResponseMessage>();
        for (var i = 0; i < 6; i++) responses.Add(await api.SendAsync(HttpMethod.Get, "/api/sensors", admin.AccessToken));

        Assert.Equal(3, responses.Count(r => r.StatusCode == HttpStatusCode.OK));
        var rejected = responses.First(r => r.StatusCode == HttpStatusCode.TooManyRequests);
        Assert.Equal(TimeSpan.FromSeconds(10), rejected.Headers.RetryAfter?.Delta); // um segmento da janela, não um "1" otimista
    }

    private static async Task<List<HttpStatusCode>> LoginAttemptsAsync(SecureApi api, string forwardedFor, int count)
    {
        var statuses = new List<HttpStatusCode>();
        for (var i = 0; i < count; i++)
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, "/api/auth/login") { Content = JsonContent.Create(new { email = SecureApi.AdminEmail, password = "errada" }) };
            request.Headers.Add("X-Forwarded-For", forwardedFor);
            statuses.Add((await api.Http.SendAsync(request)).StatusCode);
        }
        return statuses;
    }

    [Fact]
    public async Task Behind_a_trusted_proxy_the_login_limit_is_per_client_ip_not_per_proxy()
    {
        await using var api = new SecureApi(platform, new() { ["Security:RateLimits:AuthPerMinute"] = "2", ["Security:TrustForwardedHeaders"] = "true" });

        var first = await LoginAttemptsAsync(api, "203.0.113.10", 3);
        var other = await LoginAttemptsAsync(api, "203.0.113.20", 1);

        Assert.Equal(HttpStatusCode.TooManyRequests, first[2]); // o cliente 1 estourou o SEU limite...
        Assert.Equal(HttpStatusCode.Unauthorized, other[0]);    // ...e o cliente 2 não foi afetado
    }

    [Fact]
    public async Task Without_declaring_a_proxy_a_forged_forwarded_for_header_cannot_dodge_the_limit()
    {
        await using var api = new SecureApi(platform, new() { ["Security:RateLimits:AuthPerMinute"] = "2" }); // TrustForwardedHeaders = false

        var statuses = new List<HttpStatusCode>();
        for (var i = 0; i < 4; i++) statuses.AddRange(await LoginAttemptsAsync(api, $"198.51.100.{i}", 1)); // um "IP" novo a cada tentativa

        Assert.Equal(2, statuses.Count(s => s == HttpStatusCode.TooManyRequests)); // o cabeçalho foi ignorado
    }

    [Fact]
    public async Task Health_endpoints_are_never_rate_limited()
    {
        await using var api = new SecureApi(platform, new() { ["Security:RateLimits:AuthPerMinute"] = "1", ["Security:RateLimits:IngestionBurst"] = "1" });

        for (var i = 0; i < 20; i++)
            Assert.Equal(HttpStatusCode.OK, (await api.Http.GetAsync("/health/live")).StatusCode);
    }
}
