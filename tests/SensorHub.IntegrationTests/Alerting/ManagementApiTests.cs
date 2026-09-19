using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using SensorHub.Domain.Alerts;
using SensorHub.Infrastructure.Persistence;
using SensorHub.IntegrationTests.Infrastructure;

namespace SensorHub.IntegrationTests.Alerting;

[Collection(PlatformCollection.Name)]
public class ManagementApiTests(PlatformFixture platform)
{
    private readonly HttpClient _http = platform.Api.CreateClient();

    private async Task<JsonElement> ReadAsync(HttpResponseMessage response)
    {
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        return doc.RootElement.Clone();
    }

    private async Task<Guid> CreateSensorAsync(string group = "planta-x", string metric = "Temperature")
    {
        var response = await _http.PostAsJsonAsync("/api/sensors",
            new { deviceId = platform.SharedDeviceId, name = $"forno-{Guid.NewGuid():N}"[..16], metric, unit = "°C", group });
        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        return (await ReadAsync(response)).GetProperty("id").GetGuid();
    }

    // ------------------------------------------------------------------ sensores

    [Fact]
    public async Task Sensor_is_created_returned_by_id_and_serialized_with_enum_names()
    {
        var response = await _http.PostAsJsonAsync("/api/sensors",
            new { deviceId = platform.SharedDeviceId, name = "  Forno 1  ", metric = "Vibration", unit = "mm/s", group = "fabrica-1" });

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        var created = await ReadAsync(response);
        Assert.Equal("Forno 1", created.GetProperty("name").GetString());
        Assert.Equal("Vibration", created.GetProperty("metric").GetString()); // enum como texto, não número
        Assert.True(created.GetProperty("active").GetBoolean());
        Assert.NotNull(response.Headers.Location);

        var fetched = await ReadAsync(await _http.GetAsync($"/api/sensors/{created.GetProperty("id").GetGuid()}"));
        Assert.Equal("fabrica-1", fetched.GetProperty("group").GetString());
    }

    [Fact]
    public async Task Sensor_can_be_created_with_an_explicit_id_and_a_duplicate_id_is_a_409()
    {
        var id = Guid.NewGuid();
        var body = new { id, deviceId = platform.SharedDeviceId, name = "importado", metric = "Pressure", unit = "hPa", group = "g" };

        var first = await _http.PostAsJsonAsync("/api/sensors", body);
        var duplicate = await _http.PostAsJsonAsync("/api/sensors", body);

        Assert.Equal(HttpStatusCode.Created, first.StatusCode);
        Assert.Equal(id, (await ReadAsync(first)).GetProperty("id").GetGuid()); // a identidade externa foi preservada
        Assert.Equal(HttpStatusCode.Conflict, duplicate.StatusCode);
    }

    [Fact]
    public async Task A_sensor_needs_an_existing_device()
    {
        var response = await _http.PostAsJsonAsync("/api/sensors",
            new { deviceId = Guid.NewGuid(), name = "orfao", metric = "Temperature", unit = "°C", group = "g" });

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode); // sensor sem dono não existe
    }

    [Theory]
    [InlineData("", "°C", "g")]
    [InlineData("nome", "", "g")]
    [InlineData("nome", "°C", "")]
    public async Task Invalid_sensor_data_is_a_400_with_the_domain_message(string name, string unit, string group)
    {
        var response = await _http.PostAsJsonAsync("/api/sensors",
            new { deviceId = platform.SharedDeviceId, name, metric = "Temperature", unit, group });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.False(string.IsNullOrWhiteSpace((await ReadAsync(response)).GetProperty("detail").GetString()));
    }

    [Fact]
    public async Task Unknown_metric_or_broken_body_is_a_400_not_a_500()
    {
        var bad = await _http.PostAsJsonAsync("/api/sensors", new { deviceId = platform.SharedDeviceId, name = "x", metric = "Radioactivity", unit = "u", group = "g" });
        Assert.Equal(HttpStatusCode.BadRequest, bad.StatusCode);

        var empty = await _http.PostAsJsonAsync("/api/sensors", new { deviceId = Guid.Empty, name = "x", metric = "Humidity", unit = "%", group = "g" });
        Assert.Equal(HttpStatusCode.BadRequest, empty.StatusCode);
    }

    [Fact]
    public async Task Sensors_are_listed_filtered_by_group_and_paginated()
    {
        var group = $"grp-{Guid.NewGuid():N}";
        for (var i = 0; i < 5; i++) await CreateSensorAsync(group);
        var inactive = await CreateSensorAsync(group);
        await _http.PatchAsJsonAsync($"/api/sensors/{inactive}/active", new { active = false });

        var all = await ReadAsync(await _http.GetAsync($"/api/sensors?group={group}"));
        var active = await ReadAsync(await _http.GetAsync($"/api/sensors?group={group}&active=true"));
        var page = await ReadAsync(await _http.GetAsync($"/api/sensors?group={group}&skip=4&take=10"));

        Assert.Equal(6, all.GetArrayLength());
        Assert.Equal(5, active.GetArrayLength());
        Assert.Equal(2, page.GetArrayLength());
    }

    [Fact]
    public async Task Sensor_can_be_deactivated_and_reactivated_and_unknown_id_is_404()
    {
        var id = await CreateSensorAsync();

        var off = await ReadAsync(await _http.PatchAsJsonAsync($"/api/sensors/{id}/active", new { active = false }));
        Assert.False(off.GetProperty("active").GetBoolean());
        var on = await ReadAsync(await _http.PatchAsJsonAsync($"/api/sensors/{id}/active", new { active = true }));
        Assert.True(on.GetProperty("active").GetBoolean());

        Assert.Equal(HttpStatusCode.NotFound, (await _http.GetAsync($"/api/sensors/{Guid.NewGuid()}")).StatusCode);
    }

    // ------------------------------------------------------------------ regras

    [Fact]
    public async Task Rule_for_an_unknown_sensor_is_404()
    {
        var response = await _http.PostAsJsonAsync("/api/alert-rules", new
        {
            sensorId = Guid.NewGuid(), name = "r", type = "Threshold", comparison = "GreaterThan",
            threshold = 80, durationSeconds = 0, hysteresis = 0, severity = "Critical"
        });

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task Rule_is_created_listed_and_reflects_all_four_rule_types()
    {
        var sensor = await CreateSensorAsync();
        foreach (var (type, duration) in new[] { ("Threshold", 0), ("NoData", 120), ("WindowAverage", 60), ("RateOfChange", 60) })
        {
            var response = await _http.PostAsJsonAsync("/api/alert-rules", new
            {
                sensorId = sensor, name = $"regra {type}", type, comparison = "GreaterThan",
                threshold = 10, durationSeconds = duration, hysteresis = 1, severity = "Warning"
            });
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }

        var list = await ReadAsync(await _http.GetAsync($"/api/alert-rules?sensorId={sensor}"));

        Assert.Equal(4, list.GetArrayLength());
        Assert.Equal(["NoData", "RateOfChange", "Threshold", "WindowAverage"], list.EnumerateArray().Select(r => r.GetProperty("type").GetString()!).Order());
    }

    [Theory]
    [InlineData("NoData", 0)]            // exige duração
    [InlineData("WindowAverage", 0)]
    [InlineData("Threshold", 999_999)]   // duração acima de 24 h
    public async Task Invalid_rules_are_rejected_with_400(string type, int durationSeconds)
    {
        var sensor = await CreateSensorAsync();

        var response = await _http.PostAsJsonAsync("/api/alert-rules", new
        {
            sensorId = sensor, name = "r", type, comparison = "GreaterThan",
            threshold = 1, durationSeconds, hysteresis = 0, severity = "Info"
        });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task Disabling_a_rule_drops_its_state_and_deleting_removes_it()
    {
        var sensor = await CreateSensorAsync();
        var created = await ReadAsync(await _http.PostAsJsonAsync("/api/alert-rules", new
        {
            sensorId = sensor, name = "r", type = "Threshold", comparison = "GreaterThan",
            threshold = 80, durationSeconds = 0, hysteresis = 0, severity = "Critical"
        }));
        var ruleId = created.GetProperty("id").GetGuid();
        var key = $"test:rule:{ruleId:D}";
        await platform.Redis.GetDatabase().StringSetAsync(key, "{}"); // estado que o motor teria deixado

        var disabled = await ReadAsync(await _http.PatchAsJsonAsync($"/api/alert-rules/{ruleId}/enabled", new { enabled = false }));

        Assert.False(disabled.GetProperty("enabled").GetBoolean());
        Assert.False(await platform.Redis.GetDatabase().KeyExistsAsync(key)); // ao reabilitar, recomeça limpa

        Assert.Equal(HttpStatusCode.NoContent, (await _http.DeleteAsync($"/api/alert-rules/{ruleId}")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await _http.DeleteAsync($"/api/alert-rules/{ruleId}")).StatusCode);
    }

    // ------------------------------------------------------------------ alertas

    private async Task<Alert> SeedAlertAsync(Guid sensorId, bool resolved = false)
    {
        var rule = AlertTestData.Rule(sensorId);
        var alert = Alert.Fire(rule, DateTimeOffset.UtcNow.AddMinutes(-5), 91);
        await new PostgresAlertStore(platform.ContextFactory).InsertFiredAsync(alert, default);
        if (resolved) await new PostgresAlertStore(platform.ContextFactory).ResolveOpenAsync(rule.Id, DateTimeOffset.UtcNow, 70, default);
        return alert;
    }

    [Fact]
    public async Task Alerts_are_listed_by_status_and_sensor_newest_first()
    {
        var sensor = Guid.NewGuid();
        var firing = await SeedAlertAsync(sensor);
        var resolved = await SeedAlertAsync(sensor, resolved: true);

        var open = await ReadAsync(await _http.GetAsync($"/api/alerts?sensorId={sensor}&status=Firing"));
        var closed = await ReadAsync(await _http.GetAsync($"/api/alerts?sensorId={sensor}&status=Resolved"));
        var all = await ReadAsync(await _http.GetAsync($"/api/alerts?sensorId={sensor}"));

        Assert.Equal(firing.Id, Assert.Single(open.EnumerateArray()).GetProperty("id").GetGuid());
        Assert.Equal(resolved.Id, Assert.Single(closed.EnumerateArray()).GetProperty("id").GetGuid());
        Assert.Equal(2, all.GetArrayLength());
    }

    [Fact]
    public async Task Alert_can_be_acknowledged_once_and_a_second_ack_is_a_409()
    {
        var alert = await SeedAlertAsync(Guid.NewGuid());

        var first = await _http.PostAsJsonAsync($"/api/alerts/{alert.Id}/acknowledge", new { user = "maria" });
        var second = await _http.PostAsJsonAsync($"/api/alerts/{alert.Id}/acknowledge", new { user = "joao" });

        Assert.Equal(HttpStatusCode.OK, first.StatusCode);
        var body = await ReadAsync(first);
        Assert.Equal("Acknowledged", body.GetProperty("status").GetString());
        Assert.Equal("maria", body.GetProperty("acknowledgedBy").GetString());
        Assert.Equal(HttpStatusCode.Conflict, second.StatusCode); // estado inválido para a operação, não dado inválido
    }

    [Fact]
    public async Task Acknowledging_a_resolved_or_unknown_alert_fails_appropriately()
    {
        var resolved = await SeedAlertAsync(Guid.NewGuid(), resolved: true);

        Assert.Equal(HttpStatusCode.Conflict, (await _http.PostAsJsonAsync($"/api/alerts/{resolved.Id}/acknowledge", new { user = "maria" })).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await _http.PostAsJsonAsync($"/api/alerts/{Guid.NewGuid()}/acknowledge", new { user = "maria" })).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await _http.GetAsync($"/api/alerts/{Guid.NewGuid()}")).StatusCode);
    }

    [Fact]
    public async Task Acknowledge_without_a_user_is_a_400()
    {
        var alert = await SeedAlertAsync(Guid.NewGuid());

        var response = await _http.PostAsJsonAsync($"/api/alerts/{alert.Id}/acknowledge", new { user = " " });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }
}
