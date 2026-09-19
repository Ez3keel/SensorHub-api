using System.Net;
using System.Text.Json;
using SensorHub.Simulator.Load;

namespace SensorHub.Simulator.Tests;

public class FleetRegistrarTests
{
    [Fact]
    public async Task Registers_every_sensor_with_its_fixed_id_and_the_requested_rules()
    {
        var handler = new Stub(_ => HttpStatusCode.Created);
        var fleet = new SensorFleet(8);

        var report = await new FleetRegistrar(new HttpClient(handler) { BaseAddress = new Uri("http://api/") })
            .RegisterAsync(fleet, thresholdRules: true, noDataRules: true, noDataSeconds: 25);

        Assert.Equal(8, report.SensorsCreated);
        Assert.Equal(16, report.RulesCreated);
        Assert.Equal(0, report.Failures);

        var sensorBodies = handler.Bodies.Where(b => b.Url.EndsWith("api/sensors")).Select(b => JsonDocument.Parse(b.Body).RootElement).ToList();
        Assert.Equal(fleet.Sensors.Select(s => s.Id).Order(), sensorBodies.Select(b => b.GetProperty("id").GetGuid()).Order()); // ids preservados

        var noData = handler.Bodies.Where(b => b.Body.Contains("NoData")).Select(b => JsonDocument.Parse(b.Body).RootElement).ToList();
        Assert.Equal(8, noData.Count);
        Assert.All(noData, r => Assert.Equal(25, r.GetProperty("durationSeconds").GetInt32()));
    }

    [Fact]
    public async Task Is_idempotent_a_409_means_already_registered_and_skips_the_rules()
    {
        var handler = new Stub(url => url.EndsWith("api/sensors") ? HttpStatusCode.Conflict : HttpStatusCode.Created);

        var report = await new FleetRegistrar(new HttpClient(handler) { BaseAddress = new Uri("http://api/") })
            .RegisterAsync(new SensorFleet(5), true, true, 20);

        Assert.Equal(0, report.SensorsCreated);
        Assert.Equal(5, report.SensorsAlreadyExisted);
        Assert.Equal(0, report.RulesCreated);
        Assert.DoesNotContain(handler.Bodies, b => b.Url.EndsWith("api/alert-rules")); // não duplica regras
    }

    [Fact]
    public async Task Counts_failures_without_throwing()
    {
        var handler = new Stub(_ => HttpStatusCode.InternalServerError);

        var report = await new FleetRegistrar(new HttpClient(handler) { BaseAddress = new Uri("http://api/") })
            .RegisterAsync(new SensorFleet(3), true, false, 20);

        Assert.Equal(1, report.Failures);     // o dispositivo não pôde ser criado...
        Assert.Equal(0, report.SensorsCreated);
    }

    [Fact]
    public async Task Creates_the_device_first_returns_its_key_and_logs_in_as_admin_when_credentials_are_given()
    {
        var handler = new Stub(_ => HttpStatusCode.Created, bodyFor: url =>
            url.EndsWith("api/auth/login") ? """{"accessToken":"tok-123"}""" :
            url.EndsWith("api/devices") ? """{"device":{},"apiKey":"shk_abc"}""" : "{}");
        var fleet = new SensorFleet(6, sensorsPerDevice: 6);

        var report = await new FleetRegistrar(new HttpClient(handler) { BaseAddress = new Uri("http://api/") }, new AdminCredentials("a@b.c", "senha"))
            .RegisterAsync(fleet, false, false, 20);

        Assert.Equal(1, report.DevicesCreated);
        Assert.Equal("shk_abc", report.ApiKeys![fleet.Sensors[0].DeviceId]);
        Assert.Equal(6, report.SensorsCreated);

        var order = handler.Bodies.Select(b => b.Url).ToList();
        Assert.EndsWith("api/auth/login", order[0]);
        Assert.EndsWith("api/devices", order[1]);                                  // dispositivo ANTES dos sensores
        Assert.All(handler.Auth.Skip(1), a => Assert.Equal("Bearer tok-123", a)); // o token é usado nas chamadas seguintes
    }

    [Fact]
    public async Task A_429_is_waited_out_and_retried_instead_of_counted_as_a_failure()
    {
        var sensorCalls = 0;
        var handler = new Stub(url =>
            url.EndsWith("api/sensors") && Interlocked.Increment(ref sensorCalls) == 1 ? HttpStatusCode.TooManyRequests : HttpStatusCode.Created,
            retryAfterSeconds: 0);

        var report = await new FleetRegistrar(new HttpClient(handler) { BaseAddress = new Uri("http://api/") })
            .RegisterAsync(new SensorFleet(1), false, false, 20, parallelism: 1);

        Assert.Equal(1, report.SensorsCreated);
        Assert.Equal(0, report.Failures);
        Assert.Equal(2, handler.Bodies.Count(b => b.Url.EndsWith("api/sensors"))); // a primeira foi repetida
    }

    [Fact]
    public async Task An_existing_device_is_not_an_error_but_has_no_recoverable_key()
    {
        var handler = new Stub(url => url.EndsWith("api/devices") ? HttpStatusCode.Conflict : HttpStatusCode.Created);

        var report = await new FleetRegistrar(new HttpClient(handler) { BaseAddress = new Uri("http://api/") })
            .RegisterAsync(new SensorFleet(4), false, false, 20);

        Assert.Equal(1, report.DevicesAlreadyExisted);
        Assert.Empty(report.ApiKeys!);
        Assert.Equal(0, report.Failures);
    }

    private sealed class Stub(Func<string, HttpStatusCode> respond, Func<string, string>? bodyFor = null, int? retryAfterSeconds = null) : HttpMessageHandler
    {
        private readonly List<(string Url, string Body)> _bodies = [];
        private readonly List<string> _auth = [];
        public IReadOnlyList<string> Auth { get { lock (_bodies) return _auth.ToList(); } }
        public IReadOnlyList<(string Url, string Body)> Bodies { get { lock (_bodies) return _bodies.ToList(); } }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var url = request.RequestUri!.ToString();
            var body = request.Content is null ? "" : await request.Content.ReadAsStringAsync(cancellationToken);
            lock (_bodies) { _bodies.Add((url, body)); _auth.Add(request.Headers.Authorization?.ToString() ?? ""); }
            var response = new HttpResponseMessage(respond(url)) { Content = new StringContent(bodyFor?.Invoke(url) ?? "{}") };
            if (response.StatusCode == HttpStatusCode.TooManyRequests && retryAfterSeconds is { } seconds)
                response.Headers.RetryAfter = new System.Net.Http.Headers.RetryConditionHeaderValue(TimeSpan.FromSeconds(seconds));
            return response;
        }
    }
}
