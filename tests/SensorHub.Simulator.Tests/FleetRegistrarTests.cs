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

        Assert.Equal(3, report.Failures);
    }

    private sealed class Stub(Func<string, HttpStatusCode> respond) : HttpMessageHandler
    {
        private readonly List<(string Url, string Body)> _bodies = [];
        public IReadOnlyList<(string Url, string Body)> Bodies { get { lock (_bodies) return _bodies.ToList(); } }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var url = request.RequestUri!.ToString();
            var body = request.Content is null ? "" : await request.Content.ReadAsStringAsync(cancellationToken);
            lock (_bodies) _bodies.Add((url, body));
            return new HttpResponseMessage(respond(url));
        }
    }
}
