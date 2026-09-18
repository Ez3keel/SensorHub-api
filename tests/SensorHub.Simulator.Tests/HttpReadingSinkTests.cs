using System.Net;
using System.Text.Json;
using SensorHub.Simulator.Load;

namespace SensorHub.Simulator.Tests;

public class HttpReadingSinkTests
{
    private static readonly SimulatedReading[] Batch =
    [
        new(Guid.NewGuid(), new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero), 21.5, "°C")
    ];

    [Fact]
    public async Task Posts_batch_as_camel_case_json_to_the_batch_endpoint()
    {
        var handler = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.Accepted));
        var sink = new HttpReadingSink(new HttpClient(handler) { BaseAddress = new Uri("http://api/") });

        await sink.SendAsync(Batch, default);

        var request = Assert.Single(handler.Requests);
        Assert.Equal("http://api/api/readings/batch", request.Uri);
        using var doc = JsonDocument.Parse(request.Body);
        var item = doc.RootElement[0];
        Assert.Equal(Batch[0].SensorId, item.GetProperty("sensorId").GetGuid());
        Assert.Equal(21.5, item.GetProperty("value").GetDouble());
        Assert.Equal("°C", item.GetProperty("unit").GetString());
    }

    [Fact]
    public async Task Sends_api_key_header_when_configured()
    {
        var handler = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.Accepted));
        var sink = new HttpReadingSink(new HttpClient(handler) { BaseAddress = new Uri("http://api/") }, "segredo");

        await sink.SendAsync(Batch, default);

        Assert.Equal("segredo", handler.Requests.Single().ApiKey);
    }

    [Fact]
    public async Task Retries_the_same_batch_on_503_honoring_retry_after()
    {
        var calls = 0;
        var handler = new StubHandler(_ =>
        {
            if (++calls < 3)
            {
                var busy = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
                busy.Headers.RetryAfter = new System.Net.Http.Headers.RetryConditionHeaderValue(TimeSpan.Zero);
                return busy;
            }
            return new HttpResponseMessage(HttpStatusCode.Accepted);
        });
        var sink = new HttpReadingSink(new HttpClient(handler) { BaseAddress = new Uri("http://api/") });

        await sink.SendAsync(Batch, default);

        Assert.Equal(3, handler.Requests.Count);
        Assert.Equal(2, sink.Retries);
        Assert.Single(handler.Requests.Select(r => r.Body).Distinct()); // o MESMO lote foi reenviado
    }

    [Fact]
    public async Task Gives_up_after_max_retries()
    {
        var handler = new StubHandler(_ =>
        {
            var busy = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
            busy.Headers.RetryAfter = new System.Net.Http.Headers.RetryConditionHeaderValue(TimeSpan.Zero);
            return busy;
        });
        var sink = new HttpReadingSink(new HttpClient(handler) { BaseAddress = new Uri("http://api/") }, maxRetries: 2);

        await Assert.ThrowsAsync<HttpRequestException>(() => sink.SendAsync(Batch, default));

        Assert.Equal(3, handler.Requests.Count); // 1 tentativa + 2 reenvios
    }

    [Fact]
    public async Task Does_not_retry_client_errors()
    {
        var handler = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.BadRequest));
        var sink = new HttpReadingSink(new HttpClient(handler) { BaseAddress = new Uri("http://api/") });

        await Assert.ThrowsAsync<HttpRequestException>(() => sink.SendAsync(Batch, default));

        Assert.Single(handler.Requests);
    }

    [Fact]
    public async Task Retries_on_network_failure()
    {
        var calls = 0;
        var handler = new StubHandler(_ => ++calls == 1
            ? throw new HttpRequestException("connection refused")
            : new HttpResponseMessage(HttpStatusCode.Accepted));
        var sink = new HttpReadingSink(new HttpClient(handler) { BaseAddress = new Uri("http://api/") });

        await sink.SendAsync(Batch, default);

        Assert.Equal(2, calls);
    }

    private sealed record Captured(string Uri, string Body, string? ApiKey);

    private sealed class StubHandler(Func<HttpRequestMessage, HttpResponseMessage> respond) : HttpMessageHandler
    {
        public List<Captured> Requests { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var body = request.Content is null ? "" : await request.Content.ReadAsStringAsync(cancellationToken);
            request.Headers.TryGetValues("X-Api-Key", out var key);
            Requests.Add(new Captured(request.RequestUri!.ToString(), body, key?.SingleOrDefault()));
            return respond(request);
        }
    }
}
