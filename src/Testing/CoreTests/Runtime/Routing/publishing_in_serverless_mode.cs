using System.Collections.Concurrent;
using JasperFx.Core;
using Microsoft.Extensions.Hosting;
using Wolverine.Configuration;
using Wolverine.Runtime;
using Wolverine.Tracking;
using Wolverine.Transports;
using Wolverine.Transports.Sending;
using Xunit;

namespace CoreTests.Runtime.Routing;

/// <summary>
///     Serverless mode removes the local transport, and the message router used to build the durable local queue
///     unconditionally, so every publish to an external endpoint threw UnknownTransportException for local://durable/.
/// </summary>
public class publishing_in_serverless_mode
{
    [Fact]
    public async Task publishing_to_an_inline_external_endpoint_works()
    {
        var transport = new ServerlessRecordingTransport();
        using var host = await Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Transports.Add(transport);
                opts.PublishMessage<ServerlessPing>().To(ServerlessRecordingTransport.Uri).SendInline();
            }).StartAsync(TestContext.Current.CancellationToken);

        await host.MessageBus().PublishAsync(new ServerlessPing("one"));

        transport.Sender.Sent.Count.ShouldBe(1);
    }

    [Fact]
    public async Task a_scheduled_send_the_transport_cannot_schedule_is_refused_with_an_explanation()
    {
        var transport = new ServerlessRecordingTransport();
        using var host = await Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Transports.Add(transport);
                opts.PublishMessage<ServerlessPing>().To(ServerlessRecordingTransport.Uri).SendInline();
            }).StartAsync(TestContext.Current.CancellationToken);

        var router = host.GetRuntime().RoutingFor(typeof(ServerlessPing));
        var agent = router.ShouldBeAssignableTo<Wolverine.Runtime.Routing.MessageRouterBase<ServerlessPing>>()!
            .LocalDurableQueue;

        var ex = await Should.ThrowAsync<NotSupportedException>(async () =>
            await agent.StoreAndForwardAsync(new Envelope(new ServerlessPing("later"))));
        ex.Message.ShouldContain("Serverless");

        // And the caller hears about it, rather than the message quietly going nowhere
        var publishFailure = await Should.ThrowAsync<NotSupportedException>(async () =>
            await host.MessageBus().PublishAsync(new ServerlessPing("later"),
                new DeliveryOptions { ScheduleDelay = 1.Hours() }));
        publishFailure.Message.ShouldContain("Serverless");
        transport.Sender.Sent.ShouldBeEmpty();
    }
}

public record ServerlessPing(string Name);

internal class ServerlessRecordingTransport : TransportBase<ServerlessRecordingEndpoint>
{
    public const string ProtocolName = "serverless-recording";
    public static readonly Uri Uri = new($"{ProtocolName}://one");

    public ServerlessRecordingTransport() : base(ProtocolName, "Serverless recording test transport", [ProtocolName])
    {
        Endpoint = new ServerlessRecordingEndpoint(Uri, Sender);
    }

    public ServerlessRecordingSender Sender { get; } = new(Uri);
    public ServerlessRecordingEndpoint Endpoint { get; }

    protected override IEnumerable<ServerlessRecordingEndpoint> endpoints() => [Endpoint];

    protected override ServerlessRecordingEndpoint findEndpointByUri(Uri uri) => Endpoint;
}

internal class ServerlessRecordingEndpoint : Endpoint
{
    private readonly ServerlessRecordingSender _sender;

    public ServerlessRecordingEndpoint(Uri uri, ServerlessRecordingSender sender) : base(uri, EndpointRole.Application)
    {
        _sender = sender;
    }

    protected override ISender CreateSender(IWolverineRuntime runtime) => _sender;

    public override ValueTask<IListener> BuildListenerAsync(IWolverineRuntime runtime, IReceiver receiver)
    {
        throw new NotSupportedException();
    }
}

internal class ServerlessRecordingSender : ISender
{
    public ServerlessRecordingSender(Uri destination) => Destination = destination;

    public ConcurrentQueue<Envelope> Sent { get; } = new();
    public bool SupportsNativeScheduledSend => false;
    public Uri Destination { get; }

    public Task<bool> PingAsync() => Task.FromResult(true);

    public ValueTask SendAsync(Envelope envelope)
    {
        Sent.Enqueue(envelope);
        return ValueTask.CompletedTask;
    }
}
