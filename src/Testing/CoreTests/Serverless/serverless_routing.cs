using JasperFx.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Wolverine;
using Wolverine.Runtime;
using Wolverine.Tracking;
using Wolverine.Transports.SharedMemory;
using Xunit;

namespace CoreTests.Serverless;

public class serverless_routing
{
    private static Task<IHost> startServerlessHostAsync(string topic) =>
        Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Discovery.DisableConventionalDiscovery()
                    .IncludeType<ServerlessStartHandler>()
                    .IncludeType<ServerlessDelayedStartHandler>();
                opts.PublishMessage<ServerlessExternal>().ToSharedMemoryTopic(topic);
            })
            .StartAsync(TestContext.Current.CancellationToken);

    [Fact]
    public async Task host_starts_and_routes_every_handled_type()
    {
        // Startup pre-populates routing for every message type in Serverless now. Before the fix,
        // building any router threw UnknownTransportException for local://durable.
        using var host = await startServerlessHostAsync($"serverless-{Guid.NewGuid():N}");

        var runtime = (WolverineRuntime)host.Services.GetRequiredService<IWolverineRuntime>();
        runtime.RoutingFor(typeof(ServerlessExternal)).Routes.Length.ShouldBe(1);
    }

    [Fact]
    public async Task cascade_to_an_external_route_is_sent()
    {
        using var host = await startServerlessHostAsync($"serverless-{Guid.NewGuid():N}");

        var session = await host.TrackActivity()
            .InvokeMessageAndWaitAsync(new ServerlessStart());

        session.Sent.SingleMessage<ServerlessExternal>().ShouldNotBeNull();
    }

    [Fact]
    public async Task scheduled_send_throws_from_the_publish_call()
    {
        using var host = await startServerlessHostAsync($"serverless-{Guid.NewGuid():N}");

        var ex = await Should.ThrowAsync<InvalidOperationException>(async () =>
            await host.MessageBus().ScheduleAsync(new ServerlessExternal(), 5.Minutes()));

        ex.Message.ShouldContain("Scheduled or delayed delivery is not supported in Serverless mode");
    }

    [Fact]
    public async Task scheduled_cascade_fails_the_handler_instead_of_being_discarded()
    {
        using var host = await startServerlessHostAsync($"serverless-{Guid.NewGuid():N}");

        // InvokeAsync rethrows when no inline continuation applies, which proves the error came out of the
        // handler (where failure policies run) rather than being logged and discarded by the flush
        var ex = await Should.ThrowAsync<InvalidOperationException>(async () =>
            await host.MessageBus().InvokeAsync(new ServerlessDelayedStart()));
        ex.Message.ShouldContain("Scheduled or delayed delivery is not supported in Serverless mode");
    }
}

public record ServerlessStart;
public record ServerlessDelayedStart;
public record ServerlessExternal;

public class ServerlessStartHandler
{
    public static ServerlessExternal Handle(ServerlessStart _) => new();
}

public class ServerlessDelayedStartHandler
{
    public static object Handle(ServerlessDelayedStart _) => new ServerlessExternal().DelayedFor(5.Minutes());
}
