using JasperFx.Core;
using Microsoft.Extensions.Hosting;
using Wolverine;
using Wolverine.Runtime.Routing;
using Xunit;

namespace CoreTests.Runtime.Routing;

// Serverless removes the local queues, so a message type whose only route would be a local queue has no
// route at all. Publishing, cascading or scheduling such a message must fail with an explanation rather
// than record "no routes" and drop it, as happens for a message type that nothing handles.
public class local_handling_in_serverless_mode
{
    private static Task<IHost> startServerlessHostAsync(Action<WolverineOptions>? configure = null)
    {
        return Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Discovery.DisableConventionalDiscovery()
                    .IncludeType(typeof(ServerlessLocalHandler))
                    .IncludeType(typeof(ServerlessLocalSaga));

                configure?.Invoke(opts);
            }).StartAsync();
    }

    private static void shouldExplainServerless(Exception ex, Type messageType)
    {
        ex.Message.ShouldContain(nameof(DurabilityMode.Serverless));
        ex.Message.ShouldContain(messageType.FullName!);
    }

    [Fact]
    public async Task invoking_a_locally_handled_message_still_works()
    {
        using var host = await startServerlessHostAsync();

        await host.MessageBus().InvokeAsync(new ServerlessLocalMessage(Guid.NewGuid()), TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task publishing_a_locally_handled_message_explains_why_it_cannot_be_delivered()
    {
        using var host = await startServerlessHostAsync();

        var ex = await Should.ThrowAsync<IndeterminateRoutesException>(() =>
            host.MessageBus().PublishAsync(new ServerlessLocalMessage(Guid.NewGuid())).AsTask());

        shouldExplainServerless(ex, typeof(ServerlessLocalMessage));
    }

    [Fact]
    public async Task sending_a_locally_handled_message_explains_why_it_cannot_be_delivered()
    {
        using var host = await startServerlessHostAsync();

        var ex = await Should.ThrowAsync<IndeterminateRoutesException>(() =>
            host.MessageBus().SendAsync(new ServerlessLocalMessage(Guid.NewGuid())).AsTask());

        shouldExplainServerless(ex, typeof(ServerlessLocalMessage));
    }

    [Fact]
    public async Task scheduling_a_locally_handled_message_explains_why_it_cannot_be_delivered()
    {
        using var host = await startServerlessHostAsync();

        var ex = await Should.ThrowAsync<IndeterminateRoutesException>(() =>
            host.MessageBus().ScheduleAsync(new ServerlessLocalMessage(Guid.NewGuid()), 1.Hours()).AsTask());

        shouldExplainServerless(ex, typeof(ServerlessLocalMessage));
    }

    [Fact]
    public async Task cascading_a_locally_handled_message_explains_why_it_cannot_be_delivered()
    {
        using var host = await startServerlessHostAsync();

        var ex = await Should.ThrowAsync<IndeterminateRoutesException>(() =>
            host.MessageBus().InvokeAsync(new ServerlessCascadeStart(Guid.NewGuid()), TestContext.Current.CancellationToken));

        shouldExplainServerless(ex, typeof(ServerlessLocalMessage));
    }

    [Fact]
    public async Task saga_timeout_explains_why_it_cannot_be_delivered()
    {
        using var host = await startServerlessHostAsync();

        var ex = await Should.ThrowAsync<IndeterminateRoutesException>(() =>
            host.MessageBus().InvokeAsync(new StartServerlessLocalSaga(Guid.NewGuid()), TestContext.Current.CancellationToken));

        shouldExplainServerless(ex, typeof(ServerlessLocalSagaTimeout));
    }

    [Fact]
    public async Task publishing_with_an_explicit_local_queue_route_explains_why_it_cannot_be_delivered()
    {
        using var host = await startServerlessHostAsync(opts =>
            opts.PublishMessage<ServerlessLocalMessage>().ToLocalQueue("serverless"));

        var ex = await Should.ThrowAsync<IndeterminateRoutesException>(() =>
            host.MessageBus().PublishAsync(new ServerlessLocalMessage(Guid.NewGuid())).AsTask());

        shouldExplainServerless(ex, typeof(ServerlessLocalMessage));
    }

    [Fact]
    public async Task publishing_a_message_nothing_handles_or_routes_is_still_a_no_op()
    {
        using var host = await startServerlessHostAsync();

        await host.MessageBus().PublishAsync(new ServerlessUnhandledMessage(Guid.NewGuid()));
    }
}

public record ServerlessLocalMessage(Guid Id);

public record ServerlessCascadeStart(Guid Id);

public record ServerlessUnhandledMessage(Guid Id);

public class ServerlessLocalHandler
{
    public void Handle(ServerlessLocalMessage message)
    {
    }

    public ServerlessLocalMessage Handle(ServerlessCascadeStart start) => new(start.Id);
}

public record StartServerlessLocalSaga(Guid Id);

public record ServerlessLocalSagaTimeout(Guid Id) : TimeoutMessage(1.Hours());

public class ServerlessLocalSaga : Saga
{
    public Guid Id { get; set; }

    public static (ServerlessLocalSaga, ServerlessLocalSagaTimeout) Start(StartServerlessLocalSaga start)
        => (new ServerlessLocalSaga { Id = start.Id }, new ServerlessLocalSagaTimeout(start.Id));

    public void Handle(ServerlessLocalSagaTimeout timeout) => MarkCompleted();
}
