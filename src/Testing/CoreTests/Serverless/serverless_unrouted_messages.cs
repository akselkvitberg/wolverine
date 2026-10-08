using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Wolverine;
using Wolverine.Runtime;
using Wolverine.Runtime.Routing;
using Xunit;

namespace CoreTests.Serverless;

public class serverless_unrouted_messages
{
    [Fact]
    public async Task cascading_a_locally_handled_type_without_a_route_throws()
    {
        using var host = await Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Discovery.DisableConventionalDiscovery()
                    .IncludeType<LocalOnlyStartHandler>()
                    .IncludeType<LocalOnlyHandler>();
            })
            .StartAsync(TestContext.Current.CancellationToken);

        var ex = await Should.ThrowAsync<NoExternalRouteInServerlessException>(async () =>
            await host.MessageBus().InvokeAsync(new LocalOnlyStart(), TestContext.Current.CancellationToken));

        ex.MessageType.ShouldBe(typeof(LocalOnly));
    }

    [Fact]
    public async Task publishing_a_type_with_no_handler_and_no_route_still_does_not_throw()
    {
        using var host = await Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Discovery.DisableConventionalDiscovery();
            })
            .StartAsync(TestContext.Current.CancellationToken);

        await host.MessageBus().PublishAsync(new NobodyHandlesThis());
    }

    [Fact]
    public async Task publishing_a_system_message_type_with_a_local_handler_does_not_throw()
    {
        using var host = await Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Discovery.DisableConventionalDiscovery()
                    .IncludeType<ServerlessInternalHandler>();
            })
            .StartAsync(TestContext.Current.CancellationToken);

        await host.MessageBus().PublishAsync(new ServerlessInternal());
    }

    [Fact]
    public async Task outside_serverless_the_behaviour_is_unchanged()
    {
        using var host = await Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Discovery.DisableConventionalDiscovery()
                    .IncludeType<LocalOnlyStartHandler>()
                    .IncludeType<LocalOnlyHandler>();
            })
            .StartAsync(TestContext.Current.CancellationToken);

        await host.MessageBus().InvokeAsync(new LocalOnlyStart(), TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task startup_warns_about_cascaded_local_types_without_a_route()
    {
        var logs = new CapturingLoggerProvider();

        using var host = await Host.CreateDefaultBuilder()
            .ConfigureLogging(x => x.AddProvider(logs))
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Discovery.DisableConventionalDiscovery()
                    .IncludeType<LocalOnlyStartHandler>()
                    .IncludeType<LocalOnlyHandler>();
            })
            .StartAsync(TestContext.Current.CancellationToken);

        logs.Messages.ShouldContain(m => m.Contains("Serverless mode") && m.Contains(typeof(LocalOnly).FullName!));
    }
}

public record LocalOnlyStart;
public record LocalOnly;
public record NobodyHandlesThis;
public record ServerlessInternal : IInternalMessage;

public class LocalOnlyStartHandler
{
    public static LocalOnly Handle(LocalOnlyStart _) => new();
}

public class LocalOnlyHandler
{
    public static void Handle(LocalOnly _)
    {
    }
}

public class ServerlessInternalHandler
{
    public static void Handle(ServerlessInternal _)
    {
    }
}

public class CapturingLoggerProvider : ILoggerProvider
{
    public ConcurrentQueue<string> Messages { get; } = new();

    public ILogger CreateLogger(string categoryName) => new CapturingLogger(Messages);

    public void Dispose()
    {
    }

    private class CapturingLogger(ConcurrentQueue<string> messages) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (IsEnabled(logLevel)) messages.Enqueue(formatter(state, exception));
        }
    }
}
