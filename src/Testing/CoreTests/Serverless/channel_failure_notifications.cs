using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Wolverine;
using Wolverine.Logging;
using Wolverine.Runtime;
using Wolverine.Transports;
using Wolverine.Transports.Sending;
using Xunit;

namespace CoreTests.Serverless;

public class channel_failure_notifications
{
    [Fact]
    public async Task recovery_path_reports_the_failure_before_acking()
    {
        var channel = new RecordingChannel();
        var envelope = new Envelope { Id = Guid.NewGuid(), Message = new object() };

        await HandlerPipeline.RecoverFromFailedProcessingAsync(channel, envelope, new DivideByZeroException(),
            Substitute.For<IMessageTracker>(), NullLogger.Instance, null);

        channel.Calls.ShouldBe(["ProcessingFailed", "CompleteAsync"]);
    }

    [Fact]
    public async Task failed_outgoing_send_is_reported_to_the_incoming_channel()
    {
        using var host = await Host.CreateDefaultBuilder()
            .UseWolverine(opts => opts.Discovery.DisableConventionalDiscovery())
            .StartAsync(TestContext.Current.CancellationToken);
        var runtime = host.Services.GetRequiredService<IWolverineRuntime>();

        var channel = new RecordingChannel();
        var context = new MessageContext(runtime);
        context.ReadEnvelope(new Envelope { Id = Guid.NewGuid(), Message = new object() }, channel);
        await context.EnlistInOutboxAsync(context);

        var failing = Substitute.For<ISendingAgent>();
        failing.IsDurable.Returns(false);
        failing.StoreAndForwardAsync(Arg.Any<Envelope>())
            .Returns(_ => ValueTask.FromException(new DivideByZeroException()));

        await context.PersistOrSendAsync(new Envelope { Message = new object(), Sender = failing });
        await context.FlushOutgoingMessagesAsync();

        channel.Calls.ShouldContain("OutgoingSendFailed");
    }

    [Fact]
    public async Task flush_failure_without_an_incoming_envelope_does_not_throw()
    {
        using var host = await Host.CreateDefaultBuilder()
            .UseWolverine(opts => opts.Discovery.DisableConventionalDiscovery())
            .StartAsync(TestContext.Current.CancellationToken);
        var runtime = host.Services.GetRequiredService<IWolverineRuntime>();

        var context = new MessageContext(runtime);
        await context.EnlistInOutboxAsync(context);

        var failing = Substitute.For<ISendingAgent>();
        failing.StoreAndForwardAsync(Arg.Any<Envelope>())
            .Returns(_ => ValueTask.FromException(new DivideByZeroException()));

        await context.PersistOrSendAsync(new Envelope { Message = new object(), Sender = failing });
        await context.FlushOutgoingMessagesAsync();
    }

    [Fact]
    public async Task a_throwing_channel_observer_does_not_stop_the_remaining_outgoing_sends()
    {
        using var host = await Host.CreateDefaultBuilder()
            .UseWolverine(opts => opts.Discovery.DisableConventionalDiscovery())
            .StartAsync(TestContext.Current.CancellationToken);
        var runtime = host.Services.GetRequiredService<IWolverineRuntime>();

        var channel = new RecordingChannel { ThrowOnOutgoingSendFailed = true };
        var context = new MessageContext(runtime);
        context.ReadEnvelope(new Envelope { Id = Guid.NewGuid(), Message = new object() }, channel);
        await context.EnlistInOutboxAsync(context);

        var first = Substitute.For<ISendingAgent>();
        first.IsDurable.Returns(false);
        first.StoreAndForwardAsync(Arg.Any<Envelope>())
            .Returns(_ => ValueTask.FromException(new DivideByZeroException()));

        var second = Substitute.For<ISendingAgent>();
        second.IsDurable.Returns(false);
        second.StoreAndForwardAsync(Arg.Any<Envelope>())
            .Returns(_ => ValueTask.FromException(new DivideByZeroException()));

        await context.PersistOrSendAsync(new Envelope { Message = new object(), Sender = first });
        await context.PersistOrSendAsync(new Envelope { Message = new object(), Sender = second });

        await context.FlushOutgoingMessagesAsync();

        await first.Received(1).StoreAndForwardAsync(Arg.Any<Envelope>());
        await second.Received(1).StoreAndForwardAsync(Arg.Any<Envelope>());
    }

    private class RecordingChannel : IChannelCallback, IObserveChannelFailures
    {
        public List<string> Calls { get; } = new();
        public bool ThrowOnOutgoingSendFailed { get; init; }
        public IHandlerPipeline? Pipeline => null;

        public ValueTask CompleteAsync(Envelope envelope)
        {
            Calls.Add("CompleteAsync");
            return ValueTask.CompletedTask;
        }

        public ValueTask DeferAsync(Envelope envelope)
        {
            Calls.Add("DeferAsync");
            return ValueTask.CompletedTask;
        }

        public void OutgoingSendFailed(Envelope incoming, Envelope outgoing, Exception exception)
        {
            Calls.Add("OutgoingSendFailed");
            if (ThrowOnOutgoingSendFailed) throw new InvalidOperationException("observer failed");
        }

        public void ProcessingFailed(Envelope envelope, Exception exception) => Calls.Add("ProcessingFailed");
    }
}
