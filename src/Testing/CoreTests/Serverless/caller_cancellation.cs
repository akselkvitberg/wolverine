using JasperFx.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Wolverine;
using Wolverine.ErrorHandling;
using Wolverine.Runtime;
using Wolverine.Transports;
using Xunit;

namespace CoreTests.Serverless;

public class caller_cancellation
{
    private static async Task<(IHost, HandlerPipeline)> buildAsync()
    {
        var host = await Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Discovery.DisableConventionalDiscovery()
                .IncludeType<WaitsForeverHandler>()
                .IncludeType<ThrowsCancelledHandler>()
                .IncludeType<FailsOnceThenWaitsHandler>();
                opts.Policies.OnException<InvalidOperationException>().RetryTimes(2);
            })
            .StartAsync(TestContext.Current.CancellationToken);

        var runtime = (WolverineRuntime)host.Services.GetRequiredService<IWolverineRuntime>();
        return (host, new HandlerPipeline(runtime, runtime));
    }

    [Fact]
    public async Task cancelled_caller_token_leaves_the_envelope_unsettled()
    {
        var (host, pipeline) = await buildAsync();
        using var _ = host;

        var channel = new SettlementRecorder();
        var envelope = new Envelope(new WaitsForever()) { Destination = new Uri("stub://push") };
        using var cts = new CancellationTokenSource(250.Milliseconds());

        await pipeline.InvokeAsync(envelope, channel, null, cts.Token);

        // no CompleteAsync, no DeferAsync, no dead letter: the failure rules never ran
        channel.Calls.ShouldBeEmpty();
    }

    [Fact]
    public async Task already_cancelled_caller_token_never_starts_the_handler()
    {
        var (host, pipeline) = await buildAsync();
        using var _ = host;

        var channel = new SettlementRecorder();
        var envelope = new Envelope(new WaitsForever()) { Destination = new Uri("stub://push") };

        await pipeline.InvokeAsync(envelope, channel, null, new CancellationToken(true));

        envelope.Attempts.ShouldBe(0);
        channel.Calls.ShouldBeEmpty();
    }

    [Fact]
    public async Task an_operation_cancelled_exception_without_caller_cancellation_still_runs_failure_rules()
    {
        var (host, pipeline) = await buildAsync();
        using var _ = host;

        var channel = new SettlementRecorder();
        var envelope = new Envelope(new ThrowsCancelled()) { Destination = new Uri("stub://push") };

        await pipeline.InvokeAsync(envelope, channel, null, CancellationToken.None);

        // default rules end in MoveToErrorQueue, which completes the envelope on this channel
        channel.Calls.ShouldContain("CompleteAsync");
    }

    [Fact]
    public async Task cancelled_caller_token_also_covers_inline_retry_attempts()
    {
        var (host, pipeline) = await buildAsync();
        using var _ = host;

        FailsOnceThenWaitsHandler.Attempts = 0;
        var channel = new SettlementRecorder();
        var envelope = new Envelope(new FailsOnceThenWaits()) { Destination = new Uri("stub://push") };
        using var cts = new CancellationTokenSource(250.Milliseconds());

        await pipeline.InvokeAsync(envelope, channel, null, cts.Token);

        // attempt 1 threw and RetryNow ran attempt 2 inside the same call; attempt 2 saw the caller abort,
        // so nothing was settled and no failure rule ran
        FailsOnceThenWaitsHandler.Attempts.ShouldBe(2);
        channel.Calls.ShouldBeEmpty();
    }

    private class SettlementRecorder : IChannelCallback
    {
        public List<string> Calls { get; } = new();
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
    }
}

public record WaitsForever;
public record ThrowsCancelled;
public record FailsOnceThenWaits;

public class WaitsForeverHandler
{
    public static Task Handle(WaitsForever _, CancellationToken token) => Task.Delay(Timeout.Infinite, token);
}

public class FailsOnceThenWaitsHandler
{
    public static int Attempts;

    public static Task Handle(FailsOnceThenWaits _, CancellationToken token)
    {
        if (Interlocked.Increment(ref Attempts) == 1)
        {
            throw new InvalidOperationException("first attempt fails");
        }

        return Task.Delay(Timeout.Infinite, token);
    }
}

public class ThrowsCancelledHandler
{
    public static void Handle(ThrowsCancelled _) => throw new OperationCanceledException();
}
