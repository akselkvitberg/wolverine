using CoreTests.Runtime.WorkerQueues;
using JasperFx.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shouldly;
using Wolverine;
using Wolverine.Configuration;
using Wolverine.ErrorHandling;
using Wolverine.Runtime;
using Wolverine.Tracking;
using Wolverine.Transports.SharedMemory;
using Xunit;

namespace CoreTests.ErrorHandling;

/// <summary>
/// Serverless forces every endpoint Inline and never starts the in-memory scheduled job processor, so with no
/// message store and a listener that cannot schedule natively there is nothing that can hold a message until
/// a ScheduleRetry delay is up. The rescheduled envelope used to be handed to
/// <c>NullMessageStore.RescheduleExistingEnvelopeForRetryAsync</c>, whose <c>ScheduledJobs?.Enqueue</c> is a
/// no-op when the processor was never created.
///
/// <para>
/// The SharedMemory transport stands in for a broker here: it runs in Serverless and does not implement native
/// scheduling. Its <c>CompleteAsync</c> does nothing, so before the fix it behaved like a listener that acks on
/// receipt (Inline Pub/Sub) and the message was lost. Its <c>DeferAsync</c> re-posts the envelope to the same
/// listener, as the RabbitMQ listener's <c>DeferAsync</c> re-sends a copy to its queue.
/// </para>
/// </summary>
public class schedule_retry_in_serverless_mode : IAsyncLifetime
{
    private readonly RecordingLoggerProvider theLogs = new();
    private IHost theHost = null!;
    private string theTopic = null!;

    public async ValueTask InitializeAsync()
    {
        ServerlessRetryHandler.Reset();
        theTopic = $"serverless-retry-{Guid.NewGuid():N}";

        theHost = await Host.CreateDefaultBuilder()
            .ConfigureLogging(logging => logging.AddProvider(theLogs))
            .UseWolverine(opts =>
            {
                opts.ApplicationAssembly = typeof(schedule_retry_in_serverless_mode).Assembly;
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Discovery.DisableConventionalDiscovery().IncludeType<ServerlessRetryHandler>();

                opts.OnException<ServerlessRetryException>().ScheduleRetry(50.Milliseconds());

                opts.PublishAllMessages().ToSharedMemoryTopic(theTopic);
                opts.ListenToSharedMemorySubscription(theTopic, "receiver");
            }).StartAsync(TestContext.Current.CancellationToken);

        // Serverless never auto-starts listeners; the function trigger or push adapter does it. Start this one
        // the same way, so it gets the receiver Serverless builds for it: Inline.
        var runtime = (WolverineRuntime)theHost.GetRuntime();
        var endpoint = runtime.Endpoints.EndpointFor(new Uri($"shared-memory://{theTopic}/receiver"))!;
        await runtime.Endpoints.StartListenerAsync(endpoint, TestContext.Current.CancellationToken);
        endpoint.Mode.ShouldBe(EndpointMode.Inline);
    }

    public async ValueTask DisposeAsync()
    {
        await theHost.StopAsync();
        theHost.Dispose();
    }

    // Sent straight to the topic rather than through PublishAsync: in Serverless the message router's
    // constructor asks for a local://durable sending agent, and the local transport has been removed.
    private ValueTask sendAsync(ServerlessRetryMessage message)
    {
        return theHost.MessageBus().EndpointFor(new Uri($"shared-memory://{theTopic}")).SendAsync(message);
    }

    private static async Task<bool> waitForSuccessAsync()
    {
        try
        {
            await ServerlessRetryHandler.Succeeded.Task.WaitAsync(10.Seconds(), TestContext.Current.CancellationToken);
            return true;
        }
        catch (TimeoutException)
        {
            return false;
        }
    }

    [Fact]
    public async Task a_message_that_hits_a_schedule_retry_is_retried()
    {
        await sendAsync(new ServerlessRetryMessage(FailuresBeforeSuccess: 1));

        var succeeded = await waitForSuccessAsync();

        succeeded.ShouldBeTrue(
            $"The handler ran {ServerlessRetryHandler.Calls} time(s) and the message was never retried");
        ServerlessRetryHandler.Calls.ShouldBe(2);
    }

    [Fact]
    public async Task the_fallback_is_logged_as_an_error_when_it_happens()
    {
        await sendAsync(new ServerlessRetryMessage(FailuresBeforeSuccess: 1));

        await waitForSuccessAsync();

        theLogs.Records.ShouldContain(
            x => x.Level == LogLevel.Error && x.Message.Contains("cannot honor a scheduled retry"),
            string.Join(Environment.NewLine, theLogs.Records.Select(x => $"{x.Level}: {x.Message}")));
    }

    [Fact]
    public void a_warning_is_logged_at_startup_when_schedule_retry_is_configured()
    {
        theLogs.Records.ShouldContain(
            x => x.Level == LogLevel.Warning && x.Message.Contains("ScheduleRetry")
                                             && x.Message.Contains(nameof(DurabilityMode.Serverless)),
            string.Join(Environment.NewLine, theLogs.Records.Select(x => $"{x.Level}: {x.Message}")));
    }
}

public record ServerlessRetryMessage(int FailuresBeforeSuccess);

public class ServerlessRetryException : Exception;

public class ServerlessRetryHandler
{
    private static int _calls;

    public static int Calls => Volatile.Read(ref _calls);

    public static TaskCompletionSource Succeeded { get; private set; } =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public static void Reset()
    {
        Interlocked.Exchange(ref _calls, 0);
        Succeeded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public static void Handle(ServerlessRetryMessage message)
    {
        var call = Interlocked.Increment(ref _calls);
        if (call <= message.FailuresBeforeSuccess)
        {
            throw new ServerlessRetryException();
        }

        Succeeded.TrySetResult();
    }
}
