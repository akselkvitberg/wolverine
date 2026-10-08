# Serverless Core Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `DurabilityMode.Serverless` publish, cascade and fail correctly, and give channels the hooks Pub/Sub push delivery needs to settle a request from the whole pipeline run.

**Architecture:**
- Five independent changes in `src/Wolverine` core, each with its own `CoreTests` coverage:
  - a lazy `local://durable` agent;
  - a hard error for unrouted local messages in Serverless;
  - an optional channel interface for failure notifications;
  - a per-call cancellation token on the handler pipeline;
  - a verified in-request send-retry path.
- Nothing here references Pub/Sub. This is PR 1 of the three in spec §8.0.

**Tech Stack:** C# 12, .NET 9/10, xunit v3, Shouldly, NSubstitute, Wolverine tracked sessions.

**Spec:** `docs/superpowers/specs/2026-10-08-pubsub-push-delivery-design.md` (§2 findings, §6.1–§6.4, §8.0, §8.2 CoreTests). Read §4's settlement rules too; the hooks here exist to serve them.

## Global Constraints

- Target frameworks: `net9.0;net10.0`. `LangVersion` 12. `TreatWarningsAsErrors` is on, so new public members need XML docs.
- Member casing follows accessibility:
  - `public` and `internal` members use PascalCase;
  - `private` and `protected` members use camelCase;
  - fields use `_camelCase`.
- Do **not** edit `CHANGELOG.md`.
- CoreTests uses xunit v3:
  - pass `TestContext.Current.CancellationToken` to `StartAsync` and other cancellable calls (analyzer xUnit1051 is an error);
  - Shouldly is a global using.
- Get the message bus with `host.MessageBus()`, never `host.Services.GetRequiredService<IMessageBus>()`.
- Gate before pushing: `dotnet build wolverine.slnx -c Release -f net9.0`.
- Build with `MSBUILDDISABLENODEREUSE=1`.
- Behaviour for every mode other than Serverless must not change, except the per-call cancellation in Task 5. That applies only when a caller passes a token.

## Review Focus

1. **A scheduled cascade in Serverless** (handler returns `new Msg().DelayedFor(5.Minutes())` routed to an external topic). The error must come out of the handler (failure policies run), not be swallowed by the flush. Test added in Task 2.
2. **Runtime shutdown cancellation** must still run failure rules on Buffered and Durable receivers. Only a cancelled *caller* token skips them. Test added in Task 5.
3. **System/internal message types with local handlers in Serverless** (e.g. reply or agent messages) must not start throwing the new "no external route" error. Test added in Task 3.
4. **`IObserveChannelFailures` on a channel when `MessageContext.Envelope` is null** (a bus used outside a handler) must not throw a `NullReferenceException` in the flush catch. Test added in Task 4.
5. **A Serverless host whose handlers publish a type with no handler and no route** must still start and publish as today (information log, no throw). Test added in Task 3.

---

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `src/Wolverine/Runtime/Routing/LazyLocalDurableSendingAgent.cs` | Create | `ISendingAgent` that resolves `local://durable` only when asked to; `Resolve(Envelope)` gives a clear Serverless error |
| `src/Wolverine/Runtime/Routing/ScheduledSendFallback.cs` | Create | One place that resolves the local durable queue for a scheduled send, or throws the Serverless error |
| `src/Wolverine/Runtime/Routing/MessageRouterBase.cs` | Modify L35-36 | Use the lazy agent |
| `src/Wolverine/Runtime/Routing/MessageRoute.cs` | Modify L243-247 | Resolve the lazy agent before `ForScheduledSend` |
| `src/Wolverine/Runtime/DestinationEndpoint.cs` | Modify L55-60, L101-106 | Use `ScheduledSendFallback` |
| `src/Wolverine/Runtime/Routing/NoExternalRouteInServerlessException.cs` | Create | Error for a locally handled type with no route in Serverless |
| `src/Wolverine/Runtime/MessageBus.cs` | Modify L317-323 | Throw it from `PublishAsync` |
| `src/Wolverine/Runtime/WolverineRuntime.HostService.cs` | Modify L316-347 | Pre-populate routing in Serverless; warn about unrouted cascade types |
| `src/Wolverine/Transports/IChannelCallback.cs` | Modify (append) | `IObserveChannelFailures` |
| `src/Wolverine/Runtime/MessageContext.cs` | Modify flush catch L251-261, `ClearState` L926-943 | Notify the observer; carry `CallerCancellation` |
| `src/Wolverine/Runtime/HandlerPipeline.cs` | Modify | Notify the observer in recovery; 4-arg `InvokeAsync` with a caller token |
| `src/Wolverine/Runtime/Handlers/Executor.cs` | Modify catch L263-274 | Skip failure rules for caller cancellation |
| `src/Wolverine/Runtime/Handlers/TracingExecutor.cs` | Modify catch L187-198 | Same as Executor |
| `src/Wolverine/Runtime/WorkerQueues/InlineReceiver.cs` | Modify | Internal `ReceivedAsync(listener, envelopes, token)` |
| `src/Testing/CoreTests/Serverless/*.cs` | Create | Tests for every task |

---

### Task 1: Verify that `RetryBlockSync` surfaces the final send failure

Spec §6.3 has two branches, and this task picks one. JasperFx 2.81.0's `RetryBlockSync<T>` should rethrow after its last attempt; this test pins that.
- **If the test passes:** branch 1 applies. No production change is needed here, and Task 4's flush hook covers send failures.
- **If it fails:** do Step 3b before moving on.

**Files:**
- Create: `src/Testing/CoreTests/Serverless/inline_sending_agent_sync_retry.cs`
- Modify (only if Step 2 fails): `src/Wolverine/Transports/Sending/InlineSendingAgent.cs`

**Interfaces:**
- Consumes: `InlineSendingAgent(ILogger, ISender, Endpoint, IMessageTracker, DurabilitySettings)` (public ctor, `InlineSendingAgent.cs:19-22`), `DurabilitySettings.UseSyncRetryBlock`.
- Produces: the fact Task 4 relies on: with `UseSyncRetryBlock = true`, `ISendingAgent.StoreAndForwardAsync` throws the final send exception.

- [ ] **Step 1: Write the characterization test**

```csharp
using JasperFx.Core;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Wolverine;
using Wolverine.Configuration;
using Wolverine.Logging;
using Wolverine.Runtime;
using Wolverine.Transports;
using Wolverine.Transports.Sending;
using Xunit;

namespace CoreTests.Serverless;

public class inline_sending_agent_sync_retry
{
    [Fact]
    public async Task final_send_failure_is_rethrown_to_the_caller_when_sync_retry_is_on()
    {
        var sender = Substitute.For<ISender>();
        sender.Destination.Returns(new Uri("failing://one"));
        sender.SendAsync(Arg.Any<Envelope>())
            .Returns(_ => ValueTask.FromException(new DivideByZeroException()));

        var agent = new InlineSendingAgent(NullLogger.Instance, sender, new FailingEndpoint(),
            Substitute.For<IMessageTracker>(), new DurabilitySettings { UseSyncRetryBlock = true });

        await Should.ThrowAsync<DivideByZeroException>(async () =>
            await agent.StoreAndForwardAsync(new Envelope { Message = new object() }));

        // RetryBlockSync's default Pauses has four entries: one attempt plus three retries, all in the caller
        await sender.Received(4).SendAsync(Arg.Any<Envelope>());
    }

    private class FailingEndpoint : Endpoint
    {
        public FailingEndpoint() : base(new Uri("failing://one"), EndpointRole.Application)
        {
        }

        public override ValueTask<IListener> BuildListenerAsync(IWolverineRuntime runtime, IReceiver receiver)
            => throw new NotSupportedException();

        protected override ISender CreateSender(IWolverineRuntime runtime) => throw new NotSupportedException();

        protected override bool supportsMode(EndpointMode mode) => true;
    }
}
```

- [ ] **Step 2: Run it**

Run: `dotnet test src/Testing/CoreTests/CoreTests.csproj -f net9.0 --filter "FullyQualifiedName~inline_sending_agent_sync_retry"`
Expected: PASS. If the attempt count differs but the exception is thrown, change `Received(4)` to the observed count and note it in the commit message. The rethrow is what matters.

- [ ] **Step 3a (test passed): Commit**

```bash
git add src/Testing/CoreTests/Serverless/inline_sending_agent_sync_retry.cs
git commit -m "Pin that RetryBlockSync rethrows the final inline send failure"
```

- [ ] **Step 3b (only if no exception was thrown): add a direct-send mode to `InlineSendingAgent`**

In `InlineSendingAgent`, add a field and change the constructor so that `UseSyncRetryBlock` sends directly, with an in-caller retry that rethrows:

```csharp
    private readonly Func<Envelope, CancellationToken, Task>? _directSend;

    // in the 7-arg constructor, replace the UseSyncRetryBlock branch with:
        if (settings.UseSyncRetryBlock)
        {
            // Spec §6.3 branch 2: RetryBlockSync swallows the final failure, so send directly and rethrow it
            _directSend = RetryHandlerResolver(endpoint);
            _sending = new RetryBlock<Envelope>(RetryHandlerResolver(endpoint), logger, _settings.Cancellation);
        }

    // and EnqueueOutgoingAsync becomes:
    public async ValueTask EnqueueOutgoingAsync(Envelope envelope)
    {
        setDefaults(envelope);

        if (_directSend != null)
        {
            await sendDirectlyAsync(envelope);
        }
        else
        {
            await _sending.PostAsync(envelope);
        }

        LastMessageSentAt = DateTimeOffset.UtcNow;
    }

    private static readonly TimeSpan[] DirectSendPauses = [TimeSpan.Zero, 50.Milliseconds(), 100.Milliseconds(), 250.Milliseconds()];

    private async Task sendDirectlyAsync(Envelope envelope)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await _directSend!(envelope, _settings.Cancellation);
                return;
            }
            catch (Exception e) when (attempt < DirectSendPauses.Length && e is not OperationCanceledException)
            {
                _logger.LogInformation(e, "Retrying inline send of {EnvelopeId} after {Attempts} attempts", envelope.Id, attempt);
                await Task.Delay(DirectSendPauses[attempt], _settings.Cancellation);
            }
        }
    }
```

Add `using JasperFx.Core;` for `Milliseconds()`. Re-run Step 2 (expected PASS), then commit both files with message `Send inline directly under UseSyncRetryBlock so the final failure reaches the caller`.

---

### Task 2: Resolve `local://durable` only for scheduled sends (spec §6.1)

**Files:**
- Create: `src/Wolverine/Runtime/Routing/ScheduledSendFallback.cs`
- Create: `src/Wolverine/Runtime/Routing/LazyLocalDurableSendingAgent.cs`
- Modify: `src/Wolverine/Runtime/Routing/MessageRouterBase.cs:35-36`
- Modify: `src/Wolverine/Runtime/Routing/MessageRoute.cs:243-247`
- Modify: `src/Wolverine/Runtime/DestinationEndpoint.cs:55-60, 101-106`
- Modify: `src/Wolverine/Runtime/WolverineRuntime.HostService.cs:316-347`
- Test: `src/Testing/CoreTests/Serverless/serverless_routing.cs`

**Interfaces:**
- Produces:
  - `internal static class ScheduledSendFallback { public static ISendingAgent LocalDurableQueueFor(IWolverineRuntime runtime, Envelope envelope); }`
  - `internal sealed class LazyLocalDurableSendingAgent : ISendingAgent { public LazyLocalDurableSendingAgent(IWolverineRuntime runtime); public ISendingAgent Resolve(Envelope envelope); }`
  - Error text prefix, which Plan 2's docs quote: `Scheduled or delayed delivery is not supported in Serverless mode`.

- [ ] **Step 1: Write the failing tests**

```csharp
using JasperFx.Core;
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
            .IncludeExternalTransports()
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
        await Should.ThrowAsync<InvalidOperationException>(async () =>
            await host.MessageBus().InvokeAsync(new ServerlessDelayedStart()));
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
```

Add `using Microsoft.Extensions.DependencyInjection;` for `GetRequiredService`.

- [ ] **Step 2: Run them to verify they fail**

Run: `dotnet test src/Testing/CoreTests/CoreTests.csproj -f net9.0 --filter "FullyQualifiedName~serverless_routing"`
Expected:
- `host_starts…` and `cascade_to_an_external_route_is_sent` fail with `UnknownTransportException … local://durable/`;
- the two scheduled tests fail with `UnknownTransportException` instead of `InvalidOperationException`.

- [ ] **Step 3: Create `ScheduledSendFallback`**

```csharp
using JasperFx.Core.Reflection;
using Wolverine.Transports;
using Wolverine.Transports.Sending;

namespace Wolverine.Runtime.Routing;

/// <summary>
/// Resolves the local durable queue that holds a scheduled envelope whose destination cannot schedule natively.
/// Serverless mode removes the local transport, so there it fails with an error that names the message instead of
/// an UnknownTransportException for "local://durable". Spec §6.1 of the Pub/Sub push design.
/// </summary>
internal static class ScheduledSendFallback
{
    public static ISendingAgent LocalDurableQueueFor(IWolverineRuntime runtime, Envelope envelope)
    {
        if (runtime.Options.Durability.Mode == DurabilityMode.Serverless)
        {
            var messageType = envelope.Message?.GetType().FullNameInCode() ?? envelope.MessageType;
            throw new InvalidOperationException(
                $"Scheduled or delayed delivery is not supported in Serverless mode for message type {messageType} to {envelope.Destination}. " +
                "The destination cannot schedule natively and Serverless mode has no local durable queue to hold the message. " +
                "Send it without a delay, or use Solo or Balanced durability mode.");
        }

        return runtime.Endpoints.GetOrBuildSendingAgent(TransportConstants.DurableLocalUri);
    }
}
```

- [ ] **Step 4: Create `LazyLocalDurableSendingAgent`**

```csharp
using Wolverine.Configuration;
using Wolverine.Transports;
using Wolverine.Transports.Sending;

namespace Wolverine.Runtime.Routing;

/// <summary>
/// Stands in for the "local://durable" sending agent on every message router. Constructing it touches nothing, so
/// building a router no longer needs a local transport (which Serverless mode removes). Callers that actually wrap a
/// scheduled envelope call <see cref="Resolve" /> first, so the Serverless error surfaces from the publish call.
/// </summary>
internal sealed class LazyLocalDurableSendingAgent : ISendingAgent
{
    private readonly IWolverineRuntime _runtime;
    private ISendingAgent? _inner;

    public LazyLocalDurableSendingAgent(IWolverineRuntime runtime)
    {
        _runtime = runtime;
    }

    public ISendingAgent Resolve(Envelope envelope)
    {
        return _inner ??= ScheduledSendFallback.LocalDurableQueueFor(_runtime, envelope);
    }

    private ISendingAgent inner =>
        _inner ??= _runtime.Endpoints.GetOrBuildSendingAgent(TransportConstants.DurableLocalUri);

    public Uri Destination => TransportConstants.DurableLocalUri;

    public Uri? ReplyUri
    {
        get => inner.ReplyUri;
        set => inner.ReplyUri = value;
    }

    public bool Latched => inner.Latched;
    public bool IsDurable => inner.IsDurable;
    public bool SupportsNativeScheduledSend => inner.SupportsNativeScheduledSend;
    public Endpoint Endpoint => inner.Endpoint;
    public DateTimeOffset LastMessageSentAt => inner.LastMessageSentAt;

    public ValueTask EnqueueOutgoingAsync(Envelope envelope) => inner.EnqueueOutgoingAsync(envelope);
    public ValueTask StoreAndForwardAsync(Envelope envelope) => inner.StoreAndForwardAsync(envelope);
    public ValueTask<bool> TryStoreOutgoingAsync(Envelope envelope) => inner.TryStoreOutgoingAsync(envelope);
}
```

- [ ] **Step 5: Use it in `MessageRouterBase` (L35-36)**

Replace:
```csharp
        // We'll use this for executing scheduled envelopes that aren't native
        LocalDurableQueue = runtime.Endpoints.GetOrBuildSendingAgent(TransportConstants.DurableLocalUri);
```
with:
```csharp
        // We'll use this for executing scheduled envelopes that aren't native. Lazy so that building a router
        // never needs the local transport, which Serverless mode removes.
        LocalDurableQueue = new LazyLocalDurableSendingAgent(runtime);
```
Remove `using Wolverine.Transports;` if it becomes unused (warnings are errors).

- [ ] **Step 6: Resolve before wrapping in `MessageRoute.CreateForSending` (L243-247)**

Replace:
```csharp
                return envelope.ForScheduledSend(localDurableQueue);
```
with:
```csharp
                // Resolve now, in the caller, so a Serverless host gets its error from the publish call instead of
                // from the flush's catch block, which would log and discard it
                var holder = localDurableQueue is LazyLocalDurableSendingAgent lazy
                    ? lazy.Resolve(envelope)
                    : localDurableQueue;
                return envelope.ForScheduledSend(holder);
```

- [ ] **Step 7: Use the fallback in `DestinationEndpoint` (L57-58 and L103-104)**

Replace both occurrences of:
```csharp
            var localDurableQueue =
                _parent.Runtime.Endpoints.GetOrBuildSendingAgent(TransportConstants.DurableLocalUri);
```
with:
```csharp
            var localDurableQueue = ScheduledSendFallback.LocalDurableQueueFor(_parent.Runtime, envelope);
```
Add `using Wolverine.Runtime.Routing;` if needed.

- [ ] **Step 8: Pre-populate routing in Serverless (`WolverineRuntime.HostService.cs` L316-347)**

Replace the comment block and condition at L324-347 with:
```csharp
            // Skip in MediatorOnly mode: no messaging happens through this runtime, so RoutingFor() is never
            // called in steady state, and pre-populating would lazily instantiate local sending agents (the
            // LocalRoutingMessageSource resolves Endpoint.Agent as a side effect of building a route).
            //
            // Serverless pre-populates too. Router construction no longer touches local://durable
            // (LazyLocalDurableSendingAgent), so the cold-start win applies there as well.
            var mode = Options.Durability.Mode;
            if (mode != DurabilityMode.MediatorOnly)
            {
                PrepopulateRoutingCache(Handlers.AllMessageTypes());
            }
```

- [ ] **Step 9: Run the tests to verify they pass**

Run: `dotnet test src/Testing/CoreTests/CoreTests.csproj -f net9.0 --filter "FullyQualifiedName~serverless_routing"`
Expected: 4 PASS.

- [ ] **Step 10: Run the routing and scheduling regression suites**

Run: `dotnet test src/Testing/CoreTests/CoreTests.csproj -f net9.0 --filter "FullyQualifiedName~Routing|FullyQualifiedName~Scheduled|FullyQualifiedName~Bug_3263"`
Expected: all PASS.

- [ ] **Step 11: Commit**

```bash
git add src/Wolverine/Runtime/Routing/ScheduledSendFallback.cs src/Wolverine/Runtime/Routing/LazyLocalDurableSendingAgent.cs src/Wolverine/Runtime/Routing/MessageRouterBase.cs src/Wolverine/Runtime/Routing/MessageRoute.cs src/Wolverine/Runtime/DestinationEndpoint.cs src/Wolverine/Runtime/WolverineRuntime.HostService.cs src/Testing/CoreTests/Serverless/serverless_routing.cs
git commit -m "Resolve local://durable only for scheduled sends so Serverless can publish"
```

---

### Task 3: Unrouted locally handled messages fail in Serverless (spec §6.2)

**Files:**
- Create: `src/Wolverine/Runtime/Routing/NoExternalRouteInServerlessException.cs`
- Modify: `src/Wolverine/Runtime/MessageBus.cs:317-323`
- Modify: `src/Wolverine/Runtime/WolverineRuntime.HostService.cs` (after the pre-population from Task 2)
- Test: `src/Testing/CoreTests/Serverless/serverless_unrouted_messages.cs`

**Interfaces:**
- Consumes: `IWolverineRuntime.Options`, `WolverineRuntime.Handlers.CanHandle(Type)` (`HandlerGraph.cs:720`), `Type.IsSystemMessageType()` (`Wolverine.Configuration.Capabilities`, internal).
- Produces: `public class NoExternalRouteInServerlessException : InvalidOperationException { public Type MessageType { get; } }`.

- [ ] **Step 1: Write the failing tests**

```csharp
using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Wolverine;
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
            await host.MessageBus().InvokeAsync(new LocalOnlyStart()));

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

        await host.MessageBus().InvokeAsync(new LocalOnlyStart());
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
```

- [ ] **Step 2: Run them to verify they fail**

Run: `dotnet test src/Testing/CoreTests/CoreTests.csproj -f net9.0 --filter "FullyQualifiedName~serverless_unrouted_messages"`
Expected:
- does not compile (`NoExternalRouteInServerlessException` missing);
- after Step 3, `cascading…throws` and `startup_warns…` FAIL, and the other two PASS.

- [ ] **Step 3: Create the exception**

```csharp
using JasperFx.Core.Reflection;

namespace Wolverine.Runtime.Routing;

/// <summary>
/// Thrown in <see cref="DurabilityMode.Serverless" /> when a message that has a local handler is published or
/// cascaded without any route. Serverless mode has no local queues, so the handler could never run; route the
/// message to an external transport (for example a Pub/Sub topic) instead.
/// </summary>
public class NoExternalRouteInServerlessException : InvalidOperationException
{
    public NoExternalRouteInServerlessException(Type messageType) : base(
        $"Message type {messageType.FullNameInCode()} has a local handler but no route. Serverless mode has no local queues, " +
        "so publish or cascade it to an external transport (for example a Pub/Sub topic) instead.")
    {
        MessageType = messageType;
    }

    /// <summary>
    /// The message type that has a local handler but no route
    /// </summary>
    public Type MessageType { get; }
}
```

- [ ] **Step 4: Throw it from `MessageBus.PublishAsync` (L317-323)**

Replace:
```csharp
        if (outgoing.Length != 0)
        {
            return PersistOrSendAsync(outgoing);
        }

        Runtime.MessageTracking.NoRoutesFor(new Envelope(message));
        return ValueTask.CompletedTask;
```
with:
```csharp
        if (outgoing.Length != 0)
        {
            return PersistOrSendAsync(outgoing);
        }

        assertRoutableInServerless(message.GetType());

        Runtime.MessageTracking.NoRoutesFor(new Envelope(message));
        return ValueTask.CompletedTask;
```
and add to `MessageBus`:
```csharp
    private void assertRoutableInServerless(Type messageType)
    {
        if (Runtime.Options.Durability.Mode != DurabilityMode.Serverless) return;
        if (messageType.IsSystemMessageType()) return;

        if (Runtime is WolverineRuntime runtime && runtime.Handlers.CanHandle(messageType))
        {
            throw new NoExternalRouteInServerlessException(messageType);
        }
    }
```
Add `using Wolverine.Configuration.Capabilities;` and `using Wolverine.Runtime.Routing;`.

- [ ] **Step 5: Warn at startup (`WolverineRuntime.HostService.cs`, immediately after the `PrepopulateRoutingCache` block from Task 2)**

```csharp
            if (mode == DurabilityMode.Serverless)
            {
                warnAboutUnroutedCascadesInServerless();
            }
```
and add to the partial class:
```csharp
    private void warnAboutUnroutedCascadesInServerless()
    {
        var unrouted = Handlers.Chains
            .SelectMany(x => x.PublishedTypes())
            .Distinct()
            .Where(t => !t.IsSystemMessageType() && Handlers.CanHandle(t) && RoutingFor(t).Routes.Length == 0)
            .Select(t => t.FullNameInCode())
            .ToArray();

        if (unrouted.Length == 0) return;

        Logger.LogWarning(
            "Serverless mode has no local queues, but these message types are cascaded by handlers and only have local handlers, so publishing them will fail: {MessageTypes}. Route them to an external transport, for example a Pub/Sub topic.",
            string.Join(", ", unrouted));
    }
```
Add `using Wolverine.Configuration.Capabilities;` and `using JasperFx.Core.Reflection;` if not present.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `dotnet test src/Testing/CoreTests/CoreTests.csproj -f net9.0 --filter "FullyQualifiedName~serverless_"`
Expected: all PASS (this task's tests and Task 2's).

- [ ] **Step 7: Commit**

```bash
git add src/Wolverine/Runtime/Routing/NoExternalRouteInServerlessException.cs src/Wolverine/Runtime/MessageBus.cs src/Wolverine/Runtime/WolverineRuntime.HostService.cs src/Testing/CoreTests/Serverless/serverless_unrouted_messages.cs
git commit -m "Fail loudly in Serverless when a locally handled message has no route"
```

---

### Task 4: Failure notifications to the channel (spec §6.3)

**Files:**
- Modify: `src/Wolverine/Transports/IChannelCallback.cs` (append the interface)
- Modify: `src/Wolverine/Runtime/MessageContext.cs:251-261`
- Modify: `src/Wolverine/Runtime/HandlerPipeline.cs:135-172`
- Test: `src/Testing/CoreTests/Serverless/channel_failure_notifications.cs`

**Interfaces:**
- Produces (Plan 2's `PubsubPushDelivery` implements this):

```csharp
public interface IObserveChannelFailures
{
    void OutgoingSendFailed(Envelope incoming, Envelope outgoing, Exception exception);
    void ProcessingFailed(Envelope envelope, Exception exception);
}
```

- [ ] **Step 1: Write the failing tests**

```csharp
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
        context.EnlistInOutbox(context);

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
        context.EnlistInOutbox(context);

        var failing = Substitute.For<ISendingAgent>();
        failing.StoreAndForwardAsync(Arg.Any<Envelope>())
            .Returns(_ => ValueTask.FromException(new DivideByZeroException()));

        await context.PersistOrSendAsync(new Envelope { Message = new object(), Sender = failing });
        await context.FlushOutgoingMessagesAsync();
    }

    private class RecordingChannel : IChannelCallback, IObserveChannelFailures
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

        public void OutgoingSendFailed(Envelope incoming, Envelope outgoing, Exception exception)
            => Calls.Add("OutgoingSendFailed");

        public void ProcessingFailed(Envelope envelope, Exception exception) => Calls.Add("ProcessingFailed");
    }
}
```

`EnlistInOutbox` and `PersistOrSendAsync(Envelope)` are on `MessageBus`: `public` at `MessageBus.cs:379` and `internal` at `MessageBus.cs:349`, and CoreTests can see internals. `ReadEnvelope` is internal on `MessageContext`.

- [ ] **Step 2: Run them to verify they fail**

Run: `dotnet test src/Testing/CoreTests/CoreTests.csproj -f net9.0 --filter "FullyQualifiedName~channel_failure_notifications"`
Expected: compile error (`IObserveChannelFailures` missing).

- [ ] **Step 3: Add the interface to the end of `IChannelCallback.cs`**

```csharp
/// <summary>
/// Optional capability of a channel that settles a delivery from the outcome of the whole pipeline run, such as
/// Pub/Sub push delivery answering one HTTP request. Wolverine tells the channel about failures that it would
/// otherwise only log, so the channel can refuse to acknowledge the incoming message.
/// </summary>
public interface IObserveChannelFailures
{
    /// <summary>
    /// An outgoing (cascaded or published) message could not be sent and is about to be discarded
    /// </summary>
    void OutgoingSendFailed(Envelope incoming, Envelope outgoing, Exception exception);

    /// <summary>
    /// The handler pipeline failed in a way it can only recover from by completing the envelope; called before
    /// <see cref="IChannelCallback.CompleteAsync" />
    /// </summary>
    void ProcessingFailed(Envelope envelope, Exception exception);
}
```

- [ ] **Step 4: Notify from the flush catch (`MessageContext.cs` L251-261)**

Inside the existing `catch (Exception e)`, after `Runtime.MessageTracking.DiscardedEnvelope(envelope);`, add:
```csharp
                if (_channel is IObserveChannelFailures observer && Envelope != null)
                {
                    observer.OutgoingSendFailed(Envelope, envelope, e);
                }
```

- [ ] **Step 5: Notify from the recovery path (`HandlerPipeline.cs` L143-147)**

Change the start of the `try` in `RecoverFromFailedProcessingAsync` to:
```csharp
        try
        {
            // Tell a channel that settles from the whole run (Pub/Sub push) that this ack is a failure
            (channel as IObserveChannelFailures)?.ProcessingFailed(envelope, exception);

            // Gotta get the message out of here because it's something that
            // could never be handled
            await channel.CompleteAsync(envelope).ConfigureAwait(false);
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `dotnet test src/Testing/CoreTests/CoreTests.csproj -f net9.0 --filter "FullyQualifiedName~channel_failure_notifications"`
Expected: 3 PASS.

- [ ] **Step 7: Commit**

```bash
git add src/Wolverine/Transports/IChannelCallback.cs src/Wolverine/Runtime/MessageContext.cs src/Wolverine/Runtime/HandlerPipeline.cs src/Testing/CoreTests/Serverless/channel_failure_notifications.cs
git commit -m "Let a channel observe send and pipeline failures that would otherwise be acked"
```

---

### Task 5: Per-call cancellation for the handler pipeline (spec §6.4)

**Files:**
- Modify: `src/Wolverine/Runtime/HandlerPipeline.cs` (`InvokeAsync` 3-arg L80-123, `executeAsync` L363-426)
- Modify: `src/Wolverine/Runtime/MessageContext.cs` (new property; `ClearState` L926-943)
- Modify: `src/Wolverine/Runtime/Handlers/Executor.cs:263-274`
- Modify: `src/Wolverine/Runtime/Handlers/TracingExecutor.cs:187-198`
- Modify: `src/Wolverine/Runtime/WorkerQueues/InlineReceiver.cs`
- Test: `src/Testing/CoreTests/Serverless/caller_cancellation.cs`

**Interfaces:**
- Produces:
  - `public Task HandlerPipeline.InvokeAsync(Envelope envelope, IChannelCallback channel, Activity? activity, CancellationToken callerCancellation)`;
  - `internal CancellationToken MessageContext.CallerCancellation { get; set; }`;
  - `internal ValueTask InlineReceiver.ReceivedAsync(IListener listener, Envelope[] messages, CancellationToken cancellation)`. Plan 2's push processor calls this one.

- [ ] **Step 1: Write the failing tests**

```csharp
using JasperFx.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Wolverine;
using Wolverine.Runtime;
using Wolverine.Transports;
using Xunit;

namespace CoreTests.Serverless;

public class caller_cancellation
{
    private static async Task<(IHost, HandlerPipeline)> buildAsync()
    {
        var host = await Host.CreateDefaultBuilder()
            .UseWolverine(opts => opts.Discovery.DisableConventionalDiscovery()
                .IncludeType<WaitsForeverHandler>()
                .IncludeType<ThrowsCancelledHandler>())
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

public class WaitsForeverHandler
{
    public static Task Handle(WaitsForever _, CancellationToken token) => Task.Delay(Timeout.Infinite, token);
}

public class ThrowsCancelledHandler
{
    public static void Handle(ThrowsCancelled _) => throw new OperationCanceledException();
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `dotnet test src/Testing/CoreTests/CoreTests.csproj -f net9.0 --filter "FullyQualifiedName~caller_cancellation"`
Expected: compile error (no 4-arg `InvokeAsync`).

- [ ] **Step 3: Add `CallerCancellation` to `MessageContext`**

Add near the other per-message state:
```csharp
    /// <summary>
    /// The cancellation token of whoever is waiting on this message's processing, such as an HTTP request in Pub/Sub
    /// push delivery. Default when nobody is. When it is cancelled, the executor leaves the envelope unsettled
    /// instead of running failure rules.
    /// </summary>
    internal CancellationToken CallerCancellation { get; set; }
```
In `ClearState()` add `CallerCancellation = default;` after `Tracker = null;`.

- [ ] **Step 4: Add the 4-arg overload to `HandlerPipeline`**

Replace the body of the existing 3-arg `InvokeAsync(Envelope, IChannelCallback, Activity?)` with a forward:
```csharp
    public Task InvokeAsync(Envelope envelope, IChannelCallback channel, Activity? activity)
    {
        return InvokeAsync(envelope, channel, activity, CancellationToken.None);
    }

    /// <summary>
    /// Run one envelope through the pipeline on behalf of a caller that may give up, such as an HTTP request.
    /// When <paramref name="callerCancellation" /> is cancelled the handler sees cancellation and the envelope is
    /// left unsettled for the caller to decide; failure rules do not run for it.
    /// </summary>
    public async Task InvokeAsync(Envelope envelope, IChannelCallback channel, Activity? activity,
        CancellationToken callerCancellation)
    {
        using var linked = callerCancellation.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(_cancellation, callerCancellation)
            : null;
        var cancellation = linked?.Token ?? _cancellation;

        try
        {
            // Inside the try so the early return still stops the activity in the finally;
            // the 2-arg overload above relies on this method to stop what it started.
            if (cancellation.IsCancellationRequested)
            {
                return;
            }

            WolverineTracing.LinkToPreviousAttempt(activity, envelope);

            var context = _contextPool.Get();
            context.ReadEnvelope(envelope, channel);
            context.CallerCancellation = callerCancellation;

            try
            {
                var continuation = await executeAsync(context, envelope, activity, cancellation).ConfigureAwait(false);
                await continuation.ExecuteAsync(context, _runtime, DateTimeOffset.UtcNow, activity).ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
                // It's shutting down, get out of here
            }
            catch (Exception e)
            {
                // (keep the existing GH-3111 comment here)
                await RecoverFromFailedProcessingAsync(channel, envelope, e, Logger, _runtime.Logger, activity)
                    .ConfigureAwait(false);
            }
            finally
            {
                _contextPool.Return(context);
            }
        }
        finally
        {
            activity?.Stop();
        }
    }
```
Change `executeAsync`'s signature to `executeAsync(MessageContext context, Envelope envelope, Activity? activity, CancellationToken cancellation)`, and its last line to `return await executor.ExecuteAsync(context, cancellation).ConfigureAwait(false);`. Callers that pass no token see no change, because `cancellation == _cancellation`.

Check that `ReadEnvelope` does not reset `CallerCancellation`. It is set after `ReadEnvelope` above, so it doesn't matter.

- [ ] **Step 5: Skip failure rules on caller cancellation in `Executor.ExecuteAsync` (L263-274)**

In the `catch (Exception e)` block, after `await context.ClearAllAsync().ConfigureAwait(false);`, insert:
```csharp
            if (e is OperationCanceledException && context.CallerCancellation.IsCancellationRequested)
            {
                // The caller (e.g. a Pub/Sub push request) gave up. Leave the envelope unsettled: running the
                // failure rules here could dead-letter a message the broker is about to redeliver. Spec §6.4.
                return NullContinuation.Instance;
            }
```
Make the same insertion in `TracingExecutor.ExecuteAsync` after its `await context.ClearAllAsync();` (L194).

- [ ] **Step 6: Add the token-aware entry point to `InlineReceiver`**

Change `ProcessMessageAsync(IListener listener, Envelope envelope)` to take `CancellationToken cancellation = default`, and change its pipeline call (L190) to:
```csharp
            if (_pipeline is HandlerPipeline handlerPipeline)
            {
                await handlerPipeline.InvokeAsync(envelope, listener, activity, cancellation);
            }
            else
            {
                await _pipeline.InvokeAsync(envelope, listener, activity!);
            }
```
Add the overload:
```csharp
    /// <summary>
    /// Process envelopes for a caller that may give up, such as a Pub/Sub push request. See
    /// <see cref="HandlerPipeline.InvokeAsync(Envelope, IChannelCallback, Activity?, CancellationToken)" />.
    /// </summary>
    internal async ValueTask ReceivedAsync(IListener listener, Envelope[] messages, CancellationToken cancellation)
    {
        if (messages.Length == 0) return;

        stampReceipt();
        Interlocked.Add(ref _inFlightCount, messages.Length);

        foreach (var envelope in messages)
        {
            try
            {
                await ProcessMessageAsync(listener, envelope, cancellation);
            }
            finally
            {
                DecrementInFlightCount();
            }
        }
    }
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `dotnet test src/Testing/CoreTests/CoreTests.csproj -f net9.0 --filter "FullyQualifiedName~caller_cancellation"`
Expected: 3 PASS.

- [ ] **Step 8: Run the pipeline and error-handling regression suites**

Run: `dotnet test src/Testing/CoreTests/CoreTests.csproj -f net9.0 --filter "FullyQualifiedName~ErrorHandling|FullyQualifiedName~HandlerPipeline|FullyQualifiedName~Inline"`
Expected: all PASS.

- [ ] **Step 9: Commit**

```bash
git add src/Wolverine/Runtime/HandlerPipeline.cs src/Wolverine/Runtime/MessageContext.cs src/Wolverine/Runtime/Handlers/Executor.cs src/Wolverine/Runtime/Handlers/TracingExecutor.cs src/Wolverine/Runtime/WorkerQueues/InlineReceiver.cs src/Testing/CoreTests/Serverless/caller_cancellation.cs
git commit -m "Give the handler pipeline a caller cancellation token that skips failure rules"
```

---

### Task 6: Full verification

**Files:** none new.

- [ ] **Step 1: Run all of CoreTests**

Run: `dotnet test src/Testing/CoreTests/CoreTests.csproj -f net9.0`
Expected: no new failures compared with `main`. If anything fails, run the same test on `main` (`git stash` is useless for committed work; use a second worktree) to confirm whether it is pre-existing before touching it.

- [ ] **Step 2: Build the full solution pinned to net9.0 (the CI gate)**

Run: `dotnet build wolverine.slnx -c Release -f net9.0`
Expected: Build succeeded, 0 warnings treated as errors.

- [ ] **Step 3: Remove the stale TODO**

Confirm the `TODO: a follow-up could make MessageRouterBase's LocalDurableQueue lazy` comment is gone from `WolverineRuntime.HostService.cs` (Task 2 Step 8 replaced it). If it isn't, delete it and commit with message `Remove the resolved LocalDurableQueue TODO`.
