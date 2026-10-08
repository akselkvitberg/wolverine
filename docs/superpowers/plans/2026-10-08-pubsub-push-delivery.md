# Pub/Sub Push Delivery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a Cloud Run service on request-based billing receive Google Pub/Sub messages over HTTP push and run the handler, its error policies and its cascaded Pub/Sub publishes inside that request.

**Architecture:**
- Push is a delivery mode of the existing `PubsubEndpoint`.
- `WolverineFx.Pubsub` gains:
  - push settings;
  - push-aware provisioning;
  - a push-request parser;
  - a per-request settlement recorder (`PubsubPushDelivery`);
  - a processor that runs envelopes through an `InlineReceiver` with the request's cancellation token.
- A new `WolverineFx.Pubsub.AspNetCore` package maps one POST route, authenticates the request, and turns the processor's result into a status code.

This is PR 2 (plus PR 3's docs) of spec §8.0 and depends on PR 1 (`docs/superpowers/plans/2026-10-08-serverless-core-fixes.md`).

**Tech Stack:** C# 12, .NET 9/10, Google.Cloud.PubSub.V1 3.24.0, Google.Apis.Auth (transitive), ASP.NET Core minimal APIs, xunit v3, Shouldly, NSubstitute, Alba 8.5.2, Pub/Sub emulator (`docker compose up -d gcp-pubsub`, port 8085).

**Spec:** `docs/superpowers/specs/2026-10-08-pubsub-push-delivery-design.md`. Read it in full before starting; §3–§5 and §7 are the contract for this plan.

## Global Constraints

- **PR 1 must be merged first.** This plan uses:
  - `IObserveChannelFailures`;
  - `InlineReceiver.ReceivedAsync(IListener, Envelope[], CancellationToken)`;
  - `NoExternalRouteInServerlessException`;
  - the lazy `local://durable` routing fix.
- Push endpoints are supported only in `DurabilityMode.Serverless`, and only on the default Pub/Sub broker (`UsePubsub`), not named brokers.
- Status codes, verbatim from spec §4:
  - 204 for every ack;
  - 503 for requeue, scheduled retry, an unsettled envelope, a host that is stopping or not started, and nacking toward a subscription `DeadLetterPolicy`;
  - 500 for a failed dead-letter publish, a failed outgoing send, or a pipeline failure;
  - 400 for a bad body or a subscription that doesn't match;
  - 404 for an unknown endpoint or one in pull mode;
  - 401 or 403 for authentication.
- Default `RoutePrefix` is `/_wolverine/pubsub`. Push URL is `BaseUrl + RoutePrefix + "/" + Uri.EscapeDataString(endpointName)`. `Audience` defaults to `BaseUrl`.
- `ConfigurePushDelivery(...)` sets `opts.Durability.UseSyncRetryBlock = true`, at configuration time.
- No background timers in push code paths: Cloud Run gives no CPU between requests. Ack-deadline overruns are logged after the fact, not by a watchdog timer.
- Hot-path lookups (endpoint by name per request) use `ImHashMap` from `JasperFx.Core`, never `FrozenDictionary` (see `CLAUDE.md`).
- Member casing follows accessibility:
  - `public` and `internal` members use PascalCase;
  - `private` and `protected` members use camelCase;
  - fields use `_camelCase`.
- `TreatWarningsAsErrors` is on, so public members need XML docs.
- Tests use xunit v3:
  - pass `TestContext.Current.CancellationToken` to cancellable calls;
  - guard emulator tests with `Assert.SkipUnless(await TestingExtensions.IsEmulatorAvailable(), "Pub/Sub emulator is not available")`.
- Do not edit `CHANGELOG.md`.
- Gate before pushing: `dotnet build wolverine.slnx -c Release -f net9.0`.

## Review Focus

1. **A push request that arrives while the host is still starting, or after `ApplicationStopping`.** It must get 503 and never run the handler. Tests in Tasks 5 and 6.
2. **Topic names with `%`, `+` or `~` used as endpoint names.** The provisioned URL must be escaped, and the route must still match. Tests in Tasks 1 and 6.
3. **A batched push delivery (`batched` attribute) where one envelope fails and another succeeds.** It must be one nack, with the status of the first failure. Test in Task 3.
4. **`deliveryAttempt` missing, 0 or 1.** `Attempts` must not go negative, and the first execution sees 1. Test in Task 5.
5. **A tenant subscription from a project that is not registered as a tenant.** It must get 400, not be processed as the default tenant. Test in Task 5.

---

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `src/Transports/GCP/Wolverine.Pubsub/PubsubPushSettings.cs` | Create | Transport-wide push settings, auth mode, URL building |
| `src/Transports/GCP/Wolverine.Pubsub/PubsubPushEndpointOptions.cs` | Create | Per-endpoint overrides + `PubsubDeliveryMode` enum |
| `src/Transports/GCP/Wolverine.Pubsub/PubsubTransport.cs` | Modify | `Push` property |
| `src/Transports/GCP/Wolverine.Pubsub/PubsubEndpoint.cs` | Modify | Delivery mode, effective audience/email, push init (validation, provisioning, startup read), seek purge |
| `src/Transports/GCP/Wolverine.Pubsub/PubsubConfiguration.cs` | Modify | `ConfigurePushDelivery` |
| `src/Transports/GCP/Wolverine.Pubsub/PubsubTopicListenerConfiguration.cs` | Modify | `UsePushDelivery` |
| `src/Transports/GCP/Wolverine.Pubsub/Push/PubsubPushRequest.cs` | Create | Parse the push JSON body |
| `src/Transports/GCP/Wolverine.Pubsub/Push/PubsubPushResult.cs` | Create | Status code + reason |
| `src/Transports/GCP/Wolverine.Pubsub/Push/PubsubPushDelivery.cs` | Create | Per-request settlement recorder |
| `src/Transports/GCP/Wolverine.Pubsub/Push/PubsubPushValidation.cs` | Create | Startup errors and warnings |
| `src/Transports/GCP/Wolverine.Pubsub/Push/PubsubPushProcessor.cs` | Create | Resolve endpoint, map, run, settle |
| `src/Transports/GCP/Wolverine.Pubsub/AssemblyAttributes.cs` | Modify | `InternalsVisibleTo("Wolverine.Pubsub.AspNetCore")` |
| `src/Transports/GCP/Wolverine.Pubsub.AspNetCore/*` | Create | Package: route mapping, authentication |
| `src/Transports/GCP/Wolverine.Pubsub.Tests/Push/*.cs` | Create | All tests |
| `src/Transports/GCP/Wolverine.Pubsub.Tests/Wolverine.Pubsub.Tests.csproj` | Modify | Reference AspNetCore project + Alba |
| `wolverine.slnx`, `build/build.cs` | Modify | Add the new project; add to `NugetProjects` |
| `docs/guide/messaging/transports/gcp-pubsub/push.md` + sidebar + `serverless.md` + `listening.md` | Create/Modify | Docs |
| `src/Transports/GCP/Wolverine.Pubsub.Tests/DocumentationSamples.cs` | Modify | Snippet regions |

---

### Task 1: Push settings and configuration API

**Files:**
- Create: `src/Transports/GCP/Wolverine.Pubsub/PubsubPushSettings.cs`
- Create: `src/Transports/GCP/Wolverine.Pubsub/PubsubPushEndpointOptions.cs`
- Modify: `src/Transports/GCP/Wolverine.Pubsub/PubsubTransport.cs` (add property after `DeadLetter`, L32)
- Modify: `src/Transports/GCP/Wolverine.Pubsub/PubsubEndpoint.cs` (fields near L18-45)
- Modify: `src/Transports/GCP/Wolverine.Pubsub/PubsubConfiguration.cs` (after `EnableDeadLettering`, L112)
- Modify: `src/Transports/GCP/Wolverine.Pubsub/PubsubTopicListenerConfiguration.cs` (append method)
- Test: `src/Transports/GCP/Wolverine.Pubsub.Tests/Push/push_configuration.cs`

**Interfaces:**
- Produces:
  - `public enum PubsubDeliveryMode { Pull, Push }`;
  - `public enum PubsubPushAuthentication { NotConfigured, TrustCloudRunIam, VerifyOidcToken, AllowUnauthenticated }`;
  - `public class PubsubPushSettings`:
    - `string? BaseUrl`, `string RoutePrefix`, `string? ServiceAccountEmail`, `string? Audience`;
    - `PubsubPushAuthentication Authentication { get; }`;
    - `TrustCloudRunIam()`, `VerifyOidcToken()`, `AllowUnauthenticated()`;
    - `internal bool IsRouteMapped`;
    - `internal string? PushUrlFor(string endpointName)`;
  - `public class PubsubPushEndpointOptions` with `string? ServiceAccountEmail`, `string? Audience`;
  - `PubsubTransport.Push` (`PubsubPushSettings`);
  - on `PubsubEndpoint`:
    - `DeliveryMode` (`PubsubDeliveryMode`, public get, internal set);
    - `PushOptions` (`PubsubPushEndpointOptions`);
    - `internal string? EffectiveServiceAccountEmail`, `internal string? EffectiveAudience`, `internal string? PushUrl`;
  - `PubsubConfiguration ConfigurePushDelivery(Action<PubsubPushSettings> configure)`;
  - `PubsubTopicListenerConfiguration UsePushDelivery(Action<PubsubPushEndpointOptions>? configure = null)`.

- [ ] **Step 1: Write the failing tests**

```csharp
using Wolverine.Pubsub;
using Xunit;

namespace Wolverine.Pubsub.Tests.Push;

public class push_configuration
{
    [Fact]
    public void push_url_is_base_url_plus_route_prefix_plus_escaped_endpoint_name()
    {
        var settings = new PubsubPushSettings { BaseUrl = "https://orders-abc.a.run.app/" };

        settings.PushUrlFor("orders").ShouldBe("https://orders-abc.a.run.app/_wolverine/pubsub/orders");
        settings.PushUrlFor("a+b~c%d").ShouldBe("https://orders-abc.a.run.app/_wolverine/pubsub/a%2Bb~c%25d");
    }

    [Fact]
    public void push_url_is_null_without_a_base_url()
    {
        new PubsubPushSettings().PushUrlFor("orders").ShouldBeNull();
    }

    [Fact]
    public void configure_push_delivery_turns_on_sync_retry_and_records_the_auth_mode()
    {
        var opts = new WolverineOptions();
        opts.UsePubsub("wolverine").ConfigurePushDelivery(p => p.TrustCloudRunIam());

        opts.Durability.UseSyncRetryBlock.ShouldBeTrue();
        opts.Transports.GetOrCreate<PubsubTransport>().Push.Authentication
            .ShouldBe(PubsubPushAuthentication.TrustCloudRunIam);
    }

    [Fact]
    public void audience_and_email_fall_back_from_endpoint_to_transport_to_base_url()
    {
        var transport = new PubsubTransport { ProjectId = "wolverine" };
        transport.Push.BaseUrl = "https://svc.a.run.app";
        transport.Push.ServiceAccountEmail = "push@wolverine.iam.gserviceaccount.com";
        var endpoint = new PubsubEndpoint("orders", transport);

        endpoint.EffectiveAudience.ShouldBe("https://svc.a.run.app");
        endpoint.EffectiveServiceAccountEmail.ShouldBe("push@wolverine.iam.gserviceaccount.com");

        endpoint.PushOptions.Audience = "custom";
        endpoint.PushOptions.ServiceAccountEmail = "other@x.iam.gserviceaccount.com";
        endpoint.EffectiveAudience.ShouldBe("custom");
        endpoint.EffectiveServiceAccountEmail.ShouldBe("other@x.iam.gserviceaccount.com");
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `dotnet test src/Transports/GCP/Wolverine.Pubsub.Tests/Wolverine.Pubsub.Tests.csproj -f net9.0 --filter "FullyQualifiedName~push_configuration"`
Expected: compile errors (types missing).

- [ ] **Step 3: Create `PubsubPushEndpointOptions.cs`**

```csharp
namespace Wolverine.Pubsub;

/// <summary>
/// How a Google Cloud Platform Pub/Sub listener receives messages
/// </summary>
public enum PubsubDeliveryMode
{
    /// <summary>
    /// Wolverine pulls messages with a streaming pull subscriber (the default)
    /// </summary>
    Pull,

    /// <summary>
    /// Pub/Sub pushes each message to this application over HTTP. Requires DurabilityMode.Serverless and the
    /// WolverineFx.Pubsub.AspNetCore package's MapWolverinePubsubPush()
    /// </summary>
    Push
}

/// <summary>
/// Per-endpoint overrides for push delivery. Anything left null uses <see cref="PubsubPushSettings" />
/// </summary>
public class PubsubPushEndpointOptions
{
    /// <summary>
    /// Service account Pub/Sub uses to sign the push request's OIDC token for this endpoint
    /// </summary>
    public string? ServiceAccountEmail { get; set; }

    /// <summary>
    /// OIDC audience for this endpoint's push requests
    /// </summary>
    public string? Audience { get; set; }
}
```

- [ ] **Step 4: Create `PubsubPushSettings.cs`**

```csharp
using JasperFx.Core;

namespace Wolverine.Pubsub;

/// <summary>
/// How Wolverine authenticates Pub/Sub push requests
/// </summary>
public enum PubsubPushAuthentication
{
    /// <summary>
    /// Not chosen yet. Startup fails while any endpoint uses push delivery
    /// </summary>
    NotConfigured,

    /// <summary>
    /// Cloud Run checks the token and the invoker role before the request reaches the container
    /// </summary>
    TrustCloudRunIam,

    /// <summary>
    /// Wolverine verifies the Google-signed OIDC token itself
    /// </summary>
    VerifyOidcToken,

    /// <summary>
    /// No authentication. For the Pub/Sub emulator and tests only
    /// </summary>
    AllowUnauthenticated
}

/// <summary>
/// Transport-wide settings for Pub/Sub push delivery. See the "Push delivery" page in the Pub/Sub transport docs
/// </summary>
public class PubsubPushSettings
{
    /// <summary>
    /// Public HTTPS base URL of this service, e.g. https://orders-abc.a.run.app. Required with AutoProvision()
    /// </summary>
    public string? BaseUrl { get; set; }

    /// <summary>
    /// Route prefix MapWolverinePubsubPush() maps and AutoProvision() writes into push subscriptions
    /// </summary>
    public string RoutePrefix { get; set; } = "/_wolverine/pubsub";

    /// <summary>
    /// Service account Pub/Sub uses to sign push requests' OIDC tokens
    /// </summary>
    public string? ServiceAccountEmail { get; set; }

    /// <summary>
    /// OIDC audience. Defaults to <see cref="BaseUrl" />, because Cloud Run accepts the service URL or a configured
    /// custom audience, and Pub/Sub's own default (the full push URL) includes a path
    /// </summary>
    public string? Audience { get; set; }

    /// <summary>
    /// The chosen authentication mode
    /// </summary>
    public PubsubPushAuthentication Authentication { get; private set; } = PubsubPushAuthentication.NotConfigured;

    /// <summary>
    /// Trust Cloud Run's IAM check (service deployed with --no-allow-unauthenticated)
    /// </summary>
    public void TrustCloudRunIam() => Authentication = PubsubPushAuthentication.TrustCloudRunIam;

    /// <summary>
    /// Verify the Google-signed OIDC token in the application
    /// </summary>
    public void VerifyOidcToken() => Authentication = PubsubPushAuthentication.VerifyOidcToken;

    /// <summary>
    /// Accept unauthenticated push requests. Only for the emulator and tests
    /// </summary>
    public void AllowUnauthenticated() => Authentication = PubsubPushAuthentication.AllowUnauthenticated;

    /// <summary>
    /// Set by MapWolverinePubsubPush(), which runs before the host starts
    /// </summary>
    internal bool IsRouteMapped { get; set; }

    internal string? PushUrlFor(string endpointName)
    {
        if (BaseUrl.IsEmpty()) return null;

        var prefix = "/" + RoutePrefix.Trim('/');
        return $"{BaseUrl!.TrimEnd('/')}{prefix}/{Uri.EscapeDataString(endpointName)}";
    }
}
```

- [ ] **Step 5: Add `Push` to `PubsubTransport` (after `public PubsubDeadLetterOptions DeadLetter = new();`)**

```csharp
    /// <summary>
    ///     Settings for push delivery. See <see cref="PubsubConfiguration.ConfigurePushDelivery" />
    /// </summary>
    public PubsubPushSettings Push { get; } = new();
```

- [ ] **Step 6: Add the endpoint members to `PubsubEndpoint` (after `public PubsubServerOptions Server = new();`)**

```csharp
    /// <summary>
    ///     Whether this listener pulls or receives push requests. Set with UsePushDelivery()
    /// </summary>
    public PubsubDeliveryMode DeliveryMode { get; internal set; } = PubsubDeliveryMode.Pull;

    /// <summary>
    ///     Per-endpoint push overrides
    /// </summary>
    public PubsubPushEndpointOptions PushOptions { get; } = new();

    internal string? EffectiveServiceAccountEmail =>
        PushOptions.ServiceAccountEmail ?? _transport.Push.ServiceAccountEmail;

    internal string? EffectiveAudience =>
        PushOptions.Audience ?? _transport.Push.Audience ?? _transport.Push.BaseUrl;

    internal string? PushUrl => _transport.Push.PushUrlFor(EndpointName);

    /// <summary>Read from the real subscription at startup (spec §5.2). Null until then, or if the read failed</summary>
    internal int? ObservedAckDeadlineSeconds;

    /// <summary>Read from the real subscription at startup (spec §5.2). Null until then, or if the read failed</summary>
    internal bool? ObservedHasDeadLetterPolicy;
```

`EndpointName` is a `string` on the base `Endpoint`; it defaults to the topic name (`PubsubEndpoint.cs:88`).

- [ ] **Step 7: Add `ConfigurePushDelivery` to `PubsubConfiguration` (after `EnableDeadLettering`)**

```csharp
    /// <summary>
    ///     Configure push delivery for every endpoint that calls UsePushDelivery(). Also turns on
    ///     DurabilitySettings.UseSyncRetryBlock so cascaded sends retry inside the HTTP request instead of on a
    ///     background thread that gets no CPU between requests on Cloud Run.
    /// </summary>
    public PubsubConfiguration ConfigurePushDelivery(Action<PubsubPushSettings> configure)
    {
        configure(Transport.Push);

        // Must be set while options are configured: InlineSendingAgent reads it in its constructor
        Options.Durability.UseSyncRetryBlock = true;
        Options.Services.AddSingleton<Push.PubsubPushProcessor>();

        return this;
    }
```
Add `using Microsoft.Extensions.DependencyInjection;`. `PubsubPushProcessor` is created in Task 5. Until then, comment out the `AddSingleton` line and restore it in Task 5 Step 4.

- [ ] **Step 8: Add `UsePushDelivery` to `PubsubTopicListenerConfiguration`**

```csharp
    /// <summary>
    ///     Receive this endpoint's messages through Pub/Sub push (HTTP) instead of streaming pull. Requires
    ///     DurabilityMode.Serverless, ConfigurePushDelivery() and MapWolverinePubsubPush() from
    ///     WolverineFx.Pubsub.AspNetCore
    /// </summary>
    public PubsubTopicListenerConfiguration UsePushDelivery(Action<PubsubPushEndpointOptions>? configure = null)
    {
        add(e =>
        {
            e.DeliveryMode = PubsubDeliveryMode.Push;
            configure?.Invoke(e.PushOptions);
        });

        return this;
    }
```

- [ ] **Step 9: Run the tests to verify they pass**

Run: `dotnet test src/Transports/GCP/Wolverine.Pubsub.Tests/Wolverine.Pubsub.Tests.csproj -f net9.0 --filter "FullyQualifiedName~push_configuration"`
Expected: 4 PASS.

- [ ] **Step 10: Commit**

```bash
git add src/Transports/GCP/Wolverine.Pubsub src/Transports/GCP/Wolverine.Pubsub.Tests/Push/push_configuration.cs
git commit -m "Add Pub/Sub push delivery settings and configuration API"
```

---

### Task 2: Parse the push request body

**Files:**
- Create: `src/Transports/GCP/Wolverine.Pubsub/Push/PubsubPushRequest.cs`
- Test: `src/Transports/GCP/Wolverine.Pubsub.Tests/Push/push_request_parsing.cs`

**Interfaces:**
- Produces: `public sealed class PubsubPushRequest` with:
  - `PubsubMessage Message`, `string Subscription`, `int? DeliveryAttempt`;
  - `public static bool TryParse(ReadOnlyMemory<byte> json, out PubsubPushRequest? request, out string? error)`.

- [ ] **Step 1: Write the failing tests**

```csharp
using System.Text;
using Wolverine.Pubsub.Push;
using Xunit;

namespace Wolverine.Pubsub.Tests.Push;

public class push_request_parsing
{
    private static bool parse(string json, out PubsubPushRequest? request, out string? error)
        => PubsubPushRequest.TryParse(Encoding.UTF8.GetBytes(json), out request, out error);

    [Fact]
    public void parses_a_full_body()
    {
        var ok = parse("""
            {
              "message": {
                "data": "SGVsbG8=",
                "attributes": { "message-type": "orders.placed", "batched": "1" },
                "messageId": "123",
                "message_id": "123",
                "publishTime": "2021-02-26T19:13:55.749Z",
                "orderingKey": "customer-1"
              },
              "subscription": "projects/wolverine/subscriptions/orders",
              "deliveryAttempt": 3
            }
            """, out var request, out var error);

        ok.ShouldBeTrue(error);
        request!.Subscription.ShouldBe("projects/wolverine/subscriptions/orders");
        request.DeliveryAttempt.ShouldBe(3);
        request.Message.MessageId.ShouldBe("123");
        request.Message.OrderingKey.ShouldBe("customer-1");
        request.Message.Data.ToStringUtf8().ShouldBe("Hello");
        request.Message.Attributes["message-type"].ShouldBe("orders.placed");
        request.Message.PublishTime.ToDateTimeOffset().ShouldBe(DateTimeOffset.Parse("2021-02-26T19:13:55.749Z"));
    }

    [Fact]
    public void accepts_snake_case_aliases_and_missing_optional_fields()
    {
        var ok = parse("""
            { "message": { "message_id": "9", "publish_time": "2021-02-26T19:13:55Z" },
              "subscription": "projects/p/subscriptions/s" }
            """, out var request, out _);

        ok.ShouldBeTrue();
        request!.Message.MessageId.ShouldBe("9");
        request.Message.Data.IsEmpty.ShouldBeTrue();
        request.DeliveryAttempt.ShouldBeNull();
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("{}")]
    [InlineData("""{ "message": {} }""")]
    [InlineData("""{ "subscription": "projects/p/subscriptions/s" }""")]
    [InlineData("""{ "message": { "data": "***" }, "subscription": "projects/p/subscriptions/s" }""")]
    public void rejects_invalid_bodies(string json)
    {
        parse(json, out var request, out var error).ShouldBeFalse();
        request.ShouldBeNull();
        error.ShouldNotBeNullOrEmpty();
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `dotnet test src/Transports/GCP/Wolverine.Pubsub.Tests/Wolverine.Pubsub.Tests.csproj -f net9.0 --filter "FullyQualifiedName~push_request_parsing"`
Expected: compile error.

- [ ] **Step 3: Implement `PubsubPushRequest`**

```csharp
using System.Globalization;
using System.Text.Json;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace Wolverine.Pubsub.Push;

/// <summary>
/// One Pub/Sub push request, in the default wrapped format:
/// { "message": { data, attributes, messageId, publishTime, orderingKey }, "subscription", "deliveryAttempt" }
/// </summary>
public sealed class PubsubPushRequest
{
    private PubsubPushRequest(PubsubMessage message, string subscription, int? deliveryAttempt)
    {
        Message = message;
        Subscription = subscription;
        DeliveryAttempt = deliveryAttempt;
    }

    /// <summary>The pushed message, rebuilt as the same type the pull listener receives</summary>
    public PubsubMessage Message { get; }

    /// <summary>Full subscription name, projects/{project}/subscriptions/{subscription}</summary>
    public string Subscription { get; }

    /// <summary>Only set by Pub/Sub when the subscription has a dead-letter policy</summary>
    public int? DeliveryAttempt { get; }

    /// <summary>
    /// Parse a push body. Returns false with a reason when it is not a valid wrapped push request
    /// </summary>
    public static bool TryParse(ReadOnlyMemory<byte> json, out PubsubPushRequest? request, out string? error)
    {
        request = null;

        try
        {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;

            if (root.ValueKind != JsonValueKind.Object ||
                !root.TryGetProperty("message", out var messageElement) ||
                messageElement.ValueKind != JsonValueKind.Object)
            {
                error = "The body has no 'message' object";
                return false;
            }

            if (!root.TryGetProperty("subscription", out var subscriptionElement) ||
                subscriptionElement.ValueKind != JsonValueKind.String ||
                string.IsNullOrEmpty(subscriptionElement.GetString()))
            {
                error = "The body has no 'subscription'";
                return false;
            }

            var message = new PubsubMessage();

            if (messageElement.TryGetProperty("data", out var data) && data.ValueKind == JsonValueKind.String)
            {
                message.Data = ByteString.FromBase64(data.GetString()!);
            }

            if (messageElement.TryGetProperty("attributes", out var attributes) &&
                attributes.ValueKind == JsonValueKind.Object)
            {
                foreach (var attribute in attributes.EnumerateObject())
                {
                    message.Attributes[attribute.Name] = attribute.Value.GetString() ?? string.Empty;
                }
            }

            message.MessageId = readString(messageElement, "messageId", "message_id") ?? string.Empty;
            message.OrderingKey = readString(messageElement, "orderingKey", "ordering_key") ?? string.Empty;

            var publishTime = readString(messageElement, "publishTime", "publish_time");
            if (publishTime != null)
            {
                message.PublishTime = Timestamp.FromDateTimeOffset(DateTimeOffset.Parse(publishTime,
                    CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal));
            }

            int? deliveryAttempt = root.TryGetProperty("deliveryAttempt", out var attempt) &&
                                   attempt.ValueKind == JsonValueKind.Number
                ? attempt.GetInt32()
                : null;

            request = new PubsubPushRequest(message, subscriptionElement.GetString()!, deliveryAttempt);
            error = null;
            return true;
        }
        catch (Exception e) when (e is JsonException or FormatException or InvalidOperationException)
        {
            error = $"The body is not a valid Pub/Sub push request: {e.Message}";
            return false;
        }
    }

    private static string? readString(JsonElement element, string name, string alias)
    {
        if (element.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String)
            return value.GetString();
        if (element.TryGetProperty(alias, out value) && value.ValueKind == JsonValueKind.String)
            return value.GetString();
        return null;
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: same as Step 2. Expected: 7 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/Transports/GCP/Wolverine.Pubsub/Push/PubsubPushRequest.cs src/Transports/GCP/Wolverine.Pubsub.Tests/Push/push_request_parsing.cs
git commit -m "Parse Pub/Sub push request bodies"
```

---

### Task 3: Per-request settlement (`PubsubPushDelivery`)

**Files:**
- Create: `src/Transports/GCP/Wolverine.Pubsub/Push/PubsubPushResult.cs`
- Create: `src/Transports/GCP/Wolverine.Pubsub/Push/PubsubPushDelivery.cs`
- Test: `src/Transports/GCP/Wolverine.Pubsub.Tests/Push/push_delivery_settlement.cs`

**Interfaces:**
- Consumes:
  - `IObserveChannelFailures` (PR 1);
  - `PubsubEndpoint.SendMessageAsync(Envelope, ILogger, PubsubClientSet?)` (internal, `PubsubEndpoint.cs:446`);
  - `PubsubTransport.Topics`;
  - `DeadLetterQueueConstants.StampFailureMetadata(Envelope, Exception)` (used by `PubsubListener.MoveToErrorsAsync`).
- Produces:
  - `public readonly record struct PubsubPushResult(int StatusCode, string? Reason)` with `IsAck` and static factories;
  - `internal sealed class PubsubPushDelivery : IListener, ISupportDeadLetterQueue, ISupportNativeScheduling, IObserveChannelFailures`:
    - `PubsubPushDelivery(PubsubEndpoint endpoint, PubsubClientSet clients, IHandlerPipeline? pipeline, ILogger logger)`;
    - `PubsubPushResult ResultFor(IReadOnlyList<Envelope> envelopes)`.

- [ ] **Step 1: Write the failing tests (one per spec §4 row that the delivery owns)**

```csharp
using Google.Api.Gax.Grpc;
using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Wolverine.Pubsub.Internal;
using Wolverine.Pubsub.Push;
using Xunit;

namespace Wolverine.Pubsub.Tests.Push;

public class push_delivery_settlement
{
    private readonly PubsubTransport _transport = new() { ProjectId = "wolverine" };
    private readonly PublisherServiceApiClient _publisher = Substitute.For<PublisherServiceApiClient>();

    private PubsubPushDelivery deliveryFor(PubsubEndpoint endpoint)
    {
        var clients = new PubsubClientSet
        {
            ProjectId = "wolverine",
            EmulatorDetection = Google.Api.Gax.EmulatorDetection.None,
            PublisherApiClient = _publisher,
            SubscriberApiClient = Substitute.For<SubscriberServiceApiClient>()
        };
        _transport.PublisherApiClient = _publisher;
        return new PubsubPushDelivery(endpoint, clients, null, NullLogger.Instance);
    }

    private PubsubEndpoint endpoint(string name = "orders") => new(name, _transport);
    private static Envelope envelope() => new() { Id = Guid.NewGuid(), Data = [1], ContentType = "application/json" };

    [Fact]
    public async Task completed_is_204()
    {
        var e = envelope();
        var delivery = deliveryFor(endpoint());
        await delivery.CompleteAsync(e);
        delivery.ResultFor([e]).StatusCode.ShouldBe(204);
    }

    [Fact]
    public async Task deferred_is_503_and_stays_failed_after_a_later_complete()
    {
        var e = envelope();
        var delivery = deliveryFor(endpoint());
        await delivery.DeferAsync(e);
        await delivery.CompleteAsync(e);
        delivery.ResultFor([e]).StatusCode.ShouldBe(503);
    }

    [Fact]
    public async Task requeue_and_native_schedule_are_503()
    {
        var a = envelope();
        var b = envelope();
        var delivery = deliveryFor(endpoint());
        (await delivery.TryRequeueAsync(a)).ShouldBeTrue();
        await delivery.MoveToScheduledUntilAsync(b, DateTimeOffset.UtcNow.AddMinutes(5));
        delivery.ResultFor([a]).StatusCode.ShouldBe(503);
        delivery.ResultFor([b]).StatusCode.ShouldBe(503);
    }

    [Fact]
    public void unsettled_is_503()
    {
        deliveryFor(endpoint()).ResultFor([envelope()]).StatusCode.ShouldBe(503);
    }

    [Fact]
    public async Task processing_failed_then_completed_is_500()
    {
        var e = envelope();
        var delivery = deliveryFor(endpoint());
        delivery.ProcessingFailed(e, new DivideByZeroException());
        await delivery.CompleteAsync(e);
        delivery.ResultFor([e]).StatusCode.ShouldBe(500);
    }

    [Fact]
    public async Task outgoing_send_failure_is_500()
    {
        var e = envelope();
        var delivery = deliveryFor(endpoint());
        delivery.OutgoingSendFailed(e, envelope(), new DivideByZeroException());
        await delivery.CompleteAsync(e);
        delivery.ResultFor([e]).StatusCode.ShouldBe(500);
    }

    [Fact]
    public async Task dead_letter_publish_then_complete_is_204()
    {
        var ep = endpoint();
        ep.DeadLetterName = "wlvrn.dead-letter";
        var e = envelope();
        var delivery = deliveryFor(ep);

        await delivery.MoveToErrorsAsync(e, new DivideByZeroException());
        await delivery.CompleteAsync(e);

        delivery.ResultFor([e]).StatusCode.ShouldBe(204);
        await _publisher.Received(1).PublishAsync(Arg.Any<PublishRequest>(), Arg.Any<CallSettings>());
    }

    [Fact]
    public async Task failed_dead_letter_publish_is_500_and_does_not_throw()
    {
        var ep = endpoint();
        ep.DeadLetterName = "wlvrn.dead-letter";
        _publisher.PublishAsync(Arg.Any<PublishRequest>(), Arg.Any<CallSettings>())
            .Returns<Task<PublishResponse>>(_ => throw new InvalidOperationException("down"));
        var e = envelope();
        var delivery = deliveryFor(ep);

        await delivery.MoveToErrorsAsync(e, new DivideByZeroException());
        await delivery.CompleteAsync(e);

        delivery.ResultFor([e]).StatusCode.ShouldBe(500);
    }

    [Fact]
    public async Task dead_letter_with_only_a_subscription_policy_is_503()
    {
        var ep = endpoint();
        ep.Server.Subscription.Options.DeadLetterPolicy = new DeadLetterPolicy { MaxDeliveryAttempts = 5 };
        var e = envelope();
        var delivery = deliveryFor(ep);

        await delivery.MoveToErrorsAsync(e, new DivideByZeroException());
        await delivery.CompleteAsync(e);

        delivery.ResultFor([e]).StatusCode.ShouldBe(503);
    }

    [Fact]
    public async Task dead_letter_with_no_destination_acks_and_drops()
    {
        var e = envelope();
        var delivery = deliveryFor(endpoint());
        await delivery.MoveToErrorsAsync(e, new DivideByZeroException());
        await delivery.CompleteAsync(e);
        delivery.ResultFor([e]).StatusCode.ShouldBe(204);
    }

    [Fact]
    public async Task batch_uses_the_status_of_the_first_failure()
    {
        var ok = envelope();
        var requeued = envelope();
        var failed = envelope();
        var delivery = deliveryFor(endpoint());

        await delivery.CompleteAsync(ok);
        await delivery.DeferAsync(requeued);
        delivery.ProcessingFailed(failed, new DivideByZeroException());

        delivery.ResultFor([ok, requeued, failed]).StatusCode.ShouldBe(503);
    }
}
```

Check before running:
- `PubsubClientSet`'s `PublisherApiClient` and `SubscriberApiClient` must be settable in an object initializer. Read `Internal/PubsubClientSet.cs`; if they are `required ... init`, keep the initializer as written.
- `PubsubTransport.PublisherApiClient` is internal but visible to the test project.

- [ ] **Step 2: Run them to verify they fail**

Run: `dotnet test src/Transports/GCP/Wolverine.Pubsub.Tests/Wolverine.Pubsub.Tests.csproj -f net9.0 --filter "FullyQualifiedName~push_delivery_settlement"`
Expected: compile error.

- [ ] **Step 3: Create `PubsubPushResult`**

```csharp
namespace Wolverine.Pubsub.Push;

/// <summary>
/// The HTTP answer to one push request. Pub/Sub treats 102, 200, 201, 202 and 204 as an ack and anything else as a
/// nack; Wolverine always uses 204 for an ack and picks the nack code for operators' benefit
/// </summary>
public readonly record struct PubsubPushResult(int StatusCode, string? Reason)
{
    /// <summary>Whether Pub/Sub will treat this as an acknowledgement</summary>
    public bool IsAck => StatusCode == 204;

    /// <summary>Acknowledge</summary>
    public static PubsubPushResult Ack { get; } = new(204, null);

    /// <summary>Redeliver later: requeue, scheduled retry, unsettled, not ready</summary>
    public static PubsubPushResult RetryLater(string reason) => new(503, reason);

    /// <summary>Redeliver: a failure that lost or could not finish work</summary>
    public static PubsubPushResult Failed(string reason) => new(500, reason);

    /// <summary>The request itself is wrong</summary>
    public static PubsubPushResult BadRequest(string reason) => new(400, reason);

    /// <summary>No push endpoint by that name</summary>
    public static PubsubPushResult NotFound(string reason) => new(404, reason);
}
```

- [ ] **Step 4: Create `PubsubPushDelivery`**

```csharp
using JasperFx.Core;
using Microsoft.Extensions.Logging;
using Wolverine.Pubsub.Internal;
using Wolverine.Runtime;
using Wolverine.Transports;

namespace Wolverine.Pubsub.Push;

/// <summary>
/// Plays the listener role for one push request and records how the pipeline settled each envelope. The HTTP
/// response is computed from these outcomes once the pipeline returns (spec §4). Failure is sticky, settle calls
/// never throw, and an envelope nobody settled counts as failed.
/// </summary>
internal sealed class PubsubPushDelivery : IListener, ISupportDeadLetterQueue, ISupportNativeScheduling,
    IObserveChannelFailures
{
    private readonly PubsubEndpoint _endpoint;
    private readonly PubsubClientSet _clients;
    private readonly ILogger _logger;
    private readonly Dictionary<Envelope, PubsubPushResult> _outcomes = new(ReferenceEqualityComparer.Instance);
    private readonly object _lock = new();

    public PubsubPushDelivery(PubsubEndpoint endpoint, PubsubClientSet clients, IHandlerPipeline? pipeline,
        ILogger logger)
    {
        _endpoint = endpoint;
        _clients = clients;
        Pipeline = pipeline;
        _logger = logger;
    }

    public Uri Address => _endpoint.Uri;
    public IHandlerPipeline? Pipeline { get; }

    // Always true so MoveToErrorQueue comes here; MoveToErrorsAsync decides between the three destinations
    public bool NativeDeadLetterQueueEnabled => true;

    public ValueTask CompleteAsync(Envelope envelope)
    {
        record(envelope, PubsubPushResult.Ack);
        return ValueTask.CompletedTask;
    }

    public ValueTask DeferAsync(Envelope envelope)
    {
        record(envelope, PubsubPushResult.RetryLater("requeued"));
        return ValueTask.CompletedTask;
    }

    public Task<bool> TryRequeueAsync(Envelope envelope)
    {
        record(envelope, PubsubPushResult.RetryLater("requeued"));
        return Task.FromResult(true);
    }

    public Task MoveToScheduledUntilAsync(Envelope envelope, DateTimeOffset time)
    {
        // Pub/Sub push cannot delay one message; the subscription's retry policy decides the backoff
        record(envelope, PubsubPushResult.RetryLater("scheduled retry, delay handled by the subscription retry policy"));
        return Task.CompletedTask;
    }

    public async Task MoveToErrorsAsync(Envelope envelope, Exception exception)
    {
        if (_endpoint.DeadLetterName.IsNotEmpty())
        {
            try
            {
                DeadLetterQueueConstants.StampFailureMetadata(envelope, exception);
                var deadLetterTopic = _endpoint.Transport.Topics[_endpoint.DeadLetterName!];
                await deadLetterTopic.SendMessageAsync(envelope, _logger, _clients);
                record(envelope, PubsubPushResult.Ack);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "{Uri}: Could not publish envelope {EnvelopeId} to dead letter topic {Topic}",
                    _endpoint.Uri, envelope.Id, _endpoint.DeadLetterName);
                record(envelope, PubsubPushResult.Failed("dead letter publish failed"));
            }

            return;
        }

        if (_endpoint.Server.Subscription.Options.DeadLetterPolicy != null || _endpoint.ObservedHasDeadLetterPolicy == true)
        {
            record(envelope,
                PubsubPushResult.RetryLater("nacked so the subscription's dead letter policy can forward it"));
            return;
        }

        _logger.LogError(exception,
            "{Uri}: Envelope {EnvelopeId} ({MessageType}) failed and this push endpoint has no dead letter destination, so it is acknowledged and dropped. Enable dead lettering or give the subscription a DeadLetterPolicy",
            _endpoint.Uri, envelope.Id, envelope.MessageType);
        record(envelope, PubsubPushResult.Ack);
    }

    public void OutgoingSendFailed(Envelope incoming, Envelope outgoing, Exception exception)
        => record(incoming, PubsubPushResult.Failed("an outgoing message could not be sent"));

    public void ProcessingFailed(Envelope envelope, Exception exception)
        => record(envelope, PubsubPushResult.Failed("the handler pipeline failed"));

    public PubsubPushResult ResultFor(IReadOnlyList<Envelope> envelopes)
    {
        lock (_lock)
        {
            foreach (var envelope in envelopes)
            {
                if (!_outcomes.TryGetValue(envelope, out var outcome))
                {
                    return PubsubPushResult.RetryLater("the envelope was never settled");
                }

                if (!outcome.IsAck) return outcome;
            }

            return PubsubPushResult.Ack;
        }
    }

    private void record(Envelope envelope, PubsubPushResult outcome)
    {
        lock (_lock)
        {
            // Sticky failure: once failed, a later ack (e.g. the pipeline's recovery CompleteAsync) cannot undo it.
            // The first failure wins so the response reports the original cause.
            if (_outcomes.TryGetValue(envelope, out var existing) && !existing.IsAck) return;
            _outcomes[envelope] = outcome;
        }
    }

    public ValueTask StopAsync() => ValueTask.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
```

Both `PubsubEndpoint.Transport` (internal, `PubsubEndpoint.cs:45`) and `DeadLetterQueueConstants` (core, used by `PubsubListener.MoveToErrorsAsync`) already exist. If `DeadLetterQueueConstants` needs a `using`, copy it from `PubsubListener.cs`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: same as Step 2. Expected: 11 PASS.

- [ ] **Step 6: Commit**

```bash
git add src/Transports/GCP/Wolverine.Pubsub/Push/PubsubPushResult.cs src/Transports/GCP/Wolverine.Pubsub/Push/PubsubPushDelivery.cs src/Transports/GCP/Wolverine.Pubsub.Tests/Push/push_delivery_settlement.cs
git commit -m "Record per-request settlement for Pub/Sub push deliveries"
```

---

### Task 4: Startup validation, push provisioning, startup read and purge

**Files:**
- Create: `src/Transports/GCP/Wolverine.Pubsub/Push/PubsubPushValidation.cs`
- Modify: `src/Transports/GCP/Wolverine.Pubsub/PubsubEndpoint.cs` (`provisionAsync` L207-252, `PurgeAsync` L295-329, `InitializeAsync` L346-376)
- Test: `src/Transports/GCP/Wolverine.Pubsub.Tests/Push/push_startup_validation.cs`
- Test: `src/Transports/GCP/Wolverine.Pubsub.Tests/Push/push_provisioning.cs` (emulator)

**Interfaces:**
- Consumes: `Endpoint.Runtime` (internal; set during `Compile`, before `InitializeAsync`), `WolverineRuntime.Handlers.Failures.AnyRequeuePolicies()` and `HandlerChain.Failures.AnyRequeuePolicies()` (internal), `PubsubEndpoint.CircuitBreakerOptions`, `IsSubscriptionPerNode`.
- Produces: `internal static class PubsubPushValidation` with `public static void AssertValid(PubsubEndpoint endpoint, IWolverineRuntime runtime)` and `public static void LogWarnings(PubsubEndpoint endpoint, IWolverineRuntime runtime, ILogger logger)`. It also produces `ObservedAckDeadlineSeconds` and `ObservedHasDeadLetterPolicy`, which are filled at startup.

- [ ] **Step 1: Write the failing validation tests (no emulator needed; validation runs before any network call)**

```csharp
using Microsoft.Extensions.Hosting;
using Wolverine.ErrorHandling;
using Xunit;

namespace Wolverine.Pubsub.Tests.Push;

public class push_startup_validation
{
    private static async Task<Exception> startupFailure(Action<WolverineOptions> configure)
    {
        return await Should.ThrowAsync<Exception>(async () =>
        {
            using var host = await Host.CreateDefaultBuilder()
                .UseWolverine(opts =>
                {
                    opts.Discovery.DisableConventionalDiscovery();
                    configure(opts);
                })
                .StartAsync(TestContext.Current.CancellationToken);
        });
    }

    private static void serverlessPush(WolverineOptions opts, Action<PubsubPushSettings> push,
        Action<PubsubTopicListenerConfiguration>? listener = null)
    {
        opts.Durability.Mode = DurabilityMode.Serverless;
        opts.UsePubsubTesting().ConfigurePushDelivery(push);
        var config = opts.ListenToPubsubTopic("push-validation").UsePushDelivery();
        listener?.Invoke(config);
    }

    [Fact]
    public async Task push_outside_serverless_fails()
    {
        var ex = await startupFailure(opts =>
        {
            opts.UsePubsubTesting().ConfigurePushDelivery(p => p.AllowUnauthenticated());
            opts.ListenToPubsubTopic("push-validation").UsePushDelivery();
        });
        ex.ToString().ShouldContain("DurabilityMode.Serverless");
    }

    [Fact]
    public async Task no_authentication_mode_fails()
    {
        var ex = await startupFailure(opts =>
        {
            opts.Durability.Mode = DurabilityMode.Serverless;
            opts.UsePubsubTesting();
            opts.ListenToPubsubTopic("push-validation").UsePushDelivery();
        });
        ex.ToString().ShouldContain("authentication mode");
    }

    [Fact]
    public async Task auto_provision_without_base_url_fails()
    {
        var ex = await startupFailure(opts =>
        {
            serverlessPush(opts, p => p.AllowUnauthenticated());
            opts.UsePubsubTesting().AutoProvision();
        });
        ex.ToString().ShouldContain("BaseUrl");
    }

    [Fact]
    public async Task http_base_url_outside_allow_unauthenticated_fails()
    {
        var ex = await startupFailure(opts => serverlessPush(opts, p =>
        {
            p.BaseUrl = "http://insecure.example";
            p.TrustCloudRunIam();
        }));
        ex.ToString().ShouldContain("https");
    }

    [Fact]
    public async Task subscription_per_node_fails()
    {
        var ex = await startupFailure(opts =>
            serverlessPush(opts, p => p.AllowUnauthenticated(), l => l.SubscriptionPerNode()));
        ex.ToString().ShouldContain("SubscriptionPerNode");
    }

    [Fact]
    public async Task exactly_once_fails()
    {
        var ex = await startupFailure(opts => serverlessPush(opts, p => p.AllowUnauthenticated(),
            l => l.ConfigurePubsubSubscription(s => s.EnableExactlyOnceDelivery = true)));
        ex.ToString().ShouldContain("exactly-once");
    }

    [Fact]
    public async Task circuit_breaker_fails()
    {
        var ex = await startupFailure(opts =>
            serverlessPush(opts, p => p.AllowUnauthenticated(), l => l.CircuitBreaker()));
        ex.ToString().ShouldContain("CircuitBreaker");
    }

    [Fact]
    public async Task verify_oidc_without_audience_or_base_url_fails()
    {
        var ex = await startupFailure(opts => serverlessPush(opts, p =>
        {
            p.ServiceAccountEmail = "push@wolverine.iam.gserviceaccount.com";
            p.VerifyOidcToken();
        }));
        ex.ToString().ShouldContain("Audience");
    }

    [Fact]
    public async Task verify_oidc_without_service_account_fails()
    {
        var ex = await startupFailure(opts => serverlessPush(opts, p =>
        {
            p.BaseUrl = "https://svc.a.run.app";
            p.VerifyOidcToken();
        }));
        ex.ToString().ShouldContain("ServiceAccountEmail");
    }
}
```

`UsePushDelivery()` is applied through delayed configuration, and validation runs in `InitializeAsync` after `Compile`. So these failures happen during `StartAsync` and need no emulator.

- [ ] **Step 2: Run them to verify they fail**

Run: `dotnet test src/Transports/GCP/Wolverine.Pubsub.Tests/Wolverine.Pubsub.Tests.csproj -f net9.0 --filter "FullyQualifiedName~push_startup_validation"`
Expected: FAIL (hosts start, or fail for unrelated reasons).

- [ ] **Step 3: Create `PubsubPushValidation`**

```csharp
using JasperFx.Core;
using Microsoft.Extensions.Logging;
using Wolverine.Runtime;

namespace Wolverine.Pubsub.Push;

/// <summary>
/// Spec §5.4: configuration a push endpoint refuses to start with, and configuration it only warns about
/// </summary>
internal static class PubsubPushValidation
{
    public static void AssertValid(PubsubEndpoint endpoint, IWolverineRuntime runtime)
    {
        var transport = endpoint.Transport;
        var push = transport.Push;
        var problems = new List<string>();

        if (runtime.Options.Durability.Mode != DurabilityMode.Serverless)
            problems.Add("push delivery requires DurabilityMode.Serverless");

        if (transport.Protocol != PubsubTransport.ProtocolName)
            problems.Add("push delivery is only supported on the default Pub/Sub broker (UsePubsub), not named brokers");

        if (push.Authentication == PubsubPushAuthentication.NotConfigured)
            problems.Add("no push authentication mode is chosen; call ConfigurePushDelivery(p => p.TrustCloudRunIam() / VerifyOidcToken() / AllowUnauthenticated())");

        if (transport.AutoProvision && push.BaseUrl.IsEmpty())
            problems.Add("AutoProvision() needs ConfigurePushDelivery(p => p.BaseUrl = ...) to build the push endpoint URL");

        if (push.BaseUrl.IsNotEmpty() && push.Authentication != PubsubPushAuthentication.AllowUnauthenticated &&
            !push.BaseUrl!.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
            problems.Add("BaseUrl must use https; Pub/Sub only pushes to HTTPS endpoints outside the emulator");

        if (endpoint.IsSubscriptionPerNode)
            problems.Add("SubscriptionPerNode() cannot be used with push delivery; Cloud Run instances are not individually addressable");

        if (endpoint.Server.Subscription.Options.EnableExactlyOnceDelivery)
            problems.Add("exactly-once delivery is only supported for pull subscriptions");

        if (endpoint.CircuitBreakerOptions != null)
            problems.Add("CircuitBreaker() is not supported with push delivery; Pub/Sub's push backoff applies instead");

        if (push.Authentication == PubsubPushAuthentication.VerifyOidcToken)
        {
            if (endpoint.EffectiveAudience.IsEmpty())
                problems.Add("VerifyOidcToken() needs an Audience, or a BaseUrl to default it from");

            if (endpoint.EffectiveServiceAccountEmail.IsEmpty())
                problems.Add("VerifyOidcToken() needs ServiceAccountEmail");
        }

        if (problems.Count > 0)
        {
            throw new InvalidOperationException(
                $"Pub/Sub push endpoint '{endpoint.EndpointName}' is misconfigured:{Environment.NewLine}- " +
                string.Join(Environment.NewLine + "- ", problems));
        }
    }

    public static void LogWarnings(PubsubEndpoint endpoint, IWolverineRuntime runtime, ILogger logger)
    {
        var push = endpoint.Transport.Push;

        if (push.Authentication == PubsubPushAuthentication.AllowUnauthenticated)
            logger.LogWarning("{Uri}: Pub/Sub push requests are accepted without authentication. Use this only with the emulator or in tests", endpoint.Uri);

        if (!push.IsRouteMapped)
            logger.LogWarning("{Uri}: Pub/Sub push delivery is configured, but MapWolverinePubsubPush() from WolverineFx.Pubsub.AspNetCore was never called, so push requests will get 404", endpoint.Uri);

        var hasDeadLetterPolicy = endpoint.Server.Subscription.Options.DeadLetterPolicy != null ||
                                  endpoint.ObservedHasDeadLetterPolicy == true;

        if (endpoint.DeadLetterName.IsEmpty() && !hasDeadLetterPolicy)
            logger.LogWarning("{Uri}: This push endpoint has neither a Wolverine dead letter topic nor a subscription DeadLetterPolicy, so MoveToErrorQueue acknowledges and drops failed messages. Call EnableDeadLettering() or configure a DeadLetterPolicy", endpoint.Uri);

        if (!hasDeadLetterPolicy && runtime is WolverineRuntime wolverineRuntime &&
            (wolverineRuntime.Handlers.Failures.AnyRequeuePolicies() ||
             wolverineRuntime.Handlers.Chains.Any(x => x.Failures.AnyRequeuePolicies())))
            logger.LogWarning("{Uri}: Requeue policies are configured, but this push subscription has no DeadLetterPolicy, so Pub/Sub sends no deliveryAttempt and attempt counts restart at 1 on every redelivery", endpoint.Uri);
    }
}
```

- [ ] **Step 4: Hook push initialization into `PubsubEndpoint.InitializeAsync` (L346)**

At the very top of `InitializeAsync`, before the existing `if (IsExistingSubscription)` block:
```csharp
        if (DeliveryMode == PubsubDeliveryMode.Push && !_hasInitialized)
        {
            await initializePushAsync(logger);
            return;
        }
```
Add the method:
```csharp
    private async Task initializePushAsync(ILogger logger)
    {
        var runtime = Runtime ?? throw new InvalidOperationException($"{Uri}: endpoint was not compiled before initialization");

        PubsubPushValidation.AssertValid(this, runtime);

        try
        {
            if (_transport.AutoProvision && !IsExistingSubscription)
            {
                await SetupAsync(logger);
            }

            await readSubscriptionAsync(logger);

            if (_transport.AutoPurgeAllQueues)
            {
                await PurgeAsync(logger);
            }
        }
        catch (Exception ex)
        {
            throw new WolverinePubsubTransportException(
                $"{Uri}: Error trying to initialize Google Cloud Platform Pub/Sub push endpoint", ex);
        }

        PubsubPushValidation.LogWarnings(this, runtime, logger);
        _hasInitialized = true;
    }

    // Spec §5.2 "startup read": the real ack deadline and dead letter policy, whoever created the subscription
    private async Task readSubscriptionAsync(ILogger logger)
    {
        if (_transport.SubscriberApiClient is null) return;

        try
        {
            var subscription = await _transport.SubscriberApiClient.GetSubscriptionAsync(
                Server.Subscription.Name,
                CallSettings.FromExpiration(Expiration.FromTimeout(TimeSpan.FromSeconds(5))));

            ObservedAckDeadlineSeconds = subscription.AckDeadlineSeconds;
            ObservedHasDeadLetterPolicy = subscription.DeadLetterPolicy != null;
        }
        catch (Exception e)
        {
            logger.LogWarning(e,
                "{Uri}: Could not read Pub/Sub subscription {Subscription} at startup (needs pubsub.subscriptions.get); using the configured ack deadline and dead letter settings instead",
                Uri, Server.Subscription.Name);
        }
    }
```
`Expiration` and `CallSettings` come from `Google.Api.Gax` / `Google.Api.Gax.Grpc`, which are already imported in this file.

- [ ] **Step 5: Push config on create, and `ModifyPushConfig` on existing (`provisionAsync`, L207-252)**

In the `new Subscription { ... }` initializer path, after the `Filter` block (L233-236), add:
```csharp
            if (DeliveryMode == PubsubDeliveryMode.Push)
            {
                request.PushConfig = buildPushConfig();
            }
```
Replace the `catch (RpcException ex) when (ex.StatusCode == StatusCode.AlreadyExists)` body with:
```csharp
            logger.LogInformation("{Uri}: Google Cloud Platform Pub/Sub subscription \"{Subscription}\" already exists",
                Uri, subscriptionName);

            if (DeliveryMode == PubsubDeliveryMode.Push)
            {
                await alignPushConfigAsync(logger, clients, subscriptionName);
            }
```
Add the helpers:
```csharp
    private PushConfig buildPushConfig()
    {
        var config = new PushConfig { PushEndpoint = PushUrl! };

        if (EffectiveServiceAccountEmail.IsNotEmpty())
        {
            config.OidcToken = new PushConfig.Types.OidcToken
            {
                ServiceAccountEmail = EffectiveServiceAccountEmail,
                Audience = EffectiveAudience ?? string.Empty
            };
        }

        return config;
    }

    // The Cloud Run URL can change between deployments, and an existing pull subscription is converted to push
    private async Task alignPushConfigAsync(ILogger logger, PubsubClientSet clients, SubscriptionName subscriptionName)
    {
        var existing = await clients.SubscriberApiClient!.GetSubscriptionAsync(subscriptionName);
        var wanted = buildPushConfig();

        if (existing.PushConfig?.PushEndpoint == wanted.PushEndpoint &&
            Equals(existing.PushConfig?.OidcToken, wanted.OidcToken))
        {
            return;
        }

        await clients.SubscriberApiClient.ModifyPushConfigAsync(subscriptionName, wanted);

        logger.LogInformation(
            existing.PushConfig?.PushEndpoint.IsEmpty() ?? true
                ? "{Uri}: Converted Pub/Sub subscription \"{Subscription}\" from pull to push, endpoint {PushEndpoint}"
                : "{Uri}: Updated Pub/Sub subscription \"{Subscription}\" push endpoint to {PushEndpoint}",
            Uri, subscriptionName, wanted.PushEndpoint);
    }
```

- [ ] **Step 6: Purge push subscriptions with `Seek` (`PurgeAsync`, L295)**

At the top of `PurgeAsync`, after the `if (_transport.SubscriberApiClient is null || !IsListener) return;` guard:
```csharp
        if (DeliveryMode == PubsubDeliveryMode.Push)
        {
            // Pull on a push subscription is undocumented; Seek to now acknowledges every earlier message
            await _transport.SubscriberApiClient.SeekAsync(new SeekRequest
            {
                SubscriptionAsSubscriptionName = Server.Subscription.Name,
                Time = Timestamp.FromDateTime(DateTime.UtcNow)
            });
            return;
        }
```
Add `using Google.Protobuf.WellKnownTypes;`. If `Duration` becomes ambiguous, alias it as `Timestamp = Google.Protobuf.WellKnownTypes.Timestamp`.

- [ ] **Step 7: Run the validation tests to verify they pass**

Run: same as Step 2. Expected: 9 PASS.

- [ ] **Step 8: Write the emulator provisioning tests**

```csharp
using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Wolverine.Pubsub.Tests.Push;

public class push_provisioning
{
    private static Task<IHost> startAsync(string topic, string baseUrl, bool push = true) =>
        Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Discovery.DisableConventionalDiscovery();
                var pubsub = opts.UsePubsubTesting().AutoProvision();

                if (push)
                {
                    pubsub.ConfigurePushDelivery(p =>
                    {
                        p.BaseUrl = baseUrl;
                        p.AllowUnauthenticated();
                    });
                    opts.ListenToPubsubTopic(topic).UsePushDelivery();
                }
                else
                {
                    opts.ListenToPubsubTopic(topic);
                }
            })
            .StartAsync(TestContext.Current.CancellationToken);

    private static async Task<Subscription> subscriptionAsync(string topic)
    {
        var client = await new SubscriberServiceApiClientBuilder
            { EmulatorDetection = Google.Api.Gax.EmulatorDetection.EmulatorOnly }.BuildAsync();
        return await client.GetSubscriptionAsync(new SubscriptionName("wolverine", topic));
    }

    [Fact]
    public async Task creates_a_push_subscription()
    {
        Assert.SkipUnless(await TestingExtensions.IsEmulatorAvailable(), "Pub/Sub emulator is not available");
        var topic = $"push-create-{Guid.NewGuid():N}";

        using var host = await startAsync(topic, "http://localhost:5999");

        (await subscriptionAsync(topic)).PushConfig.PushEndpoint
            .ShouldBe($"http://localhost:5999/_wolverine/pubsub/{topic}");
    }

    [Fact]
    public async Task updates_the_push_endpoint_when_the_url_changes()
    {
        Assert.SkipUnless(await TestingExtensions.IsEmulatorAvailable(), "Pub/Sub emulator is not available");
        var topic = $"push-update-{Guid.NewGuid():N}";

        using (await startAsync(topic, "http://localhost:5999")) { }
        using var second = await startAsync(topic, "http://localhost:6001");

        (await subscriptionAsync(topic)).PushConfig.PushEndpoint
            .ShouldBe($"http://localhost:6001/_wolverine/pubsub/{topic}");
    }

    [Fact]
    public async Task converts_an_existing_pull_subscription_to_push()
    {
        Assert.SkipUnless(await TestingExtensions.IsEmulatorAvailable(), "Pub/Sub emulator is not available");
        var topic = $"push-convert-{Guid.NewGuid():N}";

        // Not Serverless for the pull host, so its listener behaves as today
        using (await Host.CreateDefaultBuilder()
                   .UseWolverine(opts =>
                   {
                       opts.Discovery.DisableConventionalDiscovery();
                       opts.UsePubsubTesting().AutoProvision();
                       opts.ListenToPubsubTopic(topic);
                   })
                   .StartAsync(TestContext.Current.CancellationToken)) { }

        (await subscriptionAsync(topic)).PushConfig.PushEndpoint.ShouldBeEmpty();

        using var push = await startAsync(topic, "http://localhost:5999");

        (await subscriptionAsync(topic)).PushConfig.PushEndpoint.ShouldNotBeEmpty();
    }

    [Fact]
    public async Task purge_on_startup_uses_seek_and_does_not_fail()
    {
        Assert.SkipUnless(await TestingExtensions.IsEmulatorAvailable(), "Pub/Sub emulator is not available");
        var topic = $"push-purge-{Guid.NewGuid():N}";

        using var host = await Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Discovery.DisableConventionalDiscovery();
                opts.UsePubsubTesting().AutoProvision().AutoPurgeOnStartup()
                    .ConfigurePushDelivery(p =>
                    {
                        p.BaseUrl = "http://localhost:5999";
                        p.AllowUnauthenticated();
                    });
                opts.ListenToPubsubTopic(topic).UsePushDelivery();
            })
            .StartAsync(TestContext.Current.CancellationToken);
    }
}
```

- [ ] **Step 9: Run them with the emulator**

Run: `docker compose up -d gcp-pubsub`, then `dotnet test src/Transports/GCP/Wolverine.Pubsub.Tests/Wolverine.Pubsub.Tests.csproj -f net9.0 --filter "FullyQualifiedName~push_provisioning"`
Expected: 4 PASS. Without the emulator they are reported as skipped.

- [ ] **Step 10: Run the existing provisioning regression tests**

Run: `dotnet test src/Transports/GCP/Wolverine.Pubsub.Tests/Wolverine.Pubsub.Tests.csproj -f net9.0 --filter "FullyQualifiedName~PubsubEndpointTests|FullyQualifiedName~StatefulResourceSmokeTests|FullyQualifiedName~send_and_receive"`
Expected: all PASS (pull mode unchanged).

- [ ] **Step 11: Commit**

```bash
git add src/Transports/GCP/Wolverine.Pubsub src/Transports/GCP/Wolverine.Pubsub.Tests/Push
git commit -m "Validate, provision and read Pub/Sub push subscriptions at startup"
```

---

### Task 5: The push processor

**Files:**
- Create: `src/Transports/GCP/Wolverine.Pubsub/Push/PubsubPushProcessor.cs`
- Modify: `src/Transports/GCP/Wolverine.Pubsub/PubsubConfiguration.cs` (restore the `AddSingleton` line from Task 1 Step 7)
- Test: `src/Transports/GCP/Wolverine.Pubsub.Tests/Push/PushTestSupport.cs`
- Test: `src/Transports/GCP/Wolverine.Pubsub.Tests/Push/push_processor.cs`

**Interfaces:**
- Consumes:
  - `PubsubPushRequest`, `PubsubPushResult` and `PubsubPushDelivery` (Tasks 2–3);
  - `InlineReceiver(Endpoint, IWolverineRuntime, IHandlerPipeline)` and `InlineReceiver.ReceivedAsync(IListener, Envelope[], CancellationToken)` (PR 1);
  - `HandlerPipeline(WolverineRuntime, IExecutorFactory, Endpoint)` (internal ctor);
  - `Endpoint.RulesForIncoming()` (internal);
  - `TenantIdRule` (internal);
  - `EnvelopeSerializer.ReadMany(byte[])`;
  - `WolverineRuntime.FullyStarted` (internal `Task`).
- Produces:
  - `public class PubsubPushProcessor(IWolverineRuntime runtime)`;
  - `public bool TryFindEndpoint(string endpointName, out PubsubEndpoint? endpoint)`;
  - `public Task<PubsubPushResult> ProcessAsync(string endpointName, PubsubPushRequest request, CancellationToken cancellation)`;
  - `public void LatchAll()`.

- [ ] **Step 1: Write the shared test support**

```csharp
using System.Collections.Concurrent;
using System.Text.Json;
using Google.Cloud.PubSub.V1;
using JasperFx.Core.Reflection;
using Microsoft.Extensions.Hosting;
using Wolverine.Pubsub.Internal;
using Wolverine.Pubsub.Push;
using Wolverine.Runtime;
using Wolverine.Util;

namespace Wolverine.Pubsub.Tests.Push;

public record PushPing(string Name);
public record PushShipped(string Name);

public static class PushPingHandler
{
    public static readonly ConcurrentQueue<(string Name, int Attempts, string? TenantId)> Handled = new();

    public static void Handle(PushPing ping, Envelope envelope)
    {
        if (ping.Name == "throw") throw new DivideByZeroException();
        Handled.Enqueue((ping.Name, envelope.Attempts, envelope.TenantId));
    }
}

public static class PushTestSupport
{
    public static Task<IHost> StartPushHostAsync(string topic, Action<WolverineOptions>? configure = null) =>
        Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Discovery.DisableConventionalDiscovery().IncludeType(typeof(PushPingHandler));
                opts.UsePubsubTesting().ConfigurePushDelivery(p => p.AllowUnauthenticated());
                opts.ListenToPubsubTopic(topic).UsePushDelivery();
                configure?.Invoke(opts);
            })
            .StartAsync(TestContext.Current.CancellationToken);

    public static PubsubPushRequest RequestFor(IHost host, string topic, object message,
        string? subscriptionProject = "wolverine", int? deliveryAttempt = null)
    {
        var runtime = host.GetRuntime();
        var endpoint = runtime.Options.Transports.GetOrCreate<PubsubTransport>().Topics[topic];

        var envelope = new Envelope(message)
        {
            Data = JsonSerializer.SerializeToUtf8Bytes(message, message.GetType()),
            ContentType = "application/json",
            MessageType = message.GetType().ToMessageTypeName()
        };

        var pubsubMessage = new PubsubMessage { MessageId = Guid.NewGuid().ToString() };
        new PubsubEnvelopeMapper(endpoint).MapEnvelopeToOutgoing(envelope, pubsubMessage);

        var json = JsonSerializer.Serialize(new
        {
            message = new
            {
                data = pubsubMessage.Data.ToBase64(),
                attributes = pubsubMessage.Attributes.ToDictionary(x => x.Key, x => x.Value),
                messageId = pubsubMessage.MessageId,
                publishTime = DateTimeOffset.UtcNow.ToString("O")
            },
            subscription = $"projects/{subscriptionProject}/subscriptions/{endpoint.Server.Subscription.Name.SubscriptionId}",
            deliveryAttempt
        });

        PubsubPushRequest.TryParse(System.Text.Encoding.UTF8.GetBytes(json), out var request, out var error)
            .ShouldBeTrue(error);
        return request!;
    }

    public static WolverineRuntime GetRuntime(this IHost host) =>
        (WolverineRuntime)host.Services.GetRequiredService<IWolverineRuntime>();
}
```
Add `using Microsoft.Extensions.DependencyInjection;`. `ToMessageTypeName()` is in `Wolverine.Util`. If the build reports it elsewhere, use the namespace the compiler suggests.

- [ ] **Step 2: Write the failing processor tests**

```csharp
using Microsoft.Extensions.DependencyInjection;
using Wolverine.ErrorHandling;
using Wolverine.Pubsub.Push;
using Xunit;
using static Wolverine.Pubsub.Tests.Push.PushTestSupport;

namespace Wolverine.Pubsub.Tests.Push;

public class push_processor
{
    private static PubsubPushProcessor processorFor(Microsoft.Extensions.Hosting.IHost host)
        => host.Services.GetRequiredService<PubsubPushProcessor>();

    [Fact]
    public async Task handles_the_message_and_acks()
    {
        var topic = $"push-proc-{Guid.NewGuid():N}";
        using var host = await StartPushHostAsync(topic);
        var name = Guid.NewGuid().ToString();

        var result = await processorFor(host).ProcessAsync(topic, RequestFor(host, topic, new PushPing(name)),
            TestContext.Current.CancellationToken);

        result.StatusCode.ShouldBe(204);
        PushPingHandler.Handled.ShouldContain(x => x.Name == name);
    }

    [Fact]
    public async Task unknown_endpoint_is_404()
    {
        var topic = $"push-proc-{Guid.NewGuid():N}";
        using var host = await StartPushHostAsync(topic);

        var result = await processorFor(host).ProcessAsync("nope", RequestFor(host, topic, new PushPing("x")),
            TestContext.Current.CancellationToken);

        result.StatusCode.ShouldBe(404);
    }

    [Fact]
    public async Task subscription_from_an_unknown_project_is_400()
    {
        var topic = $"push-proc-{Guid.NewGuid():N}";
        using var host = await StartPushHostAsync(topic);

        var result = await processorFor(host).ProcessAsync(topic,
            RequestFor(host, topic, new PushPing("x"), subscriptionProject: "someone-else"),
            TestContext.Current.CancellationToken);

        result.StatusCode.ShouldBe(400);
    }

    [Fact]
    public async Task tenant_project_subscription_stamps_the_tenant()
    {
        var topic = $"push-proc-{Guid.NewGuid():N}";
        using var host = await StartPushHostAsync(topic,
            opts => opts.ConfigurePubsub().AddTenant("tenant-a", "tenant-a-project"));
        var name = Guid.NewGuid().ToString();

        var result = await processorFor(host).ProcessAsync(topic,
            RequestFor(host, topic, new PushPing(name), subscriptionProject: "tenant-a-project"),
            TestContext.Current.CancellationToken);

        result.StatusCode.ShouldBe(204);
        PushPingHandler.Handled.ShouldContain(x => x.Name == name && x.TenantId == "tenant-a");
    }

    [Theory]
    [InlineData(null, 1)]
    [InlineData(0, 1)]
    [InlineData(1, 1)]
    [InlineData(4, 4)]
    public async Task delivery_attempt_seeds_attempts(int? deliveryAttempt, int expected)
    {
        var topic = $"push-proc-{Guid.NewGuid():N}";
        using var host = await StartPushHostAsync(topic);
        var name = Guid.NewGuid().ToString();

        await processorFor(host).ProcessAsync(topic,
            RequestFor(host, topic, new PushPing(name), deliveryAttempt: deliveryAttempt),
            TestContext.Current.CancellationToken);

        PushPingHandler.Handled.ShouldContain(x => x.Name == name && x.Attempts == expected);
    }

    [Fact]
    public async Task requeue_policy_is_503()
    {
        var topic = $"push-proc-{Guid.NewGuid():N}";
        using var host = await StartPushHostAsync(topic,
            opts => opts.OnException<DivideByZeroException>().Requeue());

        var result = await processorFor(host).ProcessAsync(topic, RequestFor(host, topic, new PushPing("throw")),
            TestContext.Current.CancellationToken);

        result.StatusCode.ShouldBe(503);
    }

    [Fact]
    public async Task latched_processor_defers_with_503()
    {
        var topic = $"push-proc-{Guid.NewGuid():N}";
        using var host = await StartPushHostAsync(topic);
        var processor = processorFor(host);
        var name = Guid.NewGuid().ToString();

        processor.LatchAll();
        var result = await processor.ProcessAsync(topic, RequestFor(host, topic, new PushPing(name)),
            TestContext.Current.CancellationToken);

        result.StatusCode.ShouldBe(503);
        PushPingHandler.Handled.ShouldNotContain(x => x.Name == name);
    }

    [Fact]
    public async Task unmappable_message_is_acked()
    {
        var topic = $"push-proc-{Guid.NewGuid():N}";
        using var host = await StartPushHostAsync(topic);

        // "batched" data that is not a serialized envelope batch cannot be mapped
        var endpoint = host.GetRuntime().Options.Transports.GetOrCreate<PubsubTransport>().Topics[topic];
        var json = $$"""
            { "message": { "data": "AAEC", "attributes": { "batched": "1" }, "messageId": "1" },
              "subscription": "{{endpoint.Server.Subscription.Name}}" }
            """;
        PubsubPushRequest.TryParse(System.Text.Encoding.UTF8.GetBytes(json), out var request, out _);

        var result = await processorFor(host).ProcessAsync(topic, request!, TestContext.Current.CancellationToken);

        result.StatusCode.ShouldBe(204);
    }
}
```

These hosts don't use `AutoProvision`, so they need no emulator. The startup subscription read logs a warning after at most 5 s per endpoint if the emulator is down.

- [ ] **Step 3: Run them to verify they fail**

Run: `dotnet test src/Transports/GCP/Wolverine.Pubsub.Tests/Wolverine.Pubsub.Tests.csproj -f net9.0 --filter "FullyQualifiedName~push_processor"`
Expected: compile error (`PubsubPushProcessor` missing).

- [ ] **Step 4: Implement `PubsubPushProcessor`, and restore `Options.Services.AddSingleton<Push.PubsubPushProcessor>();` in `ConfigurePushDelivery`**

```csharp
using System.Diagnostics;
using Google.Cloud.PubSub.V1;
using ImTools;
using JasperFx.Core;
using Microsoft.Extensions.Logging;
using Wolverine.Pubsub.Internal;
using Wolverine.Runtime;
using Wolverine.Runtime.Serialization;
using Wolverine.Runtime.WorkerQueues;

namespace Wolverine.Pubsub.Push;

/// <summary>
/// Runs one Pub/Sub push request through its endpoint's handler pipeline inside the request (spec §3.2). Has no HTTP
/// types; WolverineFx.Pubsub.AspNetCore turns the result into a status code.
/// </summary>
public class PubsubPushProcessor
{
    private readonly WolverineRuntime _runtime;
    private readonly ILogger _logger;
    private ImHashMap<string, PushEndpointState> _states = ImHashMap<string, PushEndpointState>.Empty;
    private readonly object _lock = new();
    private volatile bool _latched;

    public PubsubPushProcessor(IWolverineRuntime runtime)
    {
        _runtime = (WolverineRuntime)runtime;
        _logger = runtime.LoggerFactory.CreateLogger<PubsubPushProcessor>();
    }

    private PubsubTransport transport => _runtime.Options.Transports.GetOrCreate<PubsubTransport>();

    /// <summary>
    /// Find the push-mode endpoint for a route value. Answered from the per-endpoint cache after the first request
    /// </summary>
    public bool TryFindEndpoint(string endpointName, out PubsubEndpoint? endpoint)
    {
        endpoint = stateFor(endpointName)?.Endpoint;
        return endpoint != null;
    }

    private PubsubEndpoint? scanForPushEndpoint(string endpointName) =>
        transport.Topics.FirstOrDefault(x =>
            x.DeliveryMode == PubsubDeliveryMode.Push && x.EndpointName == endpointName);

    /// <summary>
    /// Stop handing new push requests to handlers; they are answered 503 so Pub/Sub redelivers elsewhere.
    /// Called on ApplicationStopping
    /// </summary>
    public void LatchAll()
    {
        _latched = true;
        foreach (var entry in _states.Enumerate()) entry.Value.Receiver.Latch();
    }

    public async Task<PubsubPushResult> ProcessAsync(string endpointName, PubsubPushRequest request,
        CancellationToken cancellation)
    {
        if (_latched || !_runtime.FullyStarted.IsCompleted || _runtime.Cancellation.IsCancellationRequested)
        {
            return PubsubPushResult.RetryLater("the host is not running");
        }

        var state = stateFor(endpointName);
        if (state == null)
        {
            return PubsubPushResult.NotFound($"no Pub/Sub push endpoint named '{endpointName}'");
        }

        if (!state.TryMatchSubscription(request.Subscription, out var clients, out var tenantId))
        {
            return PubsubPushResult.BadRequest(
                $"subscription '{request.Subscription}' does not belong to push endpoint '{endpointName}'");
        }

        var delivery = new PubsubPushDelivery(state.Endpoint, clients!, state.Receiver.Pipeline, _logger);

        Envelope[] envelopes;
        try
        {
            envelopes = readEnvelopes(state.Endpoint, request.Message);
        }
        catch (Exception e)
        {
            return await handleUnmappableAsync(state.Endpoint, clients!, request.Message, e);
        }

        if (envelopes.Length == 0) return PubsubPushResult.Ack;

        foreach (var envelope in envelopes)
        {
            // Executor increments Attempts before each run, so the first execution sees deliveryAttempt
            if (request.DeliveryAttempt is > 1) envelope.Attempts = request.DeliveryAttempt.Value - 1;

            foreach (var rule in state.IncomingRules) rule.Modify(envelope);
            if (tenantId != null) envelope.TenantId = tenantId;
        }

        var stopwatch = Stopwatch.StartNew();
        await state.Receiver.ReceivedAsync(delivery, envelopes, cancellation);
        stopwatch.Stop();

        // No timer: Cloud Run gives no CPU between requests, so the overrun is reported after the fact
        var deadline = state.Endpoint.ObservedAckDeadlineSeconds ??
                       state.Endpoint.Server.Subscription.Options.AckDeadlineSeconds;
        if (stopwatch.Elapsed > TimeSpan.FromSeconds(deadline))
        {
            _logger.LogWarning(
                "{Uri}: Push request for message {MessageId} took {Elapsed}, longer than the subscription's ack deadline of {Deadline}s, so Pub/Sub has probably redelivered it already",
                state.Endpoint.Uri, request.Message.MessageId, stopwatch.Elapsed, deadline);
        }

        return delivery.ResultFor(envelopes);
    }

    private Envelope[] readEnvelopes(PubsubEndpoint endpoint, PubsubMessage message)
    {
        if (message.Attributes.ContainsKey("batched"))
        {
            return EnvelopeSerializer.ReadMany(message.Data.ToByteArray()).ToArray();
        }

        var envelope = new Envelope();
        endpoint.EnvelopeMapper!.MapIncomingToEnvelope(envelope, message);
        return [envelope];
    }

    private async Task<PubsubPushResult> handleUnmappableAsync(PubsubEndpoint endpoint, PubsubClientSet clients,
        PubsubMessage message, Exception exception)
    {
        _logger.LogError(exception,
            "{Uri}: Could not map Pub/Sub push message {MessageId}; it is acknowledged so it is not redelivered forever",
            endpoint.Uri, message.MessageId);

        if (endpoint.DeadLetterName.IsEmpty()) return PubsubPushResult.Ack;

        try
        {
            var deadLetter = endpoint.Transport.Topics[endpoint.DeadLetterName!];
            await clients.PublisherApiClient!.PublishAsync(new PublishRequest
            {
                TopicAsTopicName = deadLetter.TopicNameFor(clients.ProjectId),
                Messages = { message }
            });
            return PubsubPushResult.Ack;
        }
        catch (Exception e)
        {
            _logger.LogError(e, "{Uri}: Could not publish unmappable message {MessageId} to the dead letter topic",
                endpoint.Uri, message.MessageId);
            return PubsubPushResult.Failed("dead letter publish of an unmappable message failed");
        }
    }

    private PushEndpointState? stateFor(string endpointName)
    {
        if (_states.TryFind(endpointName, out var state)) return state;

        lock (_lock)
        {
            if (_states.TryFind(endpointName, out state)) return state;
            var endpoint = scanForPushEndpoint(endpointName);
            if (endpoint == null) return null;

            state = new PushEndpointState(endpoint, transport, _runtime);
            if (_latched) state.Receiver.Latch();
            _states = _states.AddOrUpdate(endpointName, state);
            return state;
        }
    }

    private sealed class PushEndpointState
    {
        private readonly string _defaultSubscription;
        private readonly PubsubClientSet _defaultClients;
        private readonly (string Subscription, string TenantId, PubsubClientSet Clients)[] _tenants;

        public PushEndpointState(PubsubEndpoint endpoint, PubsubTransport transport, WolverineRuntime runtime)
        {
            Endpoint = endpoint;
            endpoint.EnvelopeMapper ??= endpoint.BuildMapper(runtime);

            var pipeline = new HandlerPipeline(runtime, runtime, endpoint) { TelemetryEnabled = endpoint.TelemetryEnabled };
            Receiver = new InlineReceiver(endpoint, runtime, pipeline);
            IncomingRules = endpoint.RulesForIncoming().ToArray();

            _defaultSubscription = endpoint.SubscriptionNameFor(transport.ProjectId).ToString();
            _defaultClients = transport.DefaultClients;
            _tenants = transport.Tenants
                .Select(t => (endpoint.SubscriptionNameFor(t.ProjectId).ToString(), t.TenantId, transport.GetTenantClients(t)))
                .ToArray();
        }

        public PubsubEndpoint Endpoint { get; }
        public InlineReceiver Receiver { get; }
        public IEnvelopeRule[] IncomingRules { get; }

        public bool TryMatchSubscription(string subscription, out PubsubClientSet? clients, out string? tenantId)
        {
            if (subscription == _defaultSubscription)
            {
                clients = _defaultClients;
                tenantId = null;
                return true;
            }

            foreach (var tenant in _tenants)
            {
                if (subscription != tenant.Subscription) continue;
                clients = tenant.Clients;
                tenantId = tenant.TenantId;
                return true;
            }

            clients = null;
            tenantId = null;
            return false;
        }
    }
}
```

Notes for the implementer:
- `SubscriptionNameFor` and `TopicNameFor` are internal on `PubsubEndpoint` (`L50-66`), and `DefaultClients` and `GetTenantClients` are internal on `PubsubTransport`, all in the same assembly.
- `PubsubTenant` exposes `TenantId` and `ProjectId`; check `Internal/PubsubTenant.cs` for the exact property names.
- `transport.Tenants` is a `LightweightCache<string, PubsubTenant>`, and enumerating it yields `PubsubTenant` values, as `PubsubEndpoint.BuildListenerAsync` does at L394.
- `ImTools.ImHashMap` is what `JasperFx.Core` re-exports in this repo. If `using ImTools;` doesn't resolve, use the namespace `WolverineRuntime.Routing.cs` uses for `ImHashMap`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: same as Step 3. Expected: 11 PASS.

- [ ] **Step 6: Commit**

```bash
git add src/Transports/GCP/Wolverine.Pubsub src/Transports/GCP/Wolverine.Pubsub.Tests/Push
git commit -m "Process Pub/Sub push requests inline with per-request settlement"
```

---

### Task 6: `WolverineFx.Pubsub.AspNetCore`: route and authentication

**Files:**
- Create: `src/Transports/GCP/Wolverine.Pubsub.AspNetCore/Wolverine.Pubsub.AspNetCore.csproj`
- Create: `src/Transports/GCP/Wolverine.Pubsub.AspNetCore/AssemblyAttributes.cs`
- Create: `src/Transports/GCP/Wolverine.Pubsub.AspNetCore/PubsubPushTokenValidator.cs`
- Create: `src/Transports/GCP/Wolverine.Pubsub.AspNetCore/PubsubPushAuthenticator.cs`
- Create: `src/Transports/GCP/Wolverine.Pubsub.AspNetCore/PubsubPushEndpointRouteBuilderExtensions.cs`
- Modify: `src/Transports/GCP/Wolverine.Pubsub/AssemblyAttributes.cs`
- Modify: `src/Transports/GCP/Wolverine.Pubsub.Tests/Wolverine.Pubsub.Tests.csproj`
- Modify: `wolverine.slnx` (GCP folder, L324-327), `build/build.cs` (`NugetProjects`, after L320)
- Test: `src/Transports/GCP/Wolverine.Pubsub.Tests/Push/push_http_endpoint.cs`

**Interfaces:**
- Consumes: `PubsubPushProcessor` and `PubsubPushRequest` (Task 5); `PubsubPushSettings.IsRouteMapped`, `RoutePrefix` and `Authentication` (Task 1).
- Produces:
  - `public static IEndpointConventionBuilder MapWolverinePubsubPush(this IEndpointRouteBuilder endpoints)`;
  - `internal interface IPubsubPushTokenValidator { Task<GoogleJsonWebSignature.Payload> ValidateAsync(string token, string audience); }`. Tests swap it through DI.

- [ ] **Step 1: Create the project**

`Wolverine.Pubsub.AspNetCore.csproj`:
```xml
<Project Sdk="Microsoft.NET.Sdk">

    <PropertyGroup>
        <PackageId>WolverineFx.Pubsub.AspNetCore</PackageId>
        <Description>Google Cloud Platform Pub/Sub push delivery endpoint for Wolverine applications on ASP.NET Core</Description>
    </PropertyGroup>

    <ItemGroup>
        <ProjectReference Include="..\Wolverine.Pubsub\Wolverine.Pubsub.csproj"/>
        <FrameworkReference Include="Microsoft.AspNetCore.App"/>
    </ItemGroup>

</Project>
```

`AssemblyAttributes.cs`:
```csharp
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("Wolverine.Pubsub.Tests")]
```

In `src/Transports/GCP/Wolverine.Pubsub/AssemblyAttributes.cs`, add:
```csharp
[assembly: InternalsVisibleTo("Wolverine.Pubsub.AspNetCore")]
```

In `wolverine.slnx`, inside `<Folder Name="/Transports/GCP/">`, add:
```xml
    <Project Path="src/Transports/GCP/Wolverine.Pubsub.AspNetCore/Wolverine.Pubsub.AspNetCore.csproj" />
```

In `build/build.cs` `NugetProjects`, after `Solution.Transports.GCP.Wolverine_Pubsub,` add:
```csharp
                Solution.Transports.GCP.Wolverine_Pubsub_AspNetCore,
```

In `Wolverine.Pubsub.Tests.csproj`, add `<ProjectReference Include="..\Wolverine.Pubsub.AspNetCore\Wolverine.Pubsub.AspNetCore.csproj"/>` and `<PackageReference Include="Alba" />`.

- [ ] **Step 2: Write the failing HTTP tests**

```csharp
using Alba;
using Google.Apis.Auth;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Wolverine.Pubsub.AspNetCore;
using Xunit;
using static Wolverine.Pubsub.Tests.Push.PushTestSupport;

namespace Wolverine.Pubsub.Tests.Push;

public class push_http_endpoint
{
    private static async Task<IAlbaHost> hostAsync(string topic, Action<PubsubPushSettings> auth,
        IPubsubPushTokenValidator? validator = null)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Host.UseWolverine(opts =>
        {
            opts.Durability.Mode = DurabilityMode.Serverless;
            opts.Discovery.DisableConventionalDiscovery().IncludeType(typeof(PushPingHandler));
            opts.UsePubsubTesting().ConfigurePushDelivery(auth);
            opts.ListenToPubsubTopic(topic).UsePushDelivery();
        });
        if (validator != null) builder.Services.AddSingleton(validator);

        return await AlbaHost.For(builder, app => app.MapWolverinePubsubPush());
    }

    private static string bodyFor(IAlbaHost host, string topic, string name)
    {
        var request = RequestFor(host, topic, new PushPing(name));
        return System.Text.Json.JsonSerializer.Serialize(new
        {
            message = new
            {
                data = request.Message.Data.ToBase64(),
                attributes = request.Message.Attributes.ToDictionary(x => x.Key, x => x.Value),
                messageId = request.Message.MessageId
            },
            subscription = request.Subscription
        });
    }

    [Fact]
    public async Task valid_push_is_204()
    {
        var topic = $"push-http-{Guid.NewGuid():N}";
        await using var host = await hostAsync(topic, p => p.AllowUnauthenticated());

        await host.Scenario(x =>
        {
            x.Post.Text(bodyFor(host, topic, "ok")).ToUrl($"/_wolverine/pubsub/{topic}");
            x.StatusCodeShouldBe(204);
        });
    }

    [Fact]
    public async Task escaped_endpoint_names_still_route()
    {
        var topic = $"push-http~{Guid.NewGuid():N}";
        await using var host = await hostAsync(topic, p => p.AllowUnauthenticated());

        await host.Scenario(x =>
        {
            x.Post.Text(bodyFor(host, topic, "ok")).ToUrl($"/_wolverine/pubsub/{Uri.EscapeDataString(topic)}");
            x.StatusCodeShouldBe(204);
        });
    }

    [Fact]
    public async Task malformed_body_is_400()
    {
        var topic = $"push-http-{Guid.NewGuid():N}";
        await using var host = await hostAsync(topic, p => p.AllowUnauthenticated());

        await host.Scenario(x =>
        {
            x.Post.Text("not json").ToUrl($"/_wolverine/pubsub/{topic}");
            x.StatusCodeShouldBe(400);
        });
    }

    [Fact]
    public async Task unknown_endpoint_is_404()
    {
        var topic = $"push-http-{Guid.NewGuid():N}";
        await using var host = await hostAsync(topic, p => p.AllowUnauthenticated());

        await host.Scenario(x =>
        {
            x.Post.Text("{}").ToUrl("/_wolverine/pubsub/missing");
            x.StatusCodeShouldBe(404);
        });
    }

    [Fact]
    public async Task verify_oidc_without_a_token_is_401()
    {
        var topic = $"push-http-{Guid.NewGuid():N}";
        await using var host = await hostAsync(topic, p =>
        {
            p.BaseUrl = "https://svc.a.run.app";
            p.ServiceAccountEmail = "push@wolverine.iam.gserviceaccount.com";
            p.VerifyOidcToken();
        }, new FakeValidator("push@wolverine.iam.gserviceaccount.com"));

        await host.Scenario(x =>
        {
            x.Post.Text(bodyFor(host, topic, "x")).ToUrl($"/_wolverine/pubsub/{topic}");
            x.StatusCodeShouldBe(401);
        });
    }

    [Fact]
    public async Task verify_oidc_with_the_wrong_principal_is_403()
    {
        var topic = $"push-http-{Guid.NewGuid():N}";
        await using var host = await hostAsync(topic, p =>
        {
            p.BaseUrl = "https://svc.a.run.app";
            p.ServiceAccountEmail = "push@wolverine.iam.gserviceaccount.com";
            p.VerifyOidcToken();
        }, new FakeValidator("intruder@evil.iam.gserviceaccount.com"));

        await host.Scenario(x =>
        {
            x.Post.Text(bodyFor(host, topic, "x")).ToUrl($"/_wolverine/pubsub/{topic}");
            x.WithRequestHeader("Authorization", "Bearer anything");
            x.StatusCodeShouldBe(403);
        });
    }

    [Fact]
    public async Task verify_oidc_with_the_right_principal_is_204()
    {
        var topic = $"push-http-{Guid.NewGuid():N}";
        await using var host = await hostAsync(topic, p =>
        {
            p.BaseUrl = "https://svc.a.run.app";
            p.ServiceAccountEmail = "push@wolverine.iam.gserviceaccount.com";
            p.VerifyOidcToken();
        }, new FakeValidator("push@wolverine.iam.gserviceaccount.com"));

        await host.Scenario(x =>
        {
            x.Post.Text(bodyFor(host, topic, "ok")).ToUrl($"/_wolverine/pubsub/{topic}");
            x.WithRequestHeader("Authorization", "Bearer anything");
            x.StatusCodeShouldBe(204);
        });
    }

    [Fact]
    public async Task trust_cloud_run_iam_rejects_a_forwarded_token_for_another_principal()
    {
        var topic = $"push-http-{Guid.NewGuid():N}";
        await using var host = await hostAsync(topic, p =>
        {
            p.BaseUrl = "https://svc.a.run.app";
            p.ServiceAccountEmail = "push@wolverine.iam.gserviceaccount.com";
            p.TrustCloudRunIam();
        });

        // header.payload.signature where payload = {"email":"intruder@evil.iam.gserviceaccount.com"}
        var payload = Convert.ToBase64String("""{"email":"intruder@evil.iam.gserviceaccount.com"}"""u8.ToArray())
            .TrimEnd('=').Replace('+', '-').Replace('/', '_');

        await host.Scenario(x =>
        {
            x.Post.Text(bodyFor(host, topic, "x")).ToUrl($"/_wolverine/pubsub/{topic}");
            x.WithRequestHeader("Authorization", $"Bearer e30.{payload}.sig");
            x.StatusCodeShouldBe(403);
        });
    }

    [Fact]
    public async Task trust_cloud_run_iam_without_a_forwarded_token_is_204()
    {
        var topic = $"push-http-{Guid.NewGuid():N}";
        await using var host = await hostAsync(topic, p =>
        {
            p.BaseUrl = "https://svc.a.run.app";
            p.ServiceAccountEmail = "push@wolverine.iam.gserviceaccount.com";
            p.TrustCloudRunIam();
        });

        await host.Scenario(x =>
        {
            x.Post.Text(bodyFor(host, topic, "ok")).ToUrl($"/_wolverine/pubsub/{topic}");
            x.StatusCodeShouldBe(204);
        });
    }

    private class FakeValidator(string email) : IPubsubPushTokenValidator
    {
        public Task<GoogleJsonWebSignature.Payload> ValidateAsync(string token, string audience) =>
            Task.FromResult(new GoogleJsonWebSignature.Payload { Email = email, EmailVerified = true });
    }
}
```

`RequestFor(IHost …)` from Task 5 takes an `IHost`. `IAlbaHost` implements `IHost`, so it can be passed directly.

- [ ] **Step 3: Run them to verify they fail**

Run: `dotnet test src/Transports/GCP/Wolverine.Pubsub.Tests/Wolverine.Pubsub.Tests.csproj -f net9.0 --filter "FullyQualifiedName~push_http_endpoint"`
Expected: compile error.

- [ ] **Step 4: Implement the token validator**

```csharp
using Google.Apis.Auth;

namespace Wolverine.Pubsub.AspNetCore;

/// <summary>
/// Verifies a Google-signed OIDC token. An interface so tests can replace it
/// </summary>
internal interface IPubsubPushTokenValidator
{
    Task<GoogleJsonWebSignature.Payload> ValidateAsync(string token, string audience);
}

internal sealed class GooglePubsubPushTokenValidator : IPubsubPushTokenValidator
{
    // Checks signature (with cached Google certificates), expiry, both Google issuer forms and the audience
    public Task<GoogleJsonWebSignature.Payload> ValidateAsync(string token, string audience) =>
        GoogleJsonWebSignature.ValidateAsync(token,
            new GoogleJsonWebSignature.ValidationSettings { Audience = [audience] });
}
```

- [ ] **Step 5: Implement the authenticator**

```csharp
using System.Text;
using System.Text.Json;
using Google.Apis.Auth;
using JasperFx.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace Wolverine.Pubsub.AspNetCore;

/// <summary>
/// Spec §5.3. Returns null when the request may proceed, otherwise 401 or 403
/// </summary>
internal sealed class PubsubPushAuthenticator
{
    private readonly IPubsubPushTokenValidator _validator;
    private readonly ILogger _logger;

    public PubsubPushAuthenticator(IPubsubPushTokenValidator validator, ILogger<PubsubPushAuthenticator> logger)
    {
        _validator = validator;
        _logger = logger;
    }

    public async Task<int?> AuthenticateAsync(HttpRequest request, PubsubEndpoint endpoint)
    {
        var push = endpoint.Transport.Push;
        var expectedEmail = endpoint.EffectiveServiceAccountEmail;

        switch (push.Authentication)
        {
            case PubsubPushAuthentication.AllowUnauthenticated:
                return null;

            case PubsubPushAuthentication.TrustCloudRunIam:
                // Cloud Run already verified the token. Compare the email only when a forwarded token can be read;
                // Google does not document whether Cloud Run forwards it intact (spec §9 item 1)
                if (expectedEmail.IsEmpty()) return null;
                var unverifiedEmail = tryReadUnverifiedEmail(request);
                if (unverifiedEmail == null)
                {
                    _logger.LogDebug("No readable forwarded token on a Pub/Sub push request; relying on Cloud Run IAM alone");
                    return null;
                }

                return string.Equals(unverifiedEmail, expectedEmail, StringComparison.OrdinalIgnoreCase) ? null : 403;

            case PubsubPushAuthentication.VerifyOidcToken:
                var token = bearerToken(request);
                if (token == null) return 401;

                GoogleJsonWebSignature.Payload payload;
                try
                {
                    payload = await _validator.ValidateAsync(token, endpoint.EffectiveAudience!);
                }
                catch (InvalidJwtException e)
                {
                    _logger.LogInformation(e, "Rejected a Pub/Sub push request with an invalid OIDC token");
                    return 401;
                }

                return payload.EmailVerified &&
                       string.Equals(payload.Email, expectedEmail, StringComparison.OrdinalIgnoreCase)
                    ? null
                    : 403;

            default:
                // Startup validation makes this unreachable; fail closed anyway
                return 401;
        }
    }

    private static string? bearerToken(HttpRequest request)
    {
        var header = request.Headers.Authorization.ToString();
        return header.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase) ? header[7..].Trim() : null;
    }

    private static string? tryReadUnverifiedEmail(HttpRequest request)
    {
        var token = bearerToken(request);
        var parts = token?.Split('.');
        if (parts is not { Length: >= 2 }) return null;

        try
        {
            var payload = parts[1].Replace('-', '+').Replace('_', '/');
            payload = payload.PadRight(payload.Length + (4 - payload.Length % 4) % 4, '=');
            using var json = JsonDocument.Parse(Encoding.UTF8.GetString(Convert.FromBase64String(payload)));
            return json.RootElement.TryGetProperty("email", out var email) ? email.GetString() : null;
        }
        catch (Exception e) when (e is FormatException or JsonException)
        {
            return null;
        }
    }
}
```

- [ ] **Step 6: Implement the route mapping**

```csharp
using JasperFx.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Wolverine.Pubsub.Push;
using Wolverine.Runtime;

namespace Wolverine.Pubsub.AspNetCore;

public static class PubsubPushEndpointRouteBuilderExtensions
{
    /// <summary>
    /// Map the single POST route that receives Google Cloud Pub/Sub push requests for every endpoint configured
    /// with UsePushDelivery(). The route is ConfigurePushDelivery()'s RoutePrefix + "/{endpointName}"
    /// </summary>
    public static IEndpointConventionBuilder MapWolverinePubsubPush(this IEndpointRouteBuilder endpoints)
    {
        var services = endpoints.ServiceProvider;
        var runtime = services.GetRequiredService<IWolverineRuntime>();
        var push = runtime.Options.Transports.GetOrCreate<PubsubTransport>().Push;
        push.IsRouteMapped = true;

        var processor = services.GetRequiredService<PubsubPushProcessor>();
        services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping.Register(processor.LatchAll);

        var validator = services.GetService<IPubsubPushTokenValidator>() ?? new GooglePubsubPushTokenValidator();
        var authenticator = new PubsubPushAuthenticator(validator,
            services.GetRequiredService<ILogger<PubsubPushAuthenticator>>());
        var logger = services.GetRequiredService<ILoggerFactory>().CreateLogger("Wolverine.Pubsub.Push");

        var prefix = "/" + push.RoutePrefix.Trim('/');
        return endpoints.MapPost(prefix + "/{endpointName}", async (HttpContext context, string endpointName) =>
        {
            if (!processor.TryFindEndpoint(endpointName, out var endpoint))
            {
                context.Response.StatusCode = 404;
                return;
            }

            var denied = await authenticator.AuthenticateAsync(context.Request, endpoint!);
            if (denied.HasValue)
            {
                context.Response.StatusCode = denied.Value;
                return;
            }

            var body = await context.Request.Body.ReadAllBytesAsync();
            if (!PubsubPushRequest.TryParse(body, out var request, out var error))
            {
                logger.LogWarning("Rejected Pub/Sub push request for {EndpointName}: {Error}", endpointName, error);
                context.Response.StatusCode = 400;
                return;
            }

            var result = await processor.ProcessAsync(endpointName, request!, context.RequestAborted);
            if (!result.IsAck)
            {
                logger.LogInformation("Pub/Sub push request for {EndpointName} answered {StatusCode}: {Reason}",
                    endpointName, result.StatusCode, result.Reason);
            }

            context.Response.StatusCode = result.StatusCode;
        });
    }
}
```
- Minimal APIs URL-decode the `{endpointName}` route value, so an escaped `%7E` arrives as `~`.
- `ReadAllBytesAsync()` is the JasperFx.Core stream extension that `HttpTransportExecutor` uses.
- Remove any `using` that the build flags as unused; warnings are errors.

- [ ] **Step 7: Run the tests to verify they pass**

Run: same as Step 3. Expected: 9 PASS.

- [ ] **Step 8: Validate the pack list**

Run: `./build.ps1 ValidatePackList` (Windows) or `./build.sh ValidatePackList`
Expected: success. If it fails, the new project is missing from `NugetProjects`.

- [ ] **Step 9: Commit**

```bash
git add src/Transports/GCP/Wolverine.Pubsub.AspNetCore src/Transports/GCP/Wolverine.Pubsub/AssemblyAttributes.cs src/Transports/GCP/Wolverine.Pubsub.Tests wolverine.slnx build/build.cs
git commit -m "Add WolverineFx.Pubsub.AspNetCore with the Pub/Sub push route and authentication"
```

---

### Task 7: Cascades and dead-lettering against the emulator

**Files:**
- Test: `src/Transports/GCP/Wolverine.Pubsub.Tests/Push/push_with_emulator.cs`

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Write the tests**

```csharp
using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.DependencyInjection;
using Wolverine.Pubsub.Push;
using Xunit;
using static Wolverine.Pubsub.Tests.Push.PushTestSupport;

namespace Wolverine.Pubsub.Tests.Push;

public static class PushCascadeHandler
{
    public static PushShipped Handle(PushPing ping) => new(ping.Name);
}

public class push_with_emulator
{
    private static async Task<PubsubMessage[]> pullAsync(string subscriptionId)
    {
        var client = await new SubscriberServiceApiClientBuilder
            { EmulatorDetection = Google.Api.Gax.EmulatorDetection.EmulatorOnly }.BuildAsync();
        var response = await client.PullAsync(new SubscriptionName("wolverine", subscriptionId), 10);
        return response.ReceivedMessages.Select(x => x.Message).ToArray();
    }

    [Fact]
    public async Task cascade_is_published_before_the_response()
    {
        Assert.SkipUnless(await TestingExtensions.IsEmulatorAvailable(), "Pub/Sub emulator is not available");
        var topic = $"push-cascade-{Guid.NewGuid():N}";
        var shipped = $"push-shipped-{Guid.NewGuid():N}";

        using var host = await Microsoft.Extensions.Hosting.Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Discovery.DisableConventionalDiscovery().IncludeType(typeof(PushCascadeHandler));
                opts.UsePubsubTesting().AutoProvision()
                    .ConfigurePushDelivery(p =>
                    {
                        p.BaseUrl = "http://localhost:5999";
                        p.AllowUnauthenticated();
                    });
                opts.ListenToPubsubTopic(topic).UsePushDelivery();
                // a pull-mode listener on the cascade topic only so AutoProvision creates its subscription;
                // in Serverless it never starts, so the test can pull from it
                opts.ListenToPubsubTopic(shipped);
                opts.PublishMessage<PushShipped>().ToPubsubTopic(shipped);
            })
            .StartAsync(TestContext.Current.CancellationToken);

        var result = await host.Services.GetRequiredService<PubsubPushProcessor>()
            .ProcessAsync(topic, RequestFor(host, topic, new PushPing("cascade")), TestContext.Current.CancellationToken);

        result.StatusCode.ShouldBe(204);
        (await pullAsync(shipped)).ShouldNotBeEmpty();
    }

    [Fact]
    public async Task failing_message_goes_to_the_dead_letter_topic_and_is_acked()
    {
        Assert.SkipUnless(await TestingExtensions.IsEmulatorAvailable(), "Pub/Sub emulator is not available");
        var topic = $"push-dlq-{Guid.NewGuid():N}";

        using var host = await Microsoft.Extensions.Hosting.Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Discovery.DisableConventionalDiscovery().IncludeType(typeof(PushPingHandler));

                // EnableDeadLettering() before ListenToPubsubTopic(): the endpoint reads it in its constructor
                opts.UsePubsubTesting().AutoProvision().EnableDeadLettering()
                    .ConfigurePushDelivery(p =>
                    {
                        p.BaseUrl = "http://localhost:5999";
                        p.AllowUnauthenticated();
                    });
                opts.ListenToPubsubTopic(topic).UsePushDelivery();
            })
            .StartAsync(TestContext.Current.CancellationToken);

        var result = await host.Services.GetRequiredService<PubsubPushProcessor>()
            .ProcessAsync(topic, RequestFor(host, topic, new PushPing("throw")), TestContext.Current.CancellationToken);

        result.StatusCode.ShouldBe(204);
    }
}
```

The dead-letter test builds its host inline, because `EnableDeadLettering()` must run before `ListenToPubsubTopic` (`PubsubEndpoint.cs:91`), and `StartPushHostAsync` calls `ListenToPubsubTopic` first.

- [ ] **Step 2: Run them with the emulator**

Run: `dotnet test src/Transports/GCP/Wolverine.Pubsub.Tests/Wolverine.Pubsub.Tests.csproj -f net9.0 --filter "FullyQualifiedName~push_with_emulator"`
Expected: 2 PASS.

- [ ] **Step 3: Commit**

```bash
git add src/Transports/GCP/Wolverine.Pubsub.Tests/Push
git commit -m "Test Pub/Sub push cascades and dead-lettering against the emulator"
```

---

### Task 8: Documentation (PR 3, or folded into PR 2)

**Files:**
- Modify: `src/Transports/GCP/Wolverine.Pubsub.Tests/DocumentationSamples.cs` (append regions)
- Create: `docs/guide/messaging/transports/gcp-pubsub/push.md`
- Modify: `docs/.vitepress/config.mts:230-236`
- Modify: `docs/guide/serverless.md`
- Modify: `docs/guide/messaging/transports/gcp-pubsub/listening.md`

- [ ] **Step 1: Add the snippet regions to `DocumentationSamples.cs`**

```csharp
    public async Task configure_pubsub_push_delivery()
    {
        #region sample_pubsub_push_delivery

        var builder = WebApplication.CreateBuilder();

        builder.Host.UseWolverine(opts =>
        {
            // Push delivery only runs in Serverless mode: no background agents, every endpoint Inline
            opts.Durability.Mode = DurabilityMode.Serverless;

            opts.UsePubsub("my-project")
                .AutoProvision()

                // Without a dead letter destination, MoveToErrorQueue acknowledges and drops failed messages
                .EnableDeadLettering()

                .ConfigurePushDelivery(push =>
                {
                    // The Cloud Run service URL. AutoProvision writes {BaseUrl}/_wolverine/pubsub/{endpoint}
                    // into each push subscription
                    push.BaseUrl = builder.Configuration["PUBSUB_PUSH_BASE_URL"];
                    push.ServiceAccountEmail = "pubsub-push@my-project.iam.gserviceaccount.com";

                    // Cloud Run deployed with --no-allow-unauthenticated checks the token for us
                    push.TrustCloudRunIam();
                });

            opts.ListenToPubsubTopic("orders").UsePushDelivery();

            // Every cascaded message needs an external route in Serverless mode
            opts.PublishMessage<OrderShipped>().ToPubsubTopic("order-shipped");
        });

        var app = builder.Build();

        // From WolverineFx.Pubsub.AspNetCore
        app.MapWolverinePubsubPush();

        await app.RunAsync();

        #endregion
    }
```
Add `OrderShipped` as a `public record OrderShipped(string OrderId);` at the bottom of the file if no such type exists. Add `using Microsoft.AspNetCore.Builder;` and `using Wolverine.Pubsub.AspNetCore;`.

- [ ] **Step 2: Write `push.md`**

Write the page with these sections, in this order. Each one takes its content from the spec section named:
1. "When to use push delivery": §1 and the billing-setting names in the §9 source table. Mention instance-based billing as the alternative that keeps pull mode.
2. "Setup": `<!-- snippet: sample_pubsub_push_delivery -->` followed by `<!-- endSnippet -->`.
3. "How a push request is processed": the Mermaid diagram from §3.2, copied verbatim.
4. "Authentication and IAM": the §5.3 table, the audience bullets and the IAM list, including the April 8, 2021 condition.
5. "Provisioning": the §5.2 bullets, including `BaseUrl`, `RoutePrefix`, `ModifyPushConfig` and the startup read.
6. "Acks, retries and dead letters": the §4 status table, plus the push window, backoff and retry-policy bullets, best-effort `deliveryAttempt`, and ordering keys versus dead-letter policies.
7. "Timeouts": ack deadline equals the push timeout (max 600 s); Cloud Run's request timeout defaults to 300 s, so raise it.
8. "What works and what doesn't": the §7 table, with a `::: warning` block stating that handlers must be idempotent, because without an outbox a redelivery runs the handler again.
9. "Local development": run the emulator, use `AllowUnauthenticated()` and an `http` `BaseUrl`; IAM and OIDC can't be tested there.
10. "Future: durable push delivery": the §7 "Path to durable support" bullets.

- [ ] **Step 3: Add the sidebar entry (`config.mts`, after the "Listening" line at L232)**

```ts
                                        {text: 'Push Delivery (Cloud Run)', link:'/guide/messaging/transports/gcp-pubsub/push'},
```

- [ ] **Step 4: Update `docs/guide/serverless.md`**

Append a section `## Cloud Run and Pub/Sub push`. It should:
- link to `/guide/messaging/transports/gcp-pubsub/push`;
- state that every cascaded message needs an explicit external route in Serverless, otherwise `NoExternalRouteInServerlessException` is thrown;
- state that scheduled or delayed sends to a transport without native scheduling throw "Scheduled or delayed delivery is not supported in Serverless mode".

- [ ] **Step 5: Link from `listening.md`**

Add at the end: `For Cloud Run with request-based billing, see [Push Delivery](/guide/messaging/transports/gcp-pubsub/push).`

- [ ] **Step 6: Regenerate snippets and build the docs**

Run: `npm install` (once), `npm run mdsnippets`, then `npm run docs:build`
Expected:
- the snippet block in `push.md` is filled in;
- the build succeeds.

- [ ] **Step 7: Commit**

```bash
git add src/Transports/GCP/Wolverine.Pubsub.Tests/DocumentationSamples.cs docs/guide
git add docs/.vitepress/config.mts
git commit -m "Document Pub/Sub push delivery for Cloud Run"
```

---

### Task 9: Optional end-to-end push through the emulator

**Files:**
- Test: `src/Transports/GCP/Wolverine.Pubsub.Tests/Push/push_end_to_end.cs`

- [ ] **Step 1: Write the test**

```csharp
using JasperFx.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Wolverine.Pubsub.AspNetCore;
using Xunit;

namespace Wolverine.Pubsub.Tests.Push;

public class push_end_to_end
{
    [Fact]
    public async Task emulator_pushes_to_kestrel_and_the_handler_runs()
    {
        Assert.SkipUnless(await TestingExtensions.IsEmulatorAvailable(), "Pub/Sub emulator is not available");

        var topic = $"push-e2e-{Guid.NewGuid():N}";
        const int port = 5987;
        // The emulator runs in Docker; host.docker.internal reaches the test process on Windows and macOS.
        // Linux CI needs extra_hosts: "host.docker.internal:host-gateway" on the gcp-pubsub service.
        var baseUrl = Environment.GetEnvironmentVariable("PUBSUB_PUSH_TEST_BASE_URL") ?? $"http://host.docker.internal:{port}";

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls($"http://0.0.0.0:{port}");
        builder.Host.UseWolverine(opts =>
        {
            opts.Durability.Mode = DurabilityMode.Serverless;
            opts.Discovery.DisableConventionalDiscovery().IncludeType(typeof(Push.PushPingHandler));
            opts.UsePubsubTesting().AutoProvision().ConfigurePushDelivery(p =>
            {
                p.BaseUrl = baseUrl;
                p.AllowUnauthenticated();
            });
            opts.ListenToPubsubTopic(topic).UsePushDelivery();
            opts.PublishMessage<Push.PushPing>().ToPubsubTopic(topic);
        });

        await using var app = builder.Build();
        app.MapWolverinePubsubPush();
        await app.StartAsync(TestContext.Current.CancellationToken);

        var name = Guid.NewGuid().ToString();
        await app.Services.GetRequiredService<IWolverineRuntime>().MessageBus().PublishAsync(new Push.PushPing(name));

        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline && !Push.PushPingHandler.Handled.Any(x => x.Name == name))
        {
            await Task.Delay(250, TestContext.Current.CancellationToken);
        }

        Assert.SkipWhen(!Push.PushPingHandler.Handled.Any(x => x.Name == name),
            "The emulator could not reach the test host; set PUBSUB_PUSH_TEST_BASE_URL or add host-gateway");
    }
}
```

Resolve `MessageBus()` from the `Wolverine` namespace extension on `IHost`: use `app.MessageBus()` instead if it compiles, since `WebApplication` implements `IHost`. Add `using Microsoft.Extensions.DependencyInjection;` and `using Wolverine.Runtime;` as needed.

- [ ] **Step 2: Run it with the emulator**

Run: `dotnet test src/Transports/GCP/Wolverine.Pubsub.Tests/Wolverine.Pubsub.Tests.csproj -f net9.0 --filter "FullyQualifiedName~push_end_to_end"`
Expected:
- PASS on Windows or macOS with Docker Desktop;
- reported as skipped (with the reason) where the emulator can't reach the host.

This also settles spec §9 item 3. Log `deliveryAttempt` from a temporary breakpoint or log line, if needed, and note the answer in the PR description.

- [ ] **Step 3: Commit**

```bash
git add src/Transports/GCP/Wolverine.Pubsub.Tests/Push/push_end_to_end.cs
git commit -m "Add an optional emulator-to-Kestrel Pub/Sub push test"
```

---

### Task 10: Full verification

- [ ] **Step 1: Run the whole Pub/Sub test project with the emulator up**

Run: `docker compose up -d gcp-pubsub postgresql`, then `dotnet test src/Transports/GCP/Wolverine.Pubsub.Tests/Wolverine.Pubsub.Tests.csproj -f net9.0`
Expected: no failures. Pull-mode tests are unchanged.

- [ ] **Step 2: Build the full solution pinned to net9.0 (the CI gate)**

Run: `dotnet build wolverine.slnx -c Release -f net9.0`
Expected: Build succeeded.

- [ ] **Step 3: Build net10.0 too**

Run: `dotnet build src/Transports/GCP/Wolverine.Pubsub.AspNetCore/Wolverine.Pubsub.AspNetCore.csproj -c Release -f net10.0`
Expected: Build succeeded.

- [ ] **Step 4: Record what is still open in the PR description**

Spec §9 items 1 and 2 can only be checked on a real Cloud Run service:
1. whether the forwarded `Authorization` header reaches the container intact;
2. whether an audience with a path is accepted.

List both as manual follow-ups in the PR description. Don't claim them as verified.
