using JasperFx.Core;
using Microsoft.Extensions.Logging;
using Wolverine.ErrorHandling;
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

        if (push.RoutePrefix.Trim('/').IsEmpty())
            problems.Add("RoutePrefix must not be empty; the push route is {RoutePrefix}/{endpointName}");

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

        // Only when Wolverine can see the subscription's configuration: it read the subscription at startup, or it
        // provisioned the subscription itself under AutoProvision()
        var subscriptionConfigIsKnown = endpoint.ObservedHasDeadLetterPolicy.HasValue ||
                                        (endpoint.Transport.AutoProvision && !endpoint.IsExistingSubscription);

        if (subscriptionConfigIsKnown && !hasDeadLetterPolicy && runtime is WolverineRuntime wolverineRuntime &&
            (anyRedeliveryPolicies(wolverineRuntime.Handlers.Failures) ||
             wolverineRuntime.Handlers.Chains.Any(x => anyRedeliveryPolicies(x.Failures))))
            logger.LogWarning("{Uri}: Requeue or scheduled retry policies are configured, but this push subscription has no DeadLetterPolicy, so Pub/Sub sends no deliveryAttempt and attempt counts restart at 1 on every redelivery", endpoint.Uri);
    }

    private static bool anyRedeliveryPolicies(FailureRuleCollection failures) =>
        failures.AnyRequeuePolicies() || failures.AnyScheduledRetryPolicies();
}
