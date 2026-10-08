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
