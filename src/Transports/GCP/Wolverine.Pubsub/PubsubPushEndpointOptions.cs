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
