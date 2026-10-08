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
