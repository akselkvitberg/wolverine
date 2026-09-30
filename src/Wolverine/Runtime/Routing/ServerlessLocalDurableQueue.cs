using Wolverine.Configuration;
using Wolverine.Transports;
using Wolverine.Transports.Sending;

namespace Wolverine.Runtime.Routing;

/// <summary>
///     Stands in for the durable local queue in <see cref="DurabilityMode.Serverless" />, which has no local transport.
///     The message router only hands it to a scheduled envelope whose transport cannot schedule natively, and that is
///     the only case that fails.
/// </summary>
internal class ServerlessLocalDurableQueue : ISendingAgent
{
    internal const string Explanation =
        "This message is scheduled for later, but its transport cannot schedule it natively, and Serverless mode has " +
        "no durable local queue to hold it until then. Use a transport with native scheduled delivery for scheduled " +
        "messages, or a durability mode other than Serverless.";

    public Uri Destination => TransportConstants.DurableLocalUri;
    public Uri? ReplyUri { get; set; }
    public bool Latched => false;
    public bool IsDurable => false;
    public bool SupportsNativeScheduledSend => false;
    public Endpoint Endpoint => throw new NotSupportedException(Explanation);
    public DateTimeOffset LastMessageSentAt => DateTimeOffset.MinValue;

    public ValueTask EnqueueOutgoingAsync(Envelope envelope) => throw new NotSupportedException(Explanation);

    public ValueTask StoreAndForwardAsync(Envelope envelope) => throw new NotSupportedException(Explanation);
}
