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
