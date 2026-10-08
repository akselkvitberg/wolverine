namespace Wolverine.Runtime.Routing;

/// <summary>
///     The router for a message type with no routes at all. Non-generic since GH-4848; see
///     <see cref="MessageRouterBase" />.
/// </summary>
public class EmptyMessageRouter : MessageRouterBase
{
    private readonly string? _missingLocalRouteExplanation;

    public EmptyMessageRouter(WolverineRuntime runtime, Type messageType) : base(runtime, messageType)
    {
    }

    /// <summary>
    ///     For a message type that is handled in this application but has no route because the local queues
    ///     that would carry it to that handler do not exist. Publishing the message then throws with
    ///     <paramref name="missingLocalRouteExplanation" /> instead of finding no subscribers.
    /// </summary>
    internal EmptyMessageRouter(WolverineRuntime runtime, Type messageType, string? missingLocalRouteExplanation)
        : this(runtime, messageType)
    {
        _missingLocalRouteExplanation = missingLocalRouteExplanation;
    }

    public override IMessageRoute[] Routes => Array.Empty<MessageRoute>();

    public override Envelope[] RouteForSend(object message, DeliveryOptions? options)
    {
        throw new IndeterminateRoutesException(MessageType, _missingLocalRouteExplanation);
    }

    public override Envelope[] RouteForPublish(object message, DeliveryOptions? options)
    {
        if (_missingLocalRouteExplanation != null)
        {
            throw new IndeterminateRoutesException(MessageType, _missingLocalRouteExplanation);
        }

        return [];
    }

    public override IMessageRoute FindSingleRouteForSending()
    {
        throw new IndeterminateRoutesException(MessageType, _missingLocalRouteExplanation);
    }
}

/// <summary>
///     Retained for compatibility. The runtime builds the non-generic <see cref="EmptyMessageRouter" />
///     since GH-4848 and never constructs this one; see <see cref="MessageRouterBase" />.
/// </summary>
public class EmptyMessageRouter<T> : MessageRouterBase<T>
{
    public EmptyMessageRouter(WolverineRuntime runtime) : base(runtime)
    {
    }

    public override IMessageRoute[] Routes => Array.Empty<MessageRoute>();

    public override Envelope[] RouteForSend(T message, DeliveryOptions? options)
    {
        throw new IndeterminateRoutesException(typeof(T));
    }

    public override Envelope[] RouteForPublish(T message, DeliveryOptions? options)
    {
        return [];
    }

    public override IMessageRoute FindSingleRouteForSending()
    {
        throw new IndeterminateRoutesException(typeof(T));
    }
}
