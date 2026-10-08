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
