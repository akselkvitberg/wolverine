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
