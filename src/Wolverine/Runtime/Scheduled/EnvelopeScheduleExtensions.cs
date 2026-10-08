using Wolverine.Runtime.Serialization;
using Wolverine.Transports;
using Wolverine.Transports.Sending;

namespace Wolverine.Runtime.Scheduled;

internal static class EnvelopeScheduleExtensions
{
    /// <summary>
    ///     The local durable queue that holds scheduled envelopes the destination transport cannot schedule
    ///     natively, or null when the local transport has been removed in <see cref="DurabilityMode.Serverless" />.
    /// </summary>
    public static ISendingAgent? TryFindLocalDurableQueue(this IWolverineRuntime runtime)
    {
        return runtime.Options.Transports.ForScheme(TransportConstants.Local) == null
            ? null
            : runtime.Endpoints.GetOrBuildSendingAgent(TransportConstants.DurableLocalUri);
    }

    /// <summary>
    ///     Wraps a scheduled envelope for delivery through the local durable queue, or throws when there is
    ///     no such queue.
    /// </summary>
    public static Envelope ForScheduledSendThroughLocalQueue(this Envelope envelope, ISendingAgent? localDurableQueue)
    {
        if (localDurableQueue == null)
        {
            throw new InvalidOperationException(
                $"Message {envelope.MessageType} is scheduled for {envelope.ScheduledTime:O} to {envelope.Destination}, " +
                "but that endpoint cannot schedule this envelope natively. Wolverine would otherwise hold it in the " +
                $"local durable queue ({TransportConstants.DurableLocalUri}) until it is due, but there is no local transport in " +
                $"{nameof(DurabilityMode)}.{nameof(DurabilityMode.Serverless)}. Send it to an endpoint with native scheduled " +
                "delivery for this message, send it without a scheduled time, or use a different durability mode.");
        }

        return envelope.ForScheduledSend(localDurableQueue);
    }

    public static Envelope ForScheduledSend(this Envelope envelope, ISendingAgent? sender)
    {
        return new Envelope(envelope, EnvelopeReaderWriter.Instance)
        {
            Message = envelope,
            MessageType = TransportConstants.ScheduledEnvelope,
            ScheduledTime = envelope.ScheduledTime,
            ContentType = TransportConstants.SerializedEnvelope,
            Destination = TransportConstants.DurableLocalUri,
            Status = EnvelopeStatus.Scheduled,
            OwnerId = TransportConstants.AnyNode,
            Sender = sender,
            Data = EnvelopeSerializer.Serialize(envelope),
            TopicName = envelope.TopicName
        };
    }
}