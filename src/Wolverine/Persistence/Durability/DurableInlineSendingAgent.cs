using Microsoft.Extensions.Logging;
using Wolverine.Configuration;
using Wolverine.Logging;
using Wolverine.Runtime;
using Wolverine.Transports;
using Wolverine.Transports.Sending;

namespace Wolverine.Persistence.Durability;

/// <summary>
///     The sending agent behind <see cref="Endpoint.DurableInlineOutbox" />. It is durable -- outgoing envelopes are
///     persisted by the active transaction exactly as for <see cref="DurableSendingAgent" /> -- but it has no
///     background work of its own: the send runs on the caller's thread when the outgoing messages are flushed after
///     the commit, and the outbox row is deleted before that call returns.
///
///     <para>
///     A failed send never throws. By the time this runs, the application's transaction has already committed, so an
///     exception would invite the caller to retry work that is done. Instead the row is released to
///     <see cref="TransportConstants.AnyNode" /> and left for <c>IWolverineRuntime.RecoverOutboxAsync()</c> -- or the
///     regular durability agent, when one runs -- to send later.
///     </para>
/// </summary>
internal class DurableInlineSendingAgent : ISendingAgent
{
    private readonly ILogger _logger;
    private readonly IMessageTracker _messageLogger;
    private readonly IMessageOutbox _outbox;
    private readonly DurabilitySettings _settings;

    public DurableInlineSendingAgent(ISender sender, Endpoint endpoint, IMessageOutbox outbox,
        DurabilitySettings settings, ILogger logger, IMessageTracker messageLogger)
    {
        Sender = sender;
        Endpoint = endpoint;
        _outbox = outbox;
        _settings = settings;
        _logger = logger;
        _messageLogger = messageLogger;
    }

    public ISender Sender { get; }
    public Endpoint Endpoint { get; }
    public Uri Destination => Sender.Destination;
    public Uri? ReplyUri { get; set; }

    // Never latched: a broken transport costs one failed send per message, and every one of them is still in the
    // outbox for recovery. There is no in-memory queue to protect.
    public bool Latched => false;
    public bool IsDurable => true;
    public bool SupportsNativeScheduledSend => Sender.SupportsNativeScheduledSend;
    public DateTimeOffset LastMessageSentAt { get; private set; } = DateTimeOffset.UtcNow;

    /// <summary>
    ///     The envelope is already in the outbox: either the application's transaction persisted it, or recovery
    ///     loaded it from there.
    /// </summary>
    public async ValueTask EnqueueOutgoingAsync(Envelope envelope)
    {
        setDefaults(envelope);
        await sendAndSettleAsync(envelope);
    }

    /// <summary>
    ///     Sending outside a transaction: persist first, so the message survives a failed send, then send inline. A
    ///     failure to persist is thrown to the caller, the same as <see cref="DurableSendingAgent" /> (GH-4662).
    /// </summary>
    public async ValueTask StoreAndForwardAsync(Envelope envelope)
    {
        setDefaults(envelope);
        await _outbox.StoreOutgoingAsync(envelope, _settings.AssignedNodeNumber);
        await sendAndSettleAsync(envelope);
    }

    private async Task sendAndSettleAsync(Envelope envelope)
    {
        using var activity = Endpoint.TelemetryEnabled ? WolverineTracing.StartSending(envelope) : null;

        try
        {
            await Sender.SendAsync(envelope);
        }
        catch (Exception e)
        {
            _logger.LogError(e,
                "Inline send of envelope {EnvelopeId} ({MessageType}) to {Destination} failed. It stays in the outbox for recovery",
                envelope.Id, envelope.MessageType, Destination);

            await releaseForRecoveryAsync(envelope);
            return;
        }

        _messageLogger.Sent(envelope);
        LastMessageSentAt = DateTimeOffset.UtcNow;

        try
        {
            await _outbox.DeleteOutgoingAsync(envelope);
        }
        catch (Exception e)
        {
            // The message went out; only the bookkeeping failed. The row stays owned by this node until it goes
            // stale (DurabilitySettings.OutboxStaleTime) and is then sent a second time -- at-least-once, not lost.
            _logger.LogError(e,
                "Envelope {EnvelopeId} was sent to {Destination} but could not be deleted from the outbox, so it may be sent again",
                envelope.Id, Destination);
        }
    }

    private async Task releaseForRecoveryAsync(Envelope envelope)
    {
        try
        {
            await _outbox.DiscardAndReassignOutgoingAsync([], [envelope], TransportConstants.AnyNode);
        }
        catch (Exception e)
        {
            // Still owned by this node, so it is recovered once it goes stale rather than on the next recovery pass.
            _logger.LogError(e, "Could not release envelope {EnvelopeId} for outbox recovery", envelope.Id);
        }
    }

    private void setDefaults(Envelope envelope)
    {
        envelope.Status = EnvelopeStatus.Outgoing;
        envelope.OwnerId = _settings.AssignedNodeNumber;
        envelope.ReplyUri ??= ReplyUri;
    }
}
