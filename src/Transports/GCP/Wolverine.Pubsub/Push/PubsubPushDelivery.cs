using JasperFx.Core;
using Microsoft.Extensions.Logging;
using Wolverine.Pubsub.Internal;
using Wolverine.Runtime;
using Wolverine.Transports;

namespace Wolverine.Pubsub.Push;

/// <summary>
/// Plays the listener role for one push request and records how the pipeline settled each envelope. The HTTP
/// response is computed from these outcomes once the pipeline returns (spec §4). Failure is sticky, settle calls
/// never throw, and an envelope nobody settled counts as failed.
/// </summary>
internal sealed class PubsubPushDelivery : IListener, ISupportDeadLetterQueue, ISupportNativeScheduling,
    IObserveChannelFailures
{
    private readonly PubsubEndpoint _endpoint;
    private readonly PubsubClientSet _clients;
    private readonly ILogger _logger;
    private readonly Dictionary<Envelope, PubsubPushResult> _outcomes = new(ReferenceEqualityComparer.Instance);
    private readonly object _lock = new();

    public PubsubPushDelivery(PubsubEndpoint endpoint, PubsubClientSet clients, IHandlerPipeline? pipeline,
        ILogger logger)
    {
        _endpoint = endpoint;
        _clients = clients;
        Pipeline = pipeline;
        _logger = logger;
    }

    public Uri Address => _endpoint.Uri;
    public IHandlerPipeline? Pipeline { get; }

    // Always true so MoveToErrorQueue comes here; MoveToErrorsAsync decides between the three destinations
    public bool NativeDeadLetterQueueEnabled => true;

    public ValueTask CompleteAsync(Envelope envelope)
    {
        record(envelope, PubsubPushResult.Ack);
        return ValueTask.CompletedTask;
    }

    public ValueTask DeferAsync(Envelope envelope)
    {
        record(envelope, PubsubPushResult.RetryLater("requeued"));
        return ValueTask.CompletedTask;
    }

    public Task<bool> TryRequeueAsync(Envelope envelope)
    {
        record(envelope, PubsubPushResult.RetryLater("requeued"));
        return Task.FromResult(true);
    }

    public Task MoveToScheduledUntilAsync(Envelope envelope, DateTimeOffset time)
    {
        // Pub/Sub push cannot delay one message; the subscription's retry policy decides the backoff
        record(envelope, PubsubPushResult.RetryLater("scheduled retry, delay handled by the subscription retry policy"));
        return Task.CompletedTask;
    }

    public async Task MoveToErrorsAsync(Envelope envelope, Exception exception)
    {
        if (_endpoint.DeadLetterName.IsNotEmpty())
        {
            try
            {
                DeadLetterQueueConstants.StampFailureMetadata(envelope, exception);
                var deadLetterTopic = _endpoint.Transport.Topics[_endpoint.DeadLetterName!];
                await deadLetterTopic.SendMessageAsync(envelope, _logger, _clients);
                record(envelope, PubsubPushResult.Ack);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "{Uri}: Could not publish envelope {EnvelopeId} to dead letter topic {Topic}",
                    _endpoint.Uri, envelope.Id, _endpoint.DeadLetterName);
                record(envelope, PubsubPushResult.Failed("dead letter publish failed"));
            }

            return;
        }

        if (_endpoint.Server.Subscription.Options.DeadLetterPolicy != null || _endpoint.ObservedHasDeadLetterPolicy == true)
        {
            record(envelope,
                PubsubPushResult.RetryLater("nacked so the subscription's dead letter policy can forward it"));
            return;
        }

        _logger.LogError(exception,
            "{Uri}: Envelope {EnvelopeId} ({MessageType}) failed and this push endpoint has no dead letter destination, so it is acknowledged and dropped. Enable dead lettering or give the subscription a DeadLetterPolicy",
            _endpoint.Uri, envelope.Id, envelope.MessageType);
        record(envelope, PubsubPushResult.Ack);
    }

    public void OutgoingSendFailed(Envelope incoming, Envelope outgoing, Exception exception)
        => record(incoming, PubsubPushResult.Failed("an outgoing message could not be sent"));

    public void ProcessingFailed(Envelope envelope, Exception exception)
        => record(envelope, PubsubPushResult.Failed("the handler pipeline failed"));

    public PubsubPushResult ResultFor(IReadOnlyList<Envelope> envelopes)
    {
        lock (_lock)
        {
            foreach (var envelope in envelopes)
            {
                if (!_outcomes.TryGetValue(envelope, out var outcome))
                {
                    return PubsubPushResult.RetryLater("the envelope was never settled");
                }

                if (!outcome.IsAck) return outcome;
            }

            return PubsubPushResult.Ack;
        }
    }

    private void record(Envelope envelope, PubsubPushResult outcome)
    {
        lock (_lock)
        {
            // Sticky failure: once failed, a later ack (e.g. the pipeline's recovery CompleteAsync) cannot undo it.
            // The first failure wins so the response reports the original cause.
            if (_outcomes.TryGetValue(envelope, out var existing) && !existing.IsAck) return;
            _outcomes[envelope] = outcome;
        }
    }

    public ValueTask StopAsync() => ValueTask.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
