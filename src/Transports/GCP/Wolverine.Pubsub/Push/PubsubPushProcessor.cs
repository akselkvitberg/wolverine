using System.Diagnostics;
using Google.Cloud.PubSub.V1;
using ImTools;
using JasperFx.Core;
using Microsoft.Extensions.Logging;
using Wolverine.Pubsub.Internal;
using Wolverine.Runtime;
using Wolverine.Runtime.Serialization;
using Wolverine.Runtime.WorkerQueues;

namespace Wolverine.Pubsub.Push;

/// <summary>
/// Runs one Pub/Sub push request through its endpoint's handler pipeline inside the request (spec §3.2). Has no HTTP
/// types; WolverineFx.Pubsub.AspNetCore turns the result into a status code.
/// </summary>
public class PubsubPushProcessor
{
    private readonly WolverineRuntime _runtime;
    private readonly ILogger _logger;
    private ImHashMap<string, PushEndpointState> _states = ImHashMap<string, PushEndpointState>.Empty;
    private readonly object _lock = new();
    private volatile bool _latched;

    public PubsubPushProcessor(IWolverineRuntime runtime)
    {
        _runtime = (WolverineRuntime)runtime;
        _logger = runtime.LoggerFactory.CreateLogger<PubsubPushProcessor>();
    }

    private PubsubTransport transport => _runtime.Options.Transports.GetOrCreate<PubsubTransport>();

    /// <summary>
    /// False while the host is starting or stopping. Push requests are then answered 503 so Pub/Sub redelivers them
    /// </summary>
    public bool IsAcceptingRequests =>
        !_latched && _runtime.FullyStarted.IsCompleted && !_runtime.Cancellation.IsCancellationRequested;

    /// <summary>
    /// Find the push-mode endpoint for a route value. Answered from the per-endpoint cache after the first processed
    /// request. Never builds the cached state itself, because that needs the transport to be connected
    /// </summary>
    public bool TryFindEndpoint(string endpointName, out PubsubEndpoint? endpoint)
    {
        endpoint = _states.TryFind(endpointName, out var state) ? state.Endpoint : scanForPushEndpoint(endpointName);
        return endpoint != null;
    }

    private PubsubEndpoint? scanForPushEndpoint(string endpointName) =>
        transport.Topics.FirstOrDefault(x =>
            x.DeliveryMode == PubsubDeliveryMode.Push && x.EndpointName == endpointName);

    /// <summary>
    /// Stop handing new push requests to handlers; they are answered 503 so Pub/Sub redelivers elsewhere.
    /// Called on ApplicationStopping
    /// </summary>
    public void LatchAll()
    {
        // Under the lock so a state stateFor() is adding right now is either latched there or enumerated here
        lock (_lock)
        {
            _latched = true;
            foreach (var entry in _states.Enumerate()) entry.Value.Receiver.Latch();
        }
    }

    /// <summary>
    /// Run one push request through the endpoint's handler pipeline and report how it settled
    /// </summary>
    public async Task<PubsubPushResult> ProcessAsync(string endpointName, PubsubPushRequest request,
        CancellationToken cancellation)
    {
        if (!IsAcceptingRequests)
        {
            return PubsubPushResult.RetryLater("the host is not running");
        }

        // The only place state is built: after the readiness check, so the transport's clients are connected
        var state = stateFor(endpointName);
        if (state == null)
        {
            return PubsubPushResult.NotFound($"no Pub/Sub push endpoint named '{endpointName}'");
        }

        if (!state.TryMatchSubscription(request.Subscription, out var clients, out var tenantId))
        {
            return PubsubPushResult.BadRequest(
                $"subscription '{request.Subscription}' does not belong to push endpoint '{endpointName}'");
        }

        var delivery = new PubsubPushDelivery(state.Endpoint, clients!, state.Receiver.Pipeline, _logger);

        Envelope[] envelopes;
        try
        {
            envelopes = readEnvelopes(state.Endpoint, request.Message);
        }
        catch (Exception e)
        {
            return await handleUnmappableAsync(state.Endpoint, clients!, request.Message, e);
        }

        if (envelopes.Length == 0) return PubsubPushResult.Ack;

        foreach (var envelope in envelopes)
        {
            // Executor increments Attempts before each run, so the first execution sees deliveryAttempt
            if (request.DeliveryAttempt is > 1) envelope.Attempts = request.DeliveryAttempt.Value - 1;

            // Same order as pull: the tenant project stamp first, then the endpoint's own rules, so an explicit
            // endpoint TenantId wins
            if (tenantId != null) envelope.TenantId = tenantId;
            foreach (var rule in state.IncomingRules) rule.Modify(envelope);
        }

        var stopwatch = Stopwatch.StartNew();
        try
        {
            await state.Receiver.ReceivedAsync(delivery, envelopes, cancellation);
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
        {
            return PubsubPushResult.RetryLater("the push request was aborted");
        }

        stopwatch.Stop();

        // No timer: Cloud Run gives no CPU between requests, so the overrun is reported after the fact
        var deadline = state.Endpoint.ObservedAckDeadlineSeconds ??
                       state.Endpoint.Server.Subscription.Options.AckDeadlineSeconds;
        if (stopwatch.Elapsed > TimeSpan.FromSeconds(deadline))
        {
            _logger.LogWarning(
                "{Uri}: Push request for message {MessageId} took {Elapsed}, longer than the subscription's ack deadline of {Deadline}s, so Pub/Sub has probably redelivered it already",
                state.Endpoint.Uri, request.Message.MessageId, stopwatch.Elapsed, deadline);
        }

        // After a caller abort the response is never trusted: the recorded settlement may come from failure handling
        // that ran after the abort (spec §6.4)
        if (cancellation.IsCancellationRequested)
        {
            return PubsubPushResult.RetryLater("the push request was aborted");
        }

        return delivery.ResultFor(envelopes);
    }

    private Envelope[] readEnvelopes(PubsubEndpoint endpoint, PubsubMessage message)
    {
        if (message.Attributes.ContainsKey("batched"))
        {
            return EnvelopeSerializer.ReadMany(message.Data.ToByteArray()).ToArray();
        }

        var envelope = new Envelope();
        endpoint.EnvelopeMapper!.MapIncomingToEnvelope(envelope, message);
        return [envelope];
    }

    private async Task<PubsubPushResult> handleUnmappableAsync(PubsubEndpoint endpoint, PubsubClientSet clients,
        PubsubMessage message, Exception exception)
    {
        _logger.LogError(exception,
            "{Uri}: Could not map Pub/Sub push message {MessageId}; it is acknowledged so it is not redelivered forever",
            endpoint.Uri, message.MessageId);

        if (endpoint.DeadLetterName.IsEmpty()) return PubsubPushResult.Ack;

        try
        {
            var deadLetter = endpoint.Transport.Topics[endpoint.DeadLetterName!];
            var publisher = clients.PublisherApiClient ??
                            throw new InvalidOperationException("the Pub/Sub publisher client is not connected");
            await publisher.PublishAsync(new PublishRequest
            {
                TopicAsTopicName = deadLetter.TopicNameFor(clients.ProjectId),
                Messages = { message }
            });
            return PubsubPushResult.Ack;
        }
        catch (Exception e)
        {
            _logger.LogError(e, "{Uri}: Could not publish unmappable message {MessageId} to the dead letter topic",
                endpoint.Uri, message.MessageId);
            return PubsubPushResult.Failed("dead letter publish of an unmappable message failed");
        }
    }

    private PushEndpointState? stateFor(string endpointName)
    {
        if (_states.TryFind(endpointName, out var state)) return state;

        lock (_lock)
        {
            if (_states.TryFind(endpointName, out state)) return state;
            var endpoint = scanForPushEndpoint(endpointName);
            if (endpoint == null) return null;

            state = new PushEndpointState(endpoint, transport, _runtime);
            if (_latched) state.Receiver.Latch();
            _states = _states.AddOrUpdate(endpointName, state);
            return state;
        }
    }

    private sealed class PushEndpointState
    {
        private readonly string _defaultSubscription;
        private readonly PubsubClientSet _defaultClients;
        private readonly (string Subscription, string TenantId, PubsubClientSet Clients)[] _tenants;

        public PushEndpointState(PubsubEndpoint endpoint, PubsubTransport transport, WolverineRuntime runtime)
        {
            Endpoint = endpoint;
            endpoint.EnvelopeMapper ??= endpoint.BuildMapper(runtime);

            var pipeline = new HandlerPipeline(runtime, runtime, endpoint) { TelemetryEnabled = endpoint.TelemetryEnabled };
            Receiver = new InlineReceiver(endpoint, runtime, pipeline);
            IncomingRules = endpoint.RulesForIncoming().ToArray();

            _defaultSubscription = endpoint.SubscriptionNameFor(transport.ProjectId).ToString();
            _defaultClients = transport.DefaultClients;
            _tenants = transport.Tenants
                .Select(t => (endpoint.SubscriptionNameFor(t.ProjectId).ToString(), t.TenantId, transport.GetTenantClients(t)))
                .ToArray();
        }

        public PubsubEndpoint Endpoint { get; }
        public InlineReceiver Receiver { get; }
        public IEnvelopeRule[] IncomingRules { get; }

        public bool TryMatchSubscription(string subscription, out PubsubClientSet? clients, out string? tenantId)
        {
            if (subscription == _defaultSubscription)
            {
                clients = _defaultClients;
                tenantId = null;
                return true;
            }

            foreach (var tenant in _tenants)
            {
                if (subscription != tenant.Subscription) continue;
                clients = tenant.Clients;
                tenantId = tenant.TenantId;
                return true;
            }

            clients = null;
            tenantId = null;
            return false;
        }
    }
}
