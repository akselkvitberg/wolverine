using Google.Api.Gax.Grpc;
using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Shouldly;
using Wolverine.Pubsub.Internal;
using Wolverine.Pubsub.Push;
using Xunit;

namespace Wolverine.Pubsub.Tests.Push;

public class push_delivery_settlement
{
    private readonly PubsubTransport _transport = new() { ProjectId = "wolverine" };
    private readonly PublisherServiceApiClient _publisher = Substitute.For<PublisherServiceApiClient>();

    private PubsubPushDelivery deliveryFor(PubsubEndpoint endpoint)
    {
        var clients = new PubsubClientSet
        {
            ProjectId = "wolverine",
            EmulatorDetection = Google.Api.Gax.EmulatorDetection.None,
            PublisherApiClient = _publisher,
            SubscriberApiClient = Substitute.For<SubscriberServiceApiClient>()
        };
        _transport.PublisherApiClient = _publisher;
        return new PubsubPushDelivery(endpoint, clients, null, NullLogger.Instance);
    }

    private PubsubEndpoint endpoint(string name = "orders") => new(name, _transport);
    private static Envelope envelope() => new() { Id = Guid.NewGuid(), Data = [1], ContentType = "application/json" };

    [Fact]
    public async Task completed_is_204()
    {
        var e = envelope();
        var delivery = deliveryFor(endpoint());
        await delivery.CompleteAsync(e);
        delivery.ResultFor([e]).StatusCode.ShouldBe(204);
    }

    [Fact]
    public async Task deferred_is_503_and_stays_failed_after_a_later_complete()
    {
        var e = envelope();
        var delivery = deliveryFor(endpoint());
        await delivery.DeferAsync(e);
        await delivery.CompleteAsync(e);
        delivery.ResultFor([e]).StatusCode.ShouldBe(503);
    }

    [Fact]
    public async Task requeue_and_native_schedule_are_503()
    {
        var a = envelope();
        var b = envelope();
        var delivery = deliveryFor(endpoint());
        (await delivery.TryRequeueAsync(a)).ShouldBeTrue();
        await delivery.MoveToScheduledUntilAsync(b, DateTimeOffset.UtcNow.AddMinutes(5));
        delivery.ResultFor([a]).StatusCode.ShouldBe(503);
        delivery.ResultFor([b]).StatusCode.ShouldBe(503);
    }

    [Fact]
    public void unsettled_is_503()
    {
        deliveryFor(endpoint()).ResultFor([envelope()]).StatusCode.ShouldBe(503);
    }

    [Fact]
    public async Task processing_failed_then_completed_is_500()
    {
        var e = envelope();
        var delivery = deliveryFor(endpoint());
        delivery.ProcessingFailed(e, new DivideByZeroException());
        await delivery.CompleteAsync(e);
        delivery.ResultFor([e]).StatusCode.ShouldBe(500);
    }

    [Fact]
    public async Task outgoing_send_failure_is_500()
    {
        var e = envelope();
        var delivery = deliveryFor(endpoint());
        delivery.OutgoingSendFailed(e, envelope(), new DivideByZeroException());
        await delivery.CompleteAsync(e);
        delivery.ResultFor([e]).StatusCode.ShouldBe(500);
    }

    [Fact]
    public async Task dead_letter_publish_then_complete_is_204()
    {
        var ep = endpoint();
        ep.DeadLetterName = "wlvrn.dead-letter";
        var e = envelope();
        var delivery = deliveryFor(ep);

        await delivery.MoveToErrorsAsync(e, new DivideByZeroException());
        await delivery.CompleteAsync(e);

        delivery.ResultFor([e]).StatusCode.ShouldBe(204);
        await _publisher.Received(1).PublishAsync(Arg.Any<PublishRequest>(), Arg.Any<CallSettings>());
    }

    [Fact]
    public async Task failed_dead_letter_publish_is_500_and_does_not_throw()
    {
        var ep = endpoint();
        ep.DeadLetterName = "wlvrn.dead-letter";
        _publisher.PublishAsync(Arg.Any<PublishRequest>(), Arg.Any<CallSettings>())
            .Returns<Task<PublishResponse>>(_ => throw new InvalidOperationException("down"));
        var e = envelope();
        var delivery = deliveryFor(ep);

        await delivery.MoveToErrorsAsync(e, new DivideByZeroException());
        await delivery.CompleteAsync(e);

        delivery.ResultFor([e]).StatusCode.ShouldBe(500);
    }

    [Fact]
    public async Task dead_letter_with_only_a_subscription_policy_is_503()
    {
        var ep = endpoint();
        ep.Server.Subscription.Options.DeadLetterPolicy = new DeadLetterPolicy { MaxDeliveryAttempts = 5 };
        var e = envelope();
        var delivery = deliveryFor(ep);

        await delivery.MoveToErrorsAsync(e, new DivideByZeroException());
        await delivery.CompleteAsync(e);

        delivery.ResultFor([e]).StatusCode.ShouldBe(503);
    }

    [Fact]
    public async Task dead_letter_with_no_destination_acks_and_drops()
    {
        var e = envelope();
        var delivery = deliveryFor(endpoint());
        await delivery.MoveToErrorsAsync(e, new DivideByZeroException());
        await delivery.CompleteAsync(e);
        delivery.ResultFor([e]).StatusCode.ShouldBe(204);
    }

    [Fact]
    public async Task batch_uses_the_status_of_the_first_failure()
    {
        var ok = envelope();
        var requeued = envelope();
        var failed = envelope();
        var delivery = deliveryFor(endpoint());

        await delivery.CompleteAsync(ok);
        await delivery.DeferAsync(requeued);
        delivery.ProcessingFailed(failed, new DivideByZeroException());

        delivery.ResultFor([ok, requeued, failed]).StatusCode.ShouldBe(503);
    }
}
