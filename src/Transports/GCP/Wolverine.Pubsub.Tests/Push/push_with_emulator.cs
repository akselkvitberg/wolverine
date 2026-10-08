using Shouldly;
using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Wolverine.Pubsub.Push;
using Xunit;
using static Wolverine.Pubsub.Tests.Push.PushTestSupport;

namespace Wolverine.Pubsub.Tests.Push;

public static class PushCascadeHandler
{
    public static PushShipped Handle(PushPing ping) => new(ping.Name);
}

public class push_with_emulator
{
    private static async Task<PubsubMessage[]> pullAsync(string subscriptionId, int maxMessages = 10)
    {
        var client = await new SubscriberServiceApiClientBuilder
            { EmulatorDetection = Google.Api.Gax.EmulatorDetection.EmulatorOnly }.BuildAsync();
        var response = await client.PullAsync(new SubscriptionName("wolverine", subscriptionId), maxMessages);
        return response.ReceivedMessages.Select(x => x.Message).ToArray();
    }

    [Fact]
    public async Task cascade_is_published_before_the_response()
    {
        Assert.SkipUnless(await TestingExtensions.IsEmulatorAvailable(), "Pub/Sub emulator is not available");
        var topic = $"push-cascade-{Guid.NewGuid():N}";
        var shipped = $"push-shipped-{Guid.NewGuid():N}";

        using var host = await Microsoft.Extensions.Hosting.Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Discovery.DisableConventionalDiscovery().IncludeType(typeof(PushCascadeHandler));
                opts.UsePubsubTesting().AutoProvision()
                    .ConfigurePushDelivery(p =>
                    {
                        p.BaseUrl = "http://localhost:5999";
                        p.AllowUnauthenticated();
                    });
                opts.ListenToPubsubTopic(topic).UsePushDelivery();
                // a pull-mode listener on the cascade topic only so AutoProvision creates its subscription;
                // in Serverless it never starts, so the test can pull from it
                opts.ListenToPubsubTopic(shipped);
                opts.PublishMessage<PushShipped>().ToPubsubTopic(shipped);
            })
            .StartAsync(TestContext.Current.CancellationToken);

        var result = await host.Services.GetRequiredService<PubsubPushProcessor>()
            .ProcessAsync(topic, RequestFor(host, topic, new PushPing("cascade")), TestContext.Current.CancellationToken);

        result.StatusCode.ShouldBe(204);
        (await pullAsync(shipped)).ShouldNotBeEmpty();
    }

    [Fact]
    public async Task failing_message_goes_to_the_dead_letter_topic_and_is_acked()
    {
        Assert.SkipUnless(await TestingExtensions.IsEmulatorAvailable(), "Pub/Sub emulator is not available");
        var topic = $"push-dlq-{Guid.NewGuid():N}";

        using var host = await Microsoft.Extensions.Hosting.Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Discovery.DisableConventionalDiscovery().IncludeType(typeof(PushPingHandler));

                // EnableDeadLettering() before ListenToPubsubTopic(): the endpoint reads it in its constructor
                opts.UsePubsubTesting().AutoProvision().EnableDeadLettering()
                    .ConfigurePushDelivery(p =>
                    {
                        p.BaseUrl = "http://localhost:5999";
                        p.AllowUnauthenticated();
                    });
                opts.ListenToPubsubTopic(topic).UsePushDelivery();
            })
            .StartAsync(TestContext.Current.CancellationToken);

        var request = RequestFor(host, topic, new PushPing("throw"));
        var result = await host.Services.GetRequiredService<PubsubPushProcessor>()
            .ProcessAsync(topic, request, TestContext.Current.CancellationToken);

        result.StatusCode.ShouldBe(204);

        // The dead letter subscription is shared by every test run, so look for this envelope's id among its backlog
        var envelopeId = request.Message.Attributes[EnvelopeConstants.IdKey];
        var deadLetters = await pullAsync(PubsubTransport.DeadLetterName, 1000);
        deadLetters.ShouldContain(x => x.Attributes.ContainsKey(EnvelopeConstants.IdKey) &&
                                       x.Attributes[EnvelopeConstants.IdKey] == envelopeId);
    }

    [Fact]
    public async Task requests_before_startup_are_503_and_do_not_cache_unconnected_clients()
    {
        Assert.SkipUnless(await TestingExtensions.IsEmulatorAvailable(), "Pub/Sub emulator is not available");
        var topic = $"push-early-{Guid.NewGuid():N}";

        using var host = Microsoft.Extensions.Hosting.Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Discovery.DisableConventionalDiscovery().IncludeType(typeof(PushPingHandler));
                opts.UsePubsubTesting().AutoProvision().EnableDeadLettering()
                    .ConfigurePushDelivery(p =>
                    {
                        p.BaseUrl = "http://localhost:5999";
                        p.AllowUnauthenticated();
                    });
                opts.ListenToPubsubTopic(topic).UsePushDelivery();
            })
            .Build();

        // Before StartAsync the transport is not connected, as when Kestrel takes a request before Wolverine is up.
        // UsePushDelivery() is applied when the endpoint is compiled during startup, so set the mode here to stand in
        // for the window where the endpoint is compiled but the transport is not connected yet
        host.GetRuntime().Options.Transports.GetOrCreate<PubsubTransport>().Topics[topic].DeliveryMode =
            PubsubDeliveryMode.Push;

        var processor = host.Services.GetRequiredService<PubsubPushProcessor>();
        processor.IsAcceptingRequests.ShouldBeFalse();
        processor.TryFindEndpoint(topic, out _).ShouldBeTrue();
        (await processor.ProcessAsync(topic, RequestFor(host, topic, new PushPing("early")),
            TestContext.Current.CancellationToken)).StatusCode.ShouldBe(503);

        await host.StartAsync(TestContext.Current.CancellationToken);
        try
        {
            processor.IsAcceptingRequests.ShouldBeTrue();

            // Dead lettering publishes through the endpoint's cached clients, so they must be the connected ones
            var result = await processor.ProcessAsync(topic, RequestFor(host, topic, new PushPing("throw")),
                TestContext.Current.CancellationToken);
            result.StatusCode.ShouldBe(204);
        }
        finally
        {
            await host.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task failed_cascade_send_is_500()
    {
        Assert.SkipUnless(await TestingExtensions.IsEmulatorAvailable(), "Pub/Sub emulator is not available");
        var topic = $"push-cascade-fail-{Guid.NewGuid():N}";
        var shipped = $"push-shipped-fail-{Guid.NewGuid():N}";

        using var host = await Microsoft.Extensions.Hosting.Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Discovery.DisableConventionalDiscovery().IncludeType(typeof(PushCascadeHandler));
                opts.UsePubsubTesting().AutoProvision()
                    .ConfigurePushDelivery(p =>
                    {
                        p.BaseUrl = "http://localhost:5999";
                        p.AllowUnauthenticated();
                    });
                opts.ListenToPubsubTopic(topic).UsePushDelivery();
                opts.PublishMessage<PushShipped>().ToPubsubTopic(shipped);
            })
            .StartAsync(TestContext.Current.CancellationToken);

        var publisher = await new PublisherServiceApiClientBuilder
            { EmulatorDetection = Google.Api.Gax.EmulatorDetection.EmulatorOnly }.BuildAsync(TestContext.Current.CancellationToken);
        await publisher.DeleteTopicAsync(new TopicName("wolverine", shipped));

        var result = await host.Services.GetRequiredService<PubsubPushProcessor>()
            .ProcessAsync(topic, RequestFor(host, topic, new PushPing("cascade")), TestContext.Current.CancellationToken);

        result.StatusCode.ShouldBe(500);
    }
}
