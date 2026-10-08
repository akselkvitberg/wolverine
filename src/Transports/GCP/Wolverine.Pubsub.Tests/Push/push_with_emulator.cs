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
}
