using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.Hosting;
using Shouldly;
using Xunit;

namespace Wolverine.Pubsub.Tests.Push;

public class push_provisioning
{
    private static Task<IHost> startAsync(string topic, string baseUrl, bool push = true) =>
        Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Discovery.DisableConventionalDiscovery();
                var pubsub = opts.UsePubsubTesting().AutoProvision();

                if (push)
                {
                    pubsub.ConfigurePushDelivery(p =>
                    {
                        p.BaseUrl = baseUrl;
                        p.AllowUnauthenticated();
                    });
                    opts.ListenToPubsubTopic(topic).UsePushDelivery();
                }
                else
                {
                    opts.ListenToPubsubTopic(topic);
                }
            })
            .StartAsync(TestContext.Current.CancellationToken);

    private static async Task<Subscription> subscriptionAsync(string topic)
    {
        var client = await new SubscriberServiceApiClientBuilder
            { EmulatorDetection = Google.Api.Gax.EmulatorDetection.EmulatorOnly }.BuildAsync();
        return await client.GetSubscriptionAsync(new SubscriptionName("wolverine", topic));
    }

    [Fact]
    public async Task creates_a_push_subscription()
    {
        Assert.SkipUnless(await TestingExtensions.IsEmulatorAvailable(), "Pub/Sub emulator is not available");
        var topic = $"push-create-{Guid.NewGuid():N}";

        using var host = await startAsync(topic, "http://localhost:5999");

        (await subscriptionAsync(topic)).PushConfig.PushEndpoint
            .ShouldBe($"http://localhost:5999/_wolverine/pubsub/{topic}");
    }

    [Fact]
    public async Task updates_the_push_endpoint_when_the_url_changes()
    {
        Assert.SkipUnless(await TestingExtensions.IsEmulatorAvailable(), "Pub/Sub emulator is not available");
        var topic = $"push-update-{Guid.NewGuid():N}";

        using (await startAsync(topic, "http://localhost:5999")) { }
        using var second = await startAsync(topic, "http://localhost:6001");

        (await subscriptionAsync(topic)).PushConfig.PushEndpoint
            .ShouldBe($"http://localhost:6001/_wolverine/pubsub/{topic}");
    }

    [Fact]
    public async Task converts_an_existing_pull_subscription_to_push()
    {
        Assert.SkipUnless(await TestingExtensions.IsEmulatorAvailable(), "Pub/Sub emulator is not available");
        var topic = $"push-convert-{Guid.NewGuid():N}";

        // Not Serverless for the pull host, so its listener behaves as today
        using (await Host.CreateDefaultBuilder()
                   .UseWolverine(opts =>
                   {
                       opts.Discovery.DisableConventionalDiscovery();
                       opts.UsePubsubTesting().AutoProvision();
                       opts.ListenToPubsubTopic(topic);
                   })
                   .StartAsync(TestContext.Current.CancellationToken)) { }

        (await subscriptionAsync(topic)).PushConfig.PushEndpoint.ShouldBeEmpty();

        using var push = await startAsync(topic, "http://localhost:5999");

        (await subscriptionAsync(topic)).PushConfig.PushEndpoint.ShouldNotBeEmpty();
    }

    [Fact]
    public async Task purge_on_startup_uses_seek_and_does_not_fail()
    {
        Assert.SkipUnless(await TestingExtensions.IsEmulatorAvailable(), "Pub/Sub emulator is not available");
        var topic = $"push-purge-{Guid.NewGuid():N}";

        using var host = await Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Discovery.DisableConventionalDiscovery();
                opts.UsePubsubTesting().AutoProvision().AutoPurgeOnStartup()
                    .ConfigurePushDelivery(p =>
                    {
                        p.BaseUrl = "http://localhost:5999";
                        p.AllowUnauthenticated();
                    });
                opts.ListenToPubsubTopic(topic).UsePushDelivery();
            })
            .StartAsync(TestContext.Current.CancellationToken);
    }
}
