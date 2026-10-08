using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
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

    [Fact]
    public async Task failed_purge_does_not_block_startup()
    {
        Assert.SkipUnless(await TestingExtensions.IsEmulatorAvailable(), "Pub/Sub emulator is not available");
        var topic = $"push-purge-missing-{Guid.NewGuid():N}";
        var recorder = new RecordingLoggerProvider();

        // No AutoProvision(), so the subscription does not exist and Seek fails
        using var host = await Host.CreateDefaultBuilder()
            .ConfigureLogging(x => x.AddProvider(recorder))
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Discovery.DisableConventionalDiscovery();
                opts.UsePubsubTesting().AutoPurgeOnStartup()
                    .ConfigurePushDelivery(p => p.AllowUnauthenticated());
                opts.ListenToPubsubTopic(topic).UsePushDelivery();
            })
            .StartAsync(TestContext.Current.CancellationToken);

        recorder.Warnings.ShouldContain(x => x.Contains("Could not purge Pub/Sub push subscription"));
    }

    [Fact]
    public async Task existing_subscription_without_auto_provision_is_read_and_left_alone()
    {
        Assert.SkipUnless(await TestingExtensions.IsEmulatorAvailable(), "Pub/Sub emulator is not available");
        var topic = $"push-existing-{Guid.NewGuid():N}";
        var deadLetterTopic = $"push-existing-dlq-{Guid.NewGuid():N}";
        var subscription = $"push-existing-sub-{Guid.NewGuid():N}";

        // UsePubsubTesting() sets this too, but the admin clients are built before the host
        Environment.SetEnvironmentVariable("PUBSUB_EMULATOR_HOST", TestingExtensions.EmulatorHost);
        var publisher = await new PublisherServiceApiClientBuilder
            { EmulatorDetection = Google.Api.Gax.EmulatorDetection.EmulatorOnly }.BuildAsync(TestContext.Current.CancellationToken);
        var subscriber = await new SubscriberServiceApiClientBuilder
            { EmulatorDetection = Google.Api.Gax.EmulatorDetection.EmulatorOnly }.BuildAsync(TestContext.Current.CancellationToken);

        await publisher.CreateTopicAsync(new TopicName("wolverine", topic));
        await publisher.CreateTopicAsync(new TopicName("wolverine", deadLetterTopic));
        await subscriber.CreateSubscriptionAsync(new Subscription
        {
            SubscriptionName = new SubscriptionName("wolverine", subscription),
            TopicAsTopicName = new TopicName("wolverine", topic),
            AckDeadlineSeconds = 42,
            PushConfig = new PushConfig { PushEndpoint = "http://localhost:5998/managed-elsewhere" },
            DeadLetterPolicy = new DeadLetterPolicy
            {
                DeadLetterTopic = new TopicName("wolverine", deadLetterTopic).ToString(),
                MaxDeliveryAttempts = 5
            }
        });

        using var host = await Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Discovery.DisableConventionalDiscovery();
                opts.UsePubsubTesting().ConfigurePushDelivery(p =>
                {
                    p.BaseUrl = "http://localhost:5999";
                    p.AllowUnauthenticated();
                });
                opts.ListenToPubsubSubscription(subscription).UsePushDelivery();
            })
            .StartAsync(TestContext.Current.CancellationToken);

        var endpoint = host.GetRuntime().Options.Transports.GetOrCreate<PubsubTransport>().Topics
            .Single(x => x.DeliveryMode == PubsubDeliveryMode.Push);
        endpoint.ObservedAckDeadlineSeconds.ShouldBe(42);
        endpoint.ObservedHasDeadLetterPolicy.ShouldBe(true);

        // Without AutoProvision() Wolverine does not touch an existing subscription's push config
        (await subscriber.GetSubscriptionAsync(new SubscriptionName("wolverine", subscription)))
            .PushConfig.PushEndpoint.ShouldBe("http://localhost:5998/managed-elsewhere");
    }

    [Fact]
    public async Task missing_existing_subscription_is_logged_as_an_error_and_the_host_starts()
    {
        Assert.SkipUnless(await TestingExtensions.IsEmulatorAvailable(), "Pub/Sub emulator is not available");
        var subscription = $"push-missing-sub-{Guid.NewGuid():N}";
        var recorder = new RecordingLoggerProvider();

        using var host = await Host.CreateDefaultBuilder()
            .ConfigureLogging(x => x.AddProvider(recorder))
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Discovery.DisableConventionalDiscovery();
                opts.UsePubsubTesting().ConfigurePushDelivery(p => p.AllowUnauthenticated());
                opts.ListenToPubsubSubscription(subscription).UsePushDelivery();
            })
            .StartAsync(TestContext.Current.CancellationToken);

        // RecordingLoggerProvider keeps warnings and above without the level; this message is only logged as an error
        recorder.Warnings.ShouldContain(x => x.Contains("does not exist, so no push requests will arrive"));
    }
}
