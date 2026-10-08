using JasperFx.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shouldly;
using Wolverine.ErrorHandling;
using Wolverine.Transports;
using Xunit;

namespace Wolverine.Pubsub.Tests.Push;

public class push_startup_validation
{
    private static async Task<Exception> startupFailure(Action<WolverineOptions> configure)
    {
        var ex = await Should.ThrowAsync<Exception>(async () =>
        {
            using var host = await Host.CreateDefaultBuilder()
                .UseWolverine(opts =>
                {
                    opts.Discovery.DisableConventionalDiscovery();
                    configure(opts);
                })
                .StartAsync(TestContext.Current.CancellationToken);
        });

        // Validation runs before the broker retry loop, so the configuration error is not retried and wrapped
        ex.ShouldNotBeOfType<BrokerInitializationException>();
        return ex;
    }

    private static void serverlessPush(WolverineOptions opts, Action<PubsubPushSettings> push,
        Action<PubsubTopicListenerConfiguration>? listener = null)
    {
        opts.Durability.Mode = DurabilityMode.Serverless;
        opts.UsePubsubTesting().ConfigurePushDelivery(push);
        var config = opts.ListenToPubsubTopic("push-validation").UsePushDelivery();
        listener?.Invoke(config);
    }

    [Fact]
    public async Task push_outside_serverless_fails()
    {
        var ex = await startupFailure(opts =>
        {
            opts.UsePubsubTesting().ConfigurePushDelivery(p => p.AllowUnauthenticated());
            opts.ListenToPubsubTopic("push-validation").UsePushDelivery();
        });
        ex.ToString().ShouldContain("DurabilityMode.Serverless");
    }

    [Fact]
    public async Task no_authentication_mode_fails()
    {
        var ex = await startupFailure(opts =>
        {
            opts.Durability.Mode = DurabilityMode.Serverless;
            opts.UsePubsubTesting();
            opts.ListenToPubsubTopic("push-validation").UsePushDelivery();
        });
        ex.ToString().ShouldContain("authentication mode");
    }

    [Fact]
    public async Task auto_provision_without_base_url_fails()
    {
        var ex = await startupFailure(opts =>
        {
            serverlessPush(opts, p => p.AllowUnauthenticated());
            opts.UsePubsubTesting().AutoProvision();
        });
        ex.ToString().ShouldContain("BaseUrl");
    }

    [Fact]
    public async Task http_base_url_outside_allow_unauthenticated_fails()
    {
        var ex = await startupFailure(opts => serverlessPush(opts, p =>
        {
            p.BaseUrl = "http://insecure.example";
            p.TrustCloudRunIam();
        }));
        ex.ToString().ShouldContain("https");
    }

    [Fact]
    public async Task empty_route_prefix_fails()
    {
        var ex = await startupFailure(opts => serverlessPush(opts, p =>
        {
            p.RoutePrefix = "/";
            p.AllowUnauthenticated();
        }));
        ex.ToString().ShouldContain("RoutePrefix");
    }

    [Fact]
    public async Task subscription_per_node_fails()
    {
        var ex = await startupFailure(opts =>
            serverlessPush(opts, p => p.AllowUnauthenticated(), l => l.SubscriptionPerNode()));
        ex.ToString().ShouldContain("SubscriptionPerNode");
    }

    [Fact]
    public async Task exactly_once_fails()
    {
        var ex = await startupFailure(opts => serverlessPush(opts, p => p.AllowUnauthenticated(),
            l => l.ConfigurePubsubSubscription(s => s.EnableExactlyOnceDelivery = true)));
        ex.ToString().ShouldContain("exactly-once");
    }

    [Fact]
    public async Task circuit_breaker_fails()
    {
        var ex = await startupFailure(opts =>
            serverlessPush(opts, p => p.AllowUnauthenticated(), l => l.CircuitBreaker()));
        ex.ToString().ShouldContain("CircuitBreaker");
    }

    [Fact]
    public async Task verify_oidc_without_audience_or_base_url_fails()
    {
        var ex = await startupFailure(opts => serverlessPush(opts, p =>
        {
            p.ServiceAccountEmail = "push@wolverine.iam.gserviceaccount.com";
            p.VerifyOidcToken();
        }));
        ex.ToString().ShouldContain("Audience");
    }

    [Fact]
    public async Task verify_oidc_without_service_account_fails()
    {
        var ex = await startupFailure(opts => serverlessPush(opts, p =>
        {
            p.BaseUrl = "https://svc.a.run.app";
            p.VerifyOidcToken();
        }));
        ex.ToString().ShouldContain("ServiceAccountEmail");
    }

    private const string RedeliveryWarning = "Requeue or scheduled retry policies are configured";

    private static async Task<RecordingLoggerProvider> startAndRecordAsync(bool autoProvision,
        Action<WolverineOptions> policies)
    {
        var recorder = new RecordingLoggerProvider();
        var topic = $"push-warn-{Guid.NewGuid():N}";

        using var host = await Host.CreateDefaultBuilder()
            .ConfigureLogging(x => x.AddProvider(recorder))
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Discovery.DisableConventionalDiscovery();
                var pubsub = opts.UsePubsubTesting().ConfigurePushDelivery(p =>
                {
                    p.BaseUrl = "http://localhost:5999";
                    p.AllowUnauthenticated();
                });
                if (autoProvision) pubsub.AutoProvision();
                opts.ListenToPubsubTopic(topic).UsePushDelivery();
                policies(opts);
            })
            .StartAsync(TestContext.Current.CancellationToken);

        return recorder;
    }

    [Fact]
    public async Task scheduled_retry_policy_without_a_dead_letter_policy_warns()
    {
        Assert.SkipUnless(await TestingExtensions.IsEmulatorAvailable(), "Pub/Sub emulator is not available");

        var recorder = await startAndRecordAsync(true,
            opts => opts.OnException<DivideByZeroException>().ScheduleRetry(1.Seconds()));

        recorder.Warnings.ShouldContain(x => x.Contains(RedeliveryWarning));
    }

    [Fact]
    public async Task requeue_policy_without_a_dead_letter_policy_warns()
    {
        Assert.SkipUnless(await TestingExtensions.IsEmulatorAvailable(), "Pub/Sub emulator is not available");

        var recorder = await startAndRecordAsync(true,
            opts => opts.OnException<DivideByZeroException>().Requeue());

        recorder.Warnings.ShouldContain(x => x.Contains(RedeliveryWarning));
    }

    [Fact]
    public async Task requeue_policy_does_not_warn_when_the_subscription_config_is_unknown()
    {
        Assert.SkipUnless(await TestingExtensions.IsEmulatorAvailable(), "Pub/Sub emulator is not available");

        // No AutoProvision() and the subscription does not exist, so the startup read fails and Wolverine
        // cannot tell whether the subscription has a DeadLetterPolicy
        var recorder = await startAndRecordAsync(false,
            opts => opts.OnException<DivideByZeroException>().Requeue());

        recorder.Warnings.ShouldContain(x => x.Contains("does not exist"));
        recorder.Warnings.ShouldNotContain(x => x.Contains(RedeliveryWarning));
    }
}
