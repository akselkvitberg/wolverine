using Microsoft.Extensions.Hosting;
using Shouldly;
using Wolverine.ErrorHandling;
using Xunit;

namespace Wolverine.Pubsub.Tests.Push;

public class push_startup_validation
{
    private static async Task<Exception> startupFailure(Action<WolverineOptions> configure)
    {
        return await Should.ThrowAsync<Exception>(async () =>
        {
            using var host = await Host.CreateDefaultBuilder()
                .UseWolverine(opts =>
                {
                    opts.Discovery.DisableConventionalDiscovery();
                    // Validation runs inside the broker initialization retry loop; do not retry a configuration error
                    opts.BrokerInitializationTimeout = TimeSpan.Zero;
                    configure(opts);
                })
                .StartAsync(TestContext.Current.CancellationToken);
        });
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
}
