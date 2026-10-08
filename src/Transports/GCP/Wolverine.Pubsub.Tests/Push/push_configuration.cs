using Shouldly;
using Wolverine.Pubsub;
using Xunit;

namespace Wolverine.Pubsub.Tests.Push;

public class push_configuration
{
    [Fact]
    public void push_url_is_base_url_plus_route_prefix_plus_escaped_endpoint_name()
    {
        var settings = new PubsubPushSettings { BaseUrl = "https://orders-abc.a.run.app/" };

        settings.PushUrlFor("orders").ShouldBe("https://orders-abc.a.run.app/_wolverine/pubsub/orders");
        settings.PushUrlFor("a+b~c%d").ShouldBe("https://orders-abc.a.run.app/_wolverine/pubsub/a%2Bb~c%25d");
    }

    [Fact]
    public void push_url_is_null_without_a_base_url()
    {
        new PubsubPushSettings().PushUrlFor("orders").ShouldBeNull();
    }

    [Fact]
    public void configure_push_delivery_turns_on_sync_retry_and_records_the_auth_mode()
    {
        var opts = new WolverineOptions();
        opts.UsePubsub("wolverine").ConfigurePushDelivery(p => p.TrustCloudRunIam());

        opts.Durability.UseSyncRetryBlock.ShouldBeTrue();
        opts.Transports.GetOrCreate<PubsubTransport>().Push.Authentication
            .ShouldBe(PubsubPushAuthentication.TrustCloudRunIam);
    }

    [Fact]
    public void audience_and_email_fall_back_from_endpoint_to_transport_to_base_url()
    {
        var transport = new PubsubTransport { ProjectId = "wolverine" };
        transport.Push.BaseUrl = "https://svc.a.run.app";
        transport.Push.ServiceAccountEmail = "push@wolverine.iam.gserviceaccount.com";
        var endpoint = new PubsubEndpoint("orders", transport);

        endpoint.EffectiveAudience.ShouldBe("https://svc.a.run.app");
        endpoint.EffectiveServiceAccountEmail.ShouldBe("push@wolverine.iam.gserviceaccount.com");

        endpoint.PushOptions.Audience = "custom";
        endpoint.PushOptions.ServiceAccountEmail = "other@x.iam.gserviceaccount.com";
        endpoint.EffectiveAudience.ShouldBe("custom");
        endpoint.EffectiveServiceAccountEmail.ShouldBe("other@x.iam.gserviceaccount.com");
    }
}
