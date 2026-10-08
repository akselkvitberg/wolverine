using Shouldly;
using Alba;
using Google.Apis.Auth;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Wolverine.Pubsub.AspNetCore;
using Xunit;
using static Wolverine.Pubsub.Tests.Push.PushTestSupport;

namespace Wolverine.Pubsub.Tests.Push;

public class push_http_endpoint
{
    private static async Task<IAlbaHost> hostAsync(string topic, Action<PubsubPushSettings> auth,
        IPubsubPushTokenValidator? validator = null)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Host.UseWolverine(opts =>
        {
            opts.Durability.Mode = DurabilityMode.Serverless;
            opts.Discovery.DisableConventionalDiscovery().IncludeType(typeof(PushPingHandler));
            opts.UsePubsubTesting().ConfigurePushDelivery(auth);
            opts.ListenToPubsubTopic(topic).UsePushDelivery();
        });
        if (validator != null) builder.Services.AddSingleton(validator);

        return await AlbaHost.For(builder, app => app.MapWolverinePubsubPush());
    }

    private static string bodyFor(IAlbaHost host, string topic, string name)
    {
        var request = RequestFor(host, topic, new PushPing(name));
        return System.Text.Json.JsonSerializer.Serialize(new
        {
            message = new
            {
                data = request.Message.Data.ToBase64(),
                attributes = request.Message.Attributes.ToDictionary(x => x.Key, x => x.Value),
                messageId = request.Message.MessageId
            },
            subscription = request.Subscription
        });
    }

    [Fact]
    public async Task valid_push_is_204()
    {
        var topic = $"push-http-{Guid.NewGuid():N}";
        await using var host = await hostAsync(topic, p => p.AllowUnauthenticated());

        await host.Scenario(x =>
        {
            x.Post.Text(bodyFor(host, topic, "ok")).ToUrl($"/_wolverine/pubsub/{topic}");
            x.StatusCodeShouldBe(204);
        });
    }

    [Fact]
    public async Task escaped_endpoint_names_still_route()
    {
        // '+' and '%' are legal in Pub/Sub names and are escaped in the URL, so the route value must be decoded
        var topic = $"push-http+{Guid.NewGuid():N}%x";
        await using var host = await hostAsync(topic, p => p.AllowUnauthenticated());

        await host.Scenario(x =>
        {
            x.Post.Text(bodyFor(host, topic, "ok")).ToUrl($"/_wolverine/pubsub/{Uri.EscapeDataString(topic)}");
            x.StatusCodeShouldBe(204);
        });
    }

    [Fact]
    public async Task malformed_body_is_400()
    {
        var topic = $"push-http-{Guid.NewGuid():N}";
        await using var host = await hostAsync(topic, p => p.AllowUnauthenticated());

        await host.Scenario(x =>
        {
            x.Post.Text("not json").ToUrl($"/_wolverine/pubsub/{topic}");
            x.StatusCodeShouldBe(400);
        });
    }

    [Fact]
    public async Task unknown_endpoint_is_404()
    {
        var topic = $"push-http-{Guid.NewGuid():N}";
        await using var host = await hostAsync(topic, p => p.AllowUnauthenticated());

        await host.Scenario(x =>
        {
            x.Post.Text("{}").ToUrl("/_wolverine/pubsub/missing");
            x.StatusCodeShouldBe(404);
        });
    }

    [Fact]
    public async Task verify_oidc_without_a_token_is_401()
    {
        var topic = $"push-http-{Guid.NewGuid():N}";
        await using var host = await hostAsync(topic, p =>
        {
            p.BaseUrl = "https://svc.a.run.app";
            p.ServiceAccountEmail = "push@wolverine.iam.gserviceaccount.com";
            p.VerifyOidcToken();
        }, new FakeValidator("push@wolverine.iam.gserviceaccount.com"));

        await host.Scenario(x =>
        {
            x.Post.Text(bodyFor(host, topic, "x")).ToUrl($"/_wolverine/pubsub/{topic}");
            x.StatusCodeShouldBe(401);
        });
    }

    [Fact]
    public async Task verify_oidc_with_the_wrong_principal_is_403()
    {
        var topic = $"push-http-{Guid.NewGuid():N}";
        await using var host = await hostAsync(topic, p =>
        {
            p.BaseUrl = "https://svc.a.run.app";
            p.ServiceAccountEmail = "push@wolverine.iam.gserviceaccount.com";
            p.VerifyOidcToken();
        }, new FakeValidator("intruder@evil.iam.gserviceaccount.com"));

        await host.Scenario(x =>
        {
            x.Post.Text(bodyFor(host, topic, "x")).ToUrl($"/_wolverine/pubsub/{topic}");
            x.WithRequestHeader("Authorization", "Bearer anything");
            x.StatusCodeShouldBe(403);
        });
    }

    [Fact]
    public async Task verify_oidc_with_the_right_principal_is_204()
    {
        var topic = $"push-http-{Guid.NewGuid():N}";
        await using var host = await hostAsync(topic, p =>
        {
            p.BaseUrl = "https://svc.a.run.app";
            p.ServiceAccountEmail = "push@wolverine.iam.gserviceaccount.com";
            p.VerifyOidcToken();
        }, new FakeValidator("push@wolverine.iam.gserviceaccount.com"));

        await host.Scenario(x =>
        {
            x.Post.Text(bodyFor(host, topic, "ok")).ToUrl($"/_wolverine/pubsub/{topic}");
            x.WithRequestHeader("Authorization", "Bearer anything");
            x.StatusCodeShouldBe(204);
        });
    }

    [Fact]
    public async Task trust_cloud_run_iam_rejects_a_forwarded_token_for_another_principal()
    {
        var topic = $"push-http-{Guid.NewGuid():N}";
        await using var host = await hostAsync(topic, p =>
        {
            p.BaseUrl = "https://svc.a.run.app";
            p.ServiceAccountEmail = "push@wolverine.iam.gserviceaccount.com";
            p.TrustCloudRunIam();
        });

        // header.payload.signature where payload = {"email":"intruder@evil.iam.gserviceaccount.com"}
        var payload = Convert.ToBase64String("""{"email":"intruder@evil.iam.gserviceaccount.com"}"""u8.ToArray())
            .TrimEnd('=').Replace('+', '-').Replace('/', '_');

        await host.Scenario(x =>
        {
            x.Post.Text(bodyFor(host, topic, "x")).ToUrl($"/_wolverine/pubsub/{topic}");
            x.WithRequestHeader("Authorization", $"Bearer e30.{payload}.sig");
            x.StatusCodeShouldBe(403);
        });
    }

    [Fact]
    public async Task trust_cloud_run_iam_without_a_forwarded_token_is_204()
    {
        var topic = $"push-http-{Guid.NewGuid():N}";
        await using var host = await hostAsync(topic, p =>
        {
            p.BaseUrl = "https://svc.a.run.app";
            p.ServiceAccountEmail = "push@wolverine.iam.gserviceaccount.com";
            p.TrustCloudRunIam();
        });

        await host.Scenario(x =>
        {
            x.Post.Text(bodyFor(host, topic, "ok")).ToUrl($"/_wolverine/pubsub/{topic}");
            x.StatusCodeShouldBe(204);
        });
    }

    [Fact]
    public async Task verify_oidc_with_an_invalid_token_is_401()
    {
        var topic = $"push-http-{Guid.NewGuid():N}";
        await using var host = await hostAsync(topic, p =>
        {
            p.BaseUrl = "https://svc.a.run.app";
            p.ServiceAccountEmail = "push@wolverine.iam.gserviceaccount.com";
            p.VerifyOidcToken();
        }, new RejectingValidator());

        await host.Scenario(x =>
        {
            x.Post.Text(bodyFor(host, topic, "x")).ToUrl($"/_wolverine/pubsub/{topic}");
            x.WithRequestHeader("Authorization", "Bearer anything");
            x.StatusCodeShouldBe(401);
        });
    }

    [Fact]
    public async Task verify_oidc_checks_the_base_url_as_audience()
    {
        var topic = $"push-http-{Guid.NewGuid():N}";
        var validator = new FakeValidator("push@wolverine.iam.gserviceaccount.com");
        await using var host = await hostAsync(topic, p =>
        {
            p.BaseUrl = "https://svc.a.run.app";
            p.ServiceAccountEmail = "push@wolverine.iam.gserviceaccount.com";
            p.VerifyOidcToken();
        }, validator);

        await host.Scenario(x =>
        {
            x.Post.Text(bodyFor(host, topic, "ok")).ToUrl($"/_wolverine/pubsub/{topic}");
            x.WithRequestHeader("Authorization", "Bearer anything");
            x.StatusCodeShouldBe(204);
        });

        validator.Audiences.ShouldBe(["https://svc.a.run.app"]);
    }

    [Theory]
    [InlineData("W10")] // []
    [InlineData("eyJlbWFpbCI6MX0")] // {"email":1}
    public async Task trust_cloud_run_iam_ignores_a_forwarded_token_it_cannot_read(string payload)
    {
        var topic = $"push-http-{Guid.NewGuid():N}";
        await using var host = await hostAsync(topic, p =>
        {
            p.BaseUrl = "https://svc.a.run.app";
            p.ServiceAccountEmail = "push@wolverine.iam.gserviceaccount.com";
            p.TrustCloudRunIam();
        });

        await host.Scenario(x =>
        {
            x.Post.Text(bodyFor(host, topic, "ok")).ToUrl($"/_wolverine/pubsub/{topic}");
            x.WithRequestHeader("Authorization", $"Bearer e30.{payload}.sig");
            x.StatusCodeShouldBe(204);
        });
    }

    [Fact]
    public async Task verify_oidc_when_the_validator_fails_for_another_reason_is_503()
    {
        var topic = $"push-http-{Guid.NewGuid():N}";
        await using var host = await hostAsync(topic, p =>
        {
            p.BaseUrl = "https://svc.a.run.app";
            p.ServiceAccountEmail = "push@wolverine.iam.gserviceaccount.com";
            p.VerifyOidcToken();
        }, new UnreachableValidator());

        await host.Scenario(x =>
        {
            x.Post.Text(bodyFor(host, topic, "x")).ToUrl($"/_wolverine/pubsub/{topic}");
            x.WithRequestHeader("Authorization", "Bearer anything");
            x.StatusCodeShouldBe(503);
        });
    }

    [Fact]
    public async Task not_accepting_requests_is_503_before_the_endpoint_lookup()
    {
        var topic = $"push-http-{Guid.NewGuid():N}";
        await using var host = await hostAsync(topic, p => p.AllowUnauthenticated());

        // Same IsAcceptingRequests check the route makes while the host is starting
        host.Services.GetRequiredService<Wolverine.Pubsub.Push.PubsubPushProcessor>().LatchAll();

        await host.Scenario(x =>
        {
            x.Post.Text("{}").ToUrl("/_wolverine/pubsub/missing");
            x.StatusCodeShouldBe(503);
        });
    }

    [Fact]
    public async Task mapping_the_route_without_configure_push_delivery_explains_what_is_missing()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Host.UseWolverine(opts =>
        {
            opts.Discovery.DisableConventionalDiscovery();
            opts.UsePubsubTesting();
        });
        await using var app = builder.Build();

        var ex = Should.Throw<InvalidOperationException>(() => app.MapWolverinePubsubPush());
        ex.Message.ShouldBe(
            "MapWolverinePubsubPush() needs opts.UsePubsub(...).ConfigurePushDelivery(...) to be configured");
    }

    private class UnreachableValidator : IPubsubPushTokenValidator
    {
        public Task<GoogleJsonWebSignature.Payload> ValidateAsync(string token, string audience) =>
            throw new HttpRequestException("could not download Google's certificates");
    }

    private class FakeValidator(string email) : IPubsubPushTokenValidator
    {
        public List<string> Audiences { get; } = [];

        public Task<GoogleJsonWebSignature.Payload> ValidateAsync(string token, string audience)
        {
            Audiences.Add(audience);
            return Task.FromResult(new GoogleJsonWebSignature.Payload { Email = email, EmailVerified = true });
        }
    }

    private class RejectingValidator : IPubsubPushTokenValidator
    {
        public Task<GoogleJsonWebSignature.Payload> ValidateAsync(string token, string audience) =>
            throw new InvalidJwtException("bad signature");
    }
}
