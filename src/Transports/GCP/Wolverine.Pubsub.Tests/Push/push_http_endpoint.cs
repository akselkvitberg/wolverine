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
        var topic = $"push-http~{Guid.NewGuid():N}";
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

    private class FakeValidator(string email) : IPubsubPushTokenValidator
    {
        public Task<GoogleJsonWebSignature.Payload> ValidateAsync(string token, string audience) =>
            Task.FromResult(new GoogleJsonWebSignature.Payload { Email = email, EmailVerified = true });
    }
}
