using Microsoft.Extensions.DependencyInjection;
using Wolverine.Runtime;
using JasperFx.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Wolverine.Pubsub.AspNetCore;
using Xunit;

namespace Wolverine.Pubsub.Tests.Push;

public class push_end_to_end
{
    [Fact]
    public async Task emulator_pushes_to_kestrel_and_the_handler_runs()
    {
        Assert.SkipUnless(await TestingExtensions.IsEmulatorAvailable(), "Pub/Sub emulator is not available");

        var topic = $"push-e2e-{Guid.NewGuid():N}";
        const int port = 5987;
        // The emulator runs in Docker; host.docker.internal reaches the test process on Windows and macOS.
        // Linux CI needs extra_hosts: "host.docker.internal:host-gateway" on the gcp-pubsub service.
        var configuredBaseUrl = Environment.GetEnvironmentVariable("PUBSUB_PUSH_TEST_BASE_URL");
        var baseUrl = configuredBaseUrl ?? $"http://host.docker.internal:{port}";

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls($"http://0.0.0.0:{port}");
        builder.Host.UseWolverine(opts =>
        {
            opts.Durability.Mode = DurabilityMode.Serverless;
            opts.Discovery.DisableConventionalDiscovery().IncludeType(typeof(Push.PushPingHandler));
            opts.UsePubsubTesting().AutoProvision().ConfigurePushDelivery(p =>
            {
                p.BaseUrl = baseUrl;
                p.AllowUnauthenticated();
            });
            opts.ListenToPubsubTopic(topic).UsePushDelivery();
            opts.PublishMessage<Push.PushPing>().ToPubsubTopic(topic);
        });

        await using var app = builder.Build();
        app.MapWolverinePubsubPush();
        await app.StartAsync(TestContext.Current.CancellationToken);

        var name = Guid.NewGuid().ToString();
        await app.MessageBus().PublishAsync(new Push.PushPing(name));

        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline && !Push.PushPingHandler.Handled.Any(x => x.Name == name))
        {
            await Task.Delay(250, TestContext.Current.CancellationToken);
        }

        var handled = Push.PushPingHandler.Handled.Any(x => x.Name == name);

        // An explicitly configured URL is expected to work; only the default host.docker.internal guess may skip
        if (configuredBaseUrl != null)
        {
            Assert.True(handled, $"The emulator did not push to PUBSUB_PUSH_TEST_BASE_URL ({configuredBaseUrl}) within 30 seconds");
        }

        Assert.SkipWhen(!handled,
            "The emulator could not reach the test host; set PUBSUB_PUSH_TEST_BASE_URL or add host-gateway");
    }
}
