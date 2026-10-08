using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Wolverine.ErrorHandling;
using Wolverine.Pubsub.Push;
using Xunit;
using static Wolverine.Pubsub.Tests.Push.PushTestSupport;

namespace Wolverine.Pubsub.Tests.Push;

public class push_processor
{
    private static PubsubPushProcessor processorFor(Microsoft.Extensions.Hosting.IHost host)
        => host.Services.GetRequiredService<PubsubPushProcessor>();

    [Fact]
    public async Task handles_the_message_and_acks()
    {
        var topic = $"push-proc-{Guid.NewGuid():N}";
        using var host = await StartPushHostAsync(topic);
        var name = Guid.NewGuid().ToString();

        var result = await processorFor(host).ProcessAsync(topic, RequestFor(host, topic, new PushPing(name)),
            TestContext.Current.CancellationToken);

        result.StatusCode.ShouldBe(204);
        PushPingHandler.Handled.ShouldContain(x => x.Name == name);
    }

    [Fact]
    public async Task unknown_endpoint_is_404()
    {
        var topic = $"push-proc-{Guid.NewGuid():N}";
        using var host = await StartPushHostAsync(topic);

        var result = await processorFor(host).ProcessAsync("nope", RequestFor(host, topic, new PushPing("x")),
            TestContext.Current.CancellationToken);

        result.StatusCode.ShouldBe(404);
    }

    [Fact]
    public async Task pull_mode_endpoint_is_404()
    {
        var topic = $"push-proc-{Guid.NewGuid():N}";
        var pullTopic = $"pull-proc-{Guid.NewGuid():N}";
        using var host = await StartPushHostAsync(topic, opts => opts.ListenToPubsubTopic(pullTopic));

        var result = await processorFor(host).ProcessAsync(pullTopic, RequestFor(host, pullTopic, new PushPing("x")),
            TestContext.Current.CancellationToken);

        result.StatusCode.ShouldBe(404);
    }

    [Fact]
    public async Task subscription_from_an_unknown_project_is_400()
    {
        var topic = $"push-proc-{Guid.NewGuid():N}";
        using var host = await StartPushHostAsync(topic);

        var result = await processorFor(host).ProcessAsync(topic,
            RequestFor(host, topic, new PushPing("x"), subscriptionProject: "someone-else"),
            TestContext.Current.CancellationToken);

        result.StatusCode.ShouldBe(400);
    }

    [Fact]
    public async Task tenant_project_subscription_stamps_the_tenant()
    {
        var topic = $"push-proc-{Guid.NewGuid():N}";
        using var host = await StartPushHostAsync(topic,
            opts => opts.ConfigurePubsub().AddTenant("tenant-a", "tenant-a-project"));
        var name = Guid.NewGuid().ToString();

        var result = await processorFor(host).ProcessAsync(topic,
            RequestFor(host, topic, new PushPing(name), subscriptionProject: "tenant-a-project"),
            TestContext.Current.CancellationToken);

        result.StatusCode.ShouldBe(204);
        PushPingHandler.Handled.ShouldContain(x => x.Name == name && x.TenantId == "tenant-a");
    }

    [Theory]
    [InlineData(null, 1)]
    [InlineData(0, 1)]
    [InlineData(1, 1)]
    [InlineData(4, 4)]
    public async Task delivery_attempt_seeds_attempts(int? deliveryAttempt, int expected)
    {
        var topic = $"push-proc-{Guid.NewGuid():N}";
        using var host = await StartPushHostAsync(topic);
        var name = Guid.NewGuid().ToString();

        await processorFor(host).ProcessAsync(topic,
            RequestFor(host, topic, new PushPing(name), deliveryAttempt: deliveryAttempt),
            TestContext.Current.CancellationToken);

        PushPingHandler.Handled.ShouldContain(x => x.Name == name && x.Attempts == expected);
    }

    [Fact]
    public async Task requeue_policy_is_503()
    {
        var topic = $"push-proc-{Guid.NewGuid():N}";
        using var host = await StartPushHostAsync(topic,
            opts => opts.OnException<DivideByZeroException>().Requeue());

        var result = await processorFor(host).ProcessAsync(topic, RequestFor(host, topic, new PushPing("throw")),
            TestContext.Current.CancellationToken);

        result.StatusCode.ShouldBe(503);
    }

    [Fact]
    public async Task latched_processor_defers_with_503()
    {
        var topic = $"push-proc-{Guid.NewGuid():N}";
        using var host = await StartPushHostAsync(topic);
        var processor = processorFor(host);
        var name = Guid.NewGuid().ToString();

        processor.LatchAll();
        var result = await processor.ProcessAsync(topic, RequestFor(host, topic, new PushPing(name)),
            TestContext.Current.CancellationToken);

        result.StatusCode.ShouldBe(503);
        PushPingHandler.Handled.ShouldNotContain(x => x.Name == name);
    }

    [Fact]
    public async Task unmappable_message_is_acked()
    {
        var topic = $"push-proc-{Guid.NewGuid():N}";
        using var host = await StartPushHostAsync(topic);

        // "batched" data that is not a serialized envelope batch cannot be mapped
        var endpoint = host.GetRuntime().Options.Transports.GetOrCreate<PubsubTransport>().Topics[topic];
        var json = $$"""
            { "message": { "data": "AAEC", "attributes": { "batched": "1" }, "messageId": "1" },
              "subscription": "{{endpoint.Server.Subscription.Name}}" }
            """;
        PubsubPushRequest.TryParse(System.Text.Encoding.UTF8.GetBytes(json), out var request, out _);

        var result = await processorFor(host).ProcessAsync(topic, request!, TestContext.Current.CancellationToken);

        result.StatusCode.ShouldBe(204);
    }

    [Fact]
    public async Task caller_abort_is_503_even_when_the_handler_then_fails()
    {
        var topic = $"push-proc-{Guid.NewGuid():N}";
        using var host = await StartPushHostAsync(topic);
        using var abort = new CancellationTokenSource();
        PushPingHandler.Abort = abort;

        try
        {
            var result = await processorFor(host).ProcessAsync(topic,
                RequestFor(host, topic, new PushPing("cancel-then-throw")), abort.Token);

            result.StatusCode.ShouldBe(503);
        }
        finally
        {
            PushPingHandler.Abort = null;
        }
    }
}
