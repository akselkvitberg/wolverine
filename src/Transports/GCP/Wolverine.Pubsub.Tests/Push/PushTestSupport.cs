using System.Collections.Concurrent;
using System.Text.Json;
using Google.Cloud.PubSub.V1;
using JasperFx.Core.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shouldly;
using Wolverine.Pubsub.Internal;
using Wolverine.Pubsub.Push;
using Wolverine.Runtime;
using Wolverine.Util;
using Xunit;

namespace Wolverine.Pubsub.Tests.Push;

public record PushPing(string Name);
public record PushShipped(string Name);

public static class PushPingHandler
{
    public static readonly ConcurrentQueue<(string Name, int Attempts, string? TenantId)> Handled = new();

    public static CancellationTokenSource? Abort { get; set; }

    public static void Handle(PushPing ping, Envelope envelope)
    {
        if (ping.Name == "throw") throw new DivideByZeroException();
        if (ping.Name == "cancel-then-throw")
        {
            Abort?.Cancel();
            throw new DivideByZeroException();
        }

        Handled.Enqueue((ping.Name, envelope.Attempts, envelope.TenantId));
    }
}

public static class PushTestSupport
{
    public static Task<IHost> StartPushHostAsync(string topic, Action<WolverineOptions>? configure = null) =>
        Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Discovery.DisableConventionalDiscovery().IncludeType(typeof(PushPingHandler));
                opts.UsePubsubTesting().ConfigurePushDelivery(p => p.AllowUnauthenticated());
                opts.ListenToPubsubTopic(topic).UsePushDelivery();
                configure?.Invoke(opts);
            })
            .StartAsync(TestContext.Current.CancellationToken);

    public static PubsubPushRequest RequestFor(IHost host, string topic, object message,
        string? subscriptionProject = "wolverine", int? deliveryAttempt = null)
    {
        var runtime = host.GetRuntime();
        var endpoint = runtime.Options.Transports.GetOrCreate<PubsubTransport>().Topics[topic];

        var envelope = new Envelope(message)
        {
            Data = JsonSerializer.SerializeToUtf8Bytes(message, message.GetType()),
            ContentType = "application/json",
            MessageType = message.GetType().ToMessageTypeName()
        };

        var pubsubMessage = new PubsubMessage { MessageId = Guid.NewGuid().ToString() };
        new PubsubEnvelopeMapper(endpoint).MapEnvelopeToOutgoing(envelope, pubsubMessage);

        var json = JsonSerializer.Serialize(new
        {
            message = new
            {
                data = pubsubMessage.Data.ToBase64(),
                attributes = pubsubMessage.Attributes.ToDictionary(x => x.Key, x => x.Value),
                messageId = pubsubMessage.MessageId,
                publishTime = DateTimeOffset.UtcNow.ToString("O")
            },
            subscription = $"projects/{subscriptionProject}/subscriptions/{endpoint.Server.Subscription.Name.SubscriptionId}",
            deliveryAttempt
        });

        PubsubPushRequest.TryParse(System.Text.Encoding.UTF8.GetBytes(json), out var request, out var error)
            .ShouldBeTrue(error);
        return request!;
    }

    public static WolverineRuntime GetRuntime(this IHost host) =>
        (WolverineRuntime)host.Services.GetRequiredService<IWolverineRuntime>();
}
