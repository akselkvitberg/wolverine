using JasperFx.Core;
using Microsoft.Extensions.Hosting;
using Wolverine;
using Wolverine.Tracking;
using Wolverine.Transports.SharedMemory;
using Xunit;

namespace CoreTests.Runtime.Routing;

// Serverless removes the local transport, so the message router must not resolve the local durable
// queue up front. Before the fix every routed PublishAsync / SendAsync from a Serverless host threw
// UnknownTransportException for local://durable/, even for message types routed to an external transport.
public class routing_in_serverless_mode : IAsyncLifetime
{
    private readonly string _topicName = $"serverless-routing-{Guid.NewGuid():N}";
    private IHost _sender = null!;
    private IHost _receiver = null!;

    public async ValueTask InitializeAsync()
    {
        _sender = await Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Discovery.DisableConventionalDiscovery();
                opts.PublishAllMessages().ToSharedMemoryTopic(_topicName);
            }).StartAsync();

        _receiver = await Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Discovery.DisableConventionalDiscovery().IncludeType(typeof(ServerlessRoutedMessageHandler));
                opts.ListenToSharedMemorySubscription(_topicName, "receiver");
            }).StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _sender.StopAsync();
        _sender.Dispose();
        await _receiver.StopAsync();
        _receiver.Dispose();
        await SharedMemoryQueueManager.ClearAllAsync();
    }

    [Fact]
    public async Task publish_routes_to_an_external_transport()
    {
        var message = new ServerlessRoutedMessage(Guid.NewGuid());

        var session = await _sender.TrackActivity()
            .AlsoTrack(_receiver)
            .ExecuteAndWaitAsync(c => c.PublishAsync(message).AsTask());

        session.Received.SingleMessage<ServerlessRoutedMessage>().Id.ShouldBe(message.Id);
    }

    [Fact]
    public async Task send_routes_to_an_external_transport()
    {
        var message = new ServerlessRoutedMessage(Guid.NewGuid());

        var session = await _sender.TrackActivity()
            .AlsoTrack(_receiver)
            .ExecuteAndWaitAsync(c => c.SendAsync(message).AsTask());

        session.Received.SingleMessage<ServerlessRoutedMessage>().Id.ShouldBe(message.Id);
    }

    [Fact]
    public async Task scheduled_publish_without_native_scheduling_explains_why_it_cannot_be_held()
    {
        var bus = _sender.MessageBus();

        var ex = await Should.ThrowAsync<InvalidOperationException>(() =>
            bus.PublishAsync(new ServerlessRoutedMessage(Guid.NewGuid()),
                new DeliveryOptions { ScheduleDelay = 1.Hours() }).AsTask());

        ex.Message.ShouldContain(nameof(DurabilityMode.Serverless));
        ex.Message.ShouldContain(_topicName);
    }

    [Fact]
    public async Task scheduled_send_to_a_specific_endpoint_without_native_scheduling_explains_why_it_cannot_be_held()
    {
        var bus = _sender.MessageBus();

        var ex = await Should.ThrowAsync<InvalidOperationException>(() =>
            bus.EndpointFor(new Uri($"shared-memory://{_topicName}"))
                .SendAsync(new ServerlessRoutedMessage(Guid.NewGuid()),
                    new DeliveryOptions { ScheduleDelay = 1.Hours() }).AsTask());

        ex.Message.ShouldContain(nameof(DurabilityMode.Serverless));
        ex.Message.ShouldContain(_topicName);
    }
}

public record ServerlessRoutedMessage(Guid Id);

public class ServerlessRoutedMessageHandler
{
    public void Handle(ServerlessRoutedMessage message)
    {
    }
}
