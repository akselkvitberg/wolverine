using System.Collections.Concurrent;
using IntegrationTests;
using JasperFx.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Shouldly;
using Weasel.Postgresql;
using Wolverine;
using Wolverine.Configuration;
using Wolverine.Persistence.Durability;
using Wolverine.Postgresql;
using Wolverine.RDBMS;
using Wolverine.Runtime;
using Wolverine.Tracking;
using Wolverine.Transports;
using Wolverine.Transports.Sending;

namespace PostgresqlTests.Durability;

/// <summary>
///     UseDurableInlineOutbox(): persisted in the caller's transaction, sent inline after the commit, recovered on
///     demand. Runs in Serverless mode, the host shape it exists for: no durability agent, so nothing but the inline
///     send and an explicit RecoverOutboxAsync(Ct) may ever deliver a message.
/// </summary>
[Collection("marten")]
public class durable_inline_outbox : IAsyncLifetime
{
    private const string SchemaName = "durable_inline_outbox";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private IHost _host = null!;
    private InlineRecordingTransport _transport = null!;

    public async ValueTask InitializeAsync()
    {
        _host = await startHostAsync();
        await _host.RebuildAllEnvelopeStorageAsync();
    }

    private async Task<IHost> startHostAsync(Action<WolverineOptions>? configure = null)
    {
        var transport = new InlineRecordingTransport();
        _transport ??= transport;

        return await Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;
                opts.Durability.OutboxStaleTime = 1.Hours();
                opts.PersistMessagesWithPostgresql(Servers.PostgresConnectionString, SchemaName);
                opts.Transports.Add(_transport);
                opts.PublishMessage<InlineOutboxPing>().To(InlineRecordingTransport.Uri).UseDurableInlineOutbox();
                configure?.Invoke(opts);
            }).StartAsync(Ct);
    }

    public async ValueTask DisposeAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
    }

    private IWolverineRuntime Runtime => _host.GetRuntime();

    private async Task<long> outboxCountAsync(int? ownerId = null)
    {
        await using var conn = new NpgsqlConnection(Servers.PostgresConnectionString);
        await conn.OpenAsync(Ct);
        var sql = $"select count(*) from {SchemaName}.{DatabaseConstants.OutgoingTable}";
        if (ownerId.HasValue) sql += $" where owner_id = {ownerId.Value}";
        return (long)(await conn.CreateCommand(sql).ExecuteScalarAsync(Ct))!;
    }

    private Task publishInTransactionAsync(params InlineOutboxPing[] messages)
        => publishInTransactionAsync(null, messages);

    private async Task publishInTransactionAsync(DeliveryOptions? options, params InlineOutboxPing[] messages)
    {
        var database = (IMessageDatabase)Runtime.Storage;

        await using var conn = new NpgsqlConnection(Servers.PostgresConnectionString);
        await conn.OpenAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(Ct);

        var context = new MessageContext(Runtime);
        await context.EnlistInOutboxAsync(new DatabaseEnvelopeTransaction(database, tx));
        foreach (var message in messages) await context.PublishAsync(message, options);

        // Persisted inside the transaction, nothing sent yet
        _transport.Sender.Sent.ShouldBeEmpty();

        await tx.CommitAsync(Ct);
        await context.FlushOutgoingMessagesAsync();
    }

    [Fact]
    public void the_flagged_endpoint_gets_the_inline_durable_agent_and_survives_serverless_mode()
    {
        var agent = Runtime.Endpoints.GetOrBuildSendingAgent(InlineRecordingTransport.Uri);

        agent.ShouldBeOfType<DurableInlineSendingAgent>();
        agent.IsDurable.ShouldBeTrue();
        agent.Endpoint.Mode.ShouldBe(EndpointMode.Durable);
    }

    [Fact]
    public async Task sends_inline_after_the_commit_and_deletes_the_outbox_row()
    {
        await publishInTransactionAsync(new InlineOutboxPing("one"), new InlineOutboxPing("two"));

        // Delivered before FlushOutgoingMessagesAsync() returned: no background agent to wait for
        _transport.Sender.Sent.Count.ShouldBe(2);
        (await outboxCountAsync()).ShouldBe(0);
    }

    [Fact]
    public async Task a_failed_send_does_not_throw_and_releases_the_row_for_recovery()
    {
        _transport.Sender.Fail = true;

        await publishInTransactionAsync(new InlineOutboxPing("fails"));

        _transport.Sender.Sent.ShouldBeEmpty();
        (await outboxCountAsync(TransportConstants.AnyNode)).ShouldBe(1);
    }

    [Fact]
    public async Task recover_outbox_sends_released_rows_and_empties_the_outbox()
    {
        _transport.Sender.Fail = true;
        await publishInTransactionAsync(new InlineOutboxPing("a"), new InlineOutboxPing("b"));
        (await outboxCountAsync(TransportConstants.AnyNode)).ShouldBe(2);

        _transport.Sender.Fail = false;
        await Runtime.RecoverOutboxAsync(Ct);

        _transport.Sender.Sent.Count.ShouldBe(2);
        (await outboxCountAsync()).ShouldBe(0);
    }

    [Fact]
    public async Task recovery_leaves_rows_that_a_live_node_still_owns()
    {
        // A row a request is publishing right now is owned by that node, not AnyNode
        var envelope = new Envelope(new InlineOutboxPing("in flight"))
        {
            Destination = InlineRecordingTransport.Uri, MessageType = "InlineOutboxPing", ContentType = "application/json",
            Data = [1]
        };
        await Runtime.Storage.Outbox.StoreOutgoingAsync(envelope, 12345);

        await Runtime.RecoverOutboxAsync(Ct);

        _transport.Sender.Sent.ShouldBeEmpty();
        (await outboxCountAsync(12345)).ShouldBe(1);
    }

    [Fact]
    public async Task recovery_sends_rows_whose_owner_went_stale()
    {
        var envelope = new Envelope(new InlineOutboxPing("orphaned"))
        {
            Destination = InlineRecordingTransport.Uri, MessageType = "InlineOutboxPing", ContentType = "application/json",
            Data = [1]
        };
        await Runtime.Storage.Outbox.StoreOutgoingAsync(envelope, 12345);

        await using (var conn = new NpgsqlConnection(Servers.PostgresConnectionString))
        {
            await conn.OpenAsync(Ct);
            await conn.CreateCommand(
                    $"update {SchemaName}.{DatabaseConstants.OutgoingTable} set \"timestamp\" = now() - interval '2 hours'")
                .ExecuteNonQueryAsync(Ct);
        }

        await Runtime.RecoverOutboxAsync(Ct);

        _transport.Sender.Sent.Count.ShouldBe(1);
        (await outboxCountAsync()).ShouldBe(0);
    }

    [Fact]
    public async Task a_scheduled_message_on_a_natively_scheduling_transport_is_sent_now_with_its_delivery_time()
    {
        _transport.Sender.NativeScheduling = true;

        await publishInTransactionAsync(new DeliveryOptions { ScheduleDelay = 1.Hours() },
            new InlineOutboxPing("later"));

        var sent = _transport.Sender.Sent.ShouldHaveSingleItem();
        sent.ScheduledTime.ShouldNotBeNull();
        sent.ScheduledTime!.Value.ShouldBeGreaterThan(DateTimeOffset.UtcNow.AddMinutes(50));
        (await outboxCountAsync()).ShouldBe(0);
    }

    [Fact]
    public async Task publishing_outside_a_transaction_persists_then_sends_inline()
    {
        _transport.Sender.Fail = true;
        await _host.MessageBus().PublishAsync(new InlineOutboxPing("no transaction"));

        // Stored before the send was attempted, so the failure left it behind for recovery
        (await outboxCountAsync(TransportConstants.AnyNode)).ShouldBe(1);

        _transport.Sender.Fail = false;
        await _host.MessageBus().PublishAsync(new InlineOutboxPing("no transaction, healthy"));
        _transport.Sender.Sent.Count.ShouldBe(1);
        (await outboxCountAsync()).ShouldBe(1);
    }

    [Fact]
    public async Task concurrent_recovery_passes_send_each_message_once()
    {
        _transport.Sender.Fail = true;
        await publishInTransactionAsync(Enumerable.Range(0, 20).Select(i => new InlineOutboxPing(i.ToString()))
            .ToArray());

        _transport.Sender.Fail = false;
        _transport.Sender.Delay = 50.Milliseconds();

        // Two independent hosts sharing the store, the way n workers or overlapping scheduler calls would. Same
        // configuration on both: a host without OutboxStaleTime would migrate the timestamp column away.
        using var second = await startHostAsync();
        await Task.WhenAll(Runtime.RecoverOutboxAsync(Ct), second.GetRuntime().RecoverOutboxAsync(Ct),
            Runtime.RecoverOutboxAsync(Ct));

        // One pass wins the recovery lock and sends everything; the others return without sending
        _transport.Sender.Sent.Count.ShouldBe(20);
        _transport.Sender.Sent.Select(x => x.Id).Distinct().Count().ShouldBe(20);
        (await outboxCountAsync()).ShouldBe(0);
        await second.StopAsync(Ct);
    }
}

/// <summary>
///     Unit-level check that the flag is inert unless set: an ordinary durable endpoint keeps the background agent.
/// </summary>
public class durable_inline_outbox_is_opt_in
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task a_durable_endpoint_without_the_flag_keeps_the_durable_sending_agent()
    {
        var transport = new InlineRecordingTransport();
        using var host = await Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.PersistMessagesWithPostgresql(Servers.PostgresConnectionString, "durable_inline_opt_in");
                opts.Transports.Add(transport);
                opts.PublishMessage<InlineOutboxPing>().To(InlineRecordingTransport.Uri).UseDurableOutbox();
            }).StartAsync(Ct);

        var agent = host.GetRuntime().Endpoints.GetOrBuildSendingAgent(InlineRecordingTransport.Uri);
        agent.ShouldBeOfType<DurableSendingAgent>();
        agent.Endpoint.SendsInline.ShouldBeFalse();

        await host.StopAsync(Ct);
    }
}

public record InlineOutboxPing(string Name);

internal class InlineRecordingTransport : TransportBase<InlineRecordingEndpoint>
{
    public const string ProtocolName = "inline-recording";
    public static readonly Uri Uri = new($"{ProtocolName}://one");

    public InlineRecordingTransport() : base(ProtocolName, "Inline recording test transport", [ProtocolName])
    {
        Endpoint = new InlineRecordingEndpoint(Uri, Sender);
    }

    public RecordingInlineSender Sender { get; } = new(Uri);
    public InlineRecordingEndpoint Endpoint { get; }

    protected override IEnumerable<InlineRecordingEndpoint> endpoints() => [Endpoint];

    protected override InlineRecordingEndpoint findEndpointByUri(Uri uri) => Endpoint;
}

internal class InlineRecordingEndpoint : Endpoint
{
    private readonly RecordingInlineSender _sender;

    public InlineRecordingEndpoint(Uri uri, RecordingInlineSender sender) : base(uri, EndpointRole.Application)
    {
        _sender = sender;
    }

    // Stands in for every transport's CreateSender gate: the inline sender only when SendsInline says so
    protected override ISender CreateSender(IWolverineRuntime runtime)
    {
        return SendsInline
            ? _sender
            : new BatchedSender(this, new AlwaysSuccessfulProtocol(), runtime.Cancellation,
                runtime.LoggerFactory.CreateLogger<BatchedSender>());
    }

    public override ValueTask<IListener> BuildListenerAsync(IWolverineRuntime runtime, IReceiver receiver)
    {
        throw new NotSupportedException();
    }

    private class AlwaysSuccessfulProtocol : ISenderProtocol
    {
        public Task SendBatchAsync(ISenderCallback callback, OutgoingMessageBatch batch)
        {
            return callback.MarkSuccessfulAsync(batch);
        }
    }
}

internal class RecordingInlineSender : ISender
{
    public RecordingInlineSender(Uri destination) => Destination = destination;

    public ConcurrentQueue<Envelope> Sent { get; } = new();
    public bool Fail { get; set; }
    public TimeSpan Delay { get; set; } = TimeSpan.Zero;

    public bool NativeScheduling { get; set; }
    public bool SupportsNativeScheduledSend => NativeScheduling;
    public Uri Destination { get; }

    public Task<bool> PingAsync() => Task.FromResult(true);

    public async ValueTask SendAsync(Envelope envelope)
    {
        if (Delay > TimeSpan.Zero) await Task.Delay(Delay);
        if (Fail) throw new InvalidOperationException("Simulated transport failure");
        Sent.Enqueue(envelope);
    }
}
