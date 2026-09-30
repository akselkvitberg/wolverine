using System.Collections.Concurrent;
using IntegrationTests;
using JasperFx;
using JasperFx.Resources;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using SharedPersistenceModels.Items;
using Shouldly;
using Wolverine;
using Wolverine.Configuration;
using Wolverine.EntityFrameworkCore;
using Wolverine.Persistence;
using Wolverine.Persistence.Durability;
using Wolverine.Postgresql;
using Wolverine.Runtime;
using Wolverine.Transports;
using Wolverine.Transports.Sending;
using Xunit;

namespace EfCoreTests;

/// <summary>
///     The durable inline outbox through the real EF Core transactional middleware and generated handler code, rather
///     than a hand-built transaction: the cascaded message must be sent inline, only after the business write is
///     committed, and its outbox row must be gone when the handler call returns.
/// </summary>
[Collection("postgresql")]
public class durable_inline_outbox_with_ef_core
{
    [Theory]
    [InlineData(TransactionMiddlewareMode.Eager, "inline_outbox_ef_eager")]
    [InlineData(TransactionMiddlewareMode.Lightweight, "inline_outbox_ef_light")]
    public async Task the_cascaded_message_is_sent_after_the_commit_and_leaves_no_outbox_row(
        TransactionMiddlewareMode mode, string schema)
    {
        var transport = new EfInlineRecordingTransport();

        using var host = await Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Serverless;

                opts.Services.AddDbContextWithWolverineIntegration<ItemsDbContext>(o =>
                {
                    o.UseNpgsql(Servers.PostgresConnectionString);
                });

                opts.PersistMessagesWithPostgresql(Servers.PostgresConnectionString, schema);
                opts.UseEntityFrameworkCoreTransactions(mode);
                opts.Policies.AutoApplyTransactions();

                opts.Transports.Add(transport);
                opts.PublishMessage<InlineOutboxItemCreated>().To(EfInlineRecordingTransport.Uri)
                    .UseDurableInlineOutbox();

                opts.UseEntityFrameworkCoreWolverineManagedMigrations();
                opts.Services.AddResourceSetupOnStartup(StartupAction.ResetState);
            })
            .StartAsync(TestContext.Current.CancellationToken);

        var id = Guid.NewGuid();
        await host.MessageBus().InvokeAsync(new CreateInlineOutboxItem(id, "inline"), TestContext.Current.CancellationToken);

        // Sent inline: already there when InvokeAsync returns, with no background agent to wait for
        var sent = transport.Sender.Sent.ShouldHaveSingleItem();
        sent.Message.ShouldBeOfType<InlineOutboxItemCreated>().Id.ShouldBe(id);

        // ...and only after the business write was visible outside the transaction
        transport.Sender.ItemWasCommittedWhenSent[id].ShouldBeTrue();

        var counts = await host.Services.GetRequiredService<IMessageStore>().Admin.FetchCountsAsync();
        counts.Outgoing.ShouldBe(0);
    }
}

public record CreateInlineOutboxItem(Guid Id, string Name);

public record InlineOutboxItemCreated(Guid Id);

public static class CreateInlineOutboxItemHandler
{
    public static InlineOutboxItemCreated Handle(CreateInlineOutboxItem command, ItemsDbContext db)
    {
        db.Items.Add(new Item { Id = command.Id, Name = command.Name });
        return new InlineOutboxItemCreated(command.Id);
    }
}

internal class EfInlineRecordingTransport : TransportBase<EfInlineRecordingEndpoint>
{
    public const string ProtocolName = "ef-inline-recording";
    public static readonly Uri Uri = new($"{ProtocolName}://one");

    public EfInlineRecordingTransport() : base(ProtocolName, "EF Core inline recording test transport", [ProtocolName])
    {
        Endpoint = new EfInlineRecordingEndpoint(Uri, Sender);
    }

    public EfInlineRecordingSender Sender { get; } = new(Uri);
    public EfInlineRecordingEndpoint Endpoint { get; }

    protected override IEnumerable<EfInlineRecordingEndpoint> endpoints() => [Endpoint];

    protected override EfInlineRecordingEndpoint findEndpointByUri(Uri uri) => Endpoint;
}

internal class EfInlineRecordingEndpoint : Endpoint
{
    private readonly EfInlineRecordingSender _sender;

    public EfInlineRecordingEndpoint(Uri uri, EfInlineRecordingSender sender) : base(uri, EndpointRole.Application)
    {
        _sender = sender;
    }

    protected override ISender CreateSender(IWolverineRuntime runtime) => _sender;

    public override ValueTask<IListener> BuildListenerAsync(IWolverineRuntime runtime, IReceiver receiver)
    {
        throw new NotSupportedException();
    }
}

internal class EfInlineRecordingSender : ISender
{
    public EfInlineRecordingSender(Uri destination) => Destination = destination;

    public ConcurrentQueue<Envelope> Sent { get; } = new();
    public ConcurrentDictionary<Guid, bool> ItemWasCommittedWhenSent { get; } = new();

    public bool SupportsNativeScheduledSend => false;
    public Uri Destination { get; }

    public Task<bool> PingAsync() => Task.FromResult(true);

    public async ValueTask SendAsync(Envelope envelope)
    {
        if (envelope.Message is InlineOutboxItemCreated created)
        {
            // A separate connection only sees committed rows
            await using var conn = new NpgsqlConnection(Servers.PostgresConnectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand("select count(*) from mt_items.items where id = @id", conn);
            cmd.Parameters.AddWithValue("id", created.Id);
            ItemWasCommittedWhenSent[created.Id] = (long)(await cmd.ExecuteScalarAsync())! == 1;
        }

        Sent.Enqueue(envelope);
    }
}
