using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Wolverine.Persistence.Durability;
using Wolverine.Runtime;
using Wolverine.Tracking;

namespace Wolverine.Persistence.Durability
{
    /// <summary>
    ///     A message store that can recover its outbox on demand, for hosts that run no durability agent -- see
    ///     <see cref="Wolverine.Configuration.Endpoint.DurableInlineOutbox" />.
    /// </summary>
    public interface IOutboxRecovery
    {
        /// <summary>
        ///     Make one recovery pass over this store's outbox: release rows older than
        ///     <see cref="DurabilitySettings.OutboxStaleTime" /> (when configured) and send every row that no node
        ///     owns, up to <see cref="DurabilitySettings.RecoveryBatchSize" /> per destination. Returns without doing
        ///     anything when another recovery pass holds this store's recovery lock.
        /// </summary>
        Task RecoverOutboxAsync(IWolverineRuntime runtime, CancellationToken cancellation);
    }
}

namespace Wolverine
{
    public static class OutboxRecoveryExtensions
    {
        /// <summary>
        ///     Make one outbox recovery pass over every message store that supports it, awaiting the sends. Meant to be
        ///     called from a scheduled HTTP request or an always-on worker when the host runs without a durability
        ///     agent, e.g. with <see cref="DurabilityMode.Serverless" /> and <c>UseDurableInlineOutbox()</c>.
        /// </summary>
        public static async Task RecoverOutboxAsync(this IWolverineRuntime runtime,
            CancellationToken cancellation = default)
        {
            var stores = await runtime.Stores.FindAllAsync();
            var recoverable = stores.OfType<IOutboxRecovery>().ToList();

            // Silence would look like an empty outbox. Say which stores this cannot reach.
            foreach (var store in stores.Where(x => x is not IOutboxRecovery && x is not NullMessageStore))
            {
                runtime.Logger.LogWarning(
                    "Message store {Store} does not support on-demand outbox recovery, so its outbox was not recovered",
                    store.Uri);
            }

            foreach (var store in recoverable)
            {
                cancellation.ThrowIfCancellationRequested();
                await store.RecoverOutboxAsync(runtime, cancellation);
            }
        }

        /// <inheritdoc cref="RecoverOutboxAsync(IWolverineRuntime, CancellationToken)" />
        public static Task RecoverOutboxAsync(this IHost host, CancellationToken cancellation = default)
        {
            return host.GetRuntime().RecoverOutboxAsync(cancellation);
        }
    }
}
