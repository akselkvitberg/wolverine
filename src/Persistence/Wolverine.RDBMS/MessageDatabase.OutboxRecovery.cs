using JasperFx.Core;
using Microsoft.Extensions.Logging;
using Wolverine.Persistence.Durability;
using Wolverine.RDBMS.Durability;
using Wolverine.RDBMS.Polling;
using Wolverine.Runtime;
using Wolverine.Runtime.Agents;

namespace Wolverine.RDBMS;

public abstract partial class MessageDatabase<T> : IOutboxRecovery
{
    /// <summary>
    ///     On-demand outbox recovery for hosts without a durability agent. It runs the same two operations the
    ///     durability agent's recovery timer runs for the outbox -- bump stale rows, then recover unowned rows per
    ///     destination -- but awaits them, and everything they trigger, on the calling thread.
    ///
    ///     <para>
    ///     Loading and reassigning recovered rows are two statements with no row lock between them, which is safe
    ///     for the durability agent because agent assignment runs it on exactly one node. A recovery endpoint or a
    ///     set of workers has no such guarantee, so the pass is serialized across processes with an advisory lock
    ///     instead, and a caller that cannot take it returns immediately: another pass is already doing the work.
    ///     </para>
    /// </summary>
    public async Task RecoverOutboxAsync(IWolverineRuntime runtime, CancellationToken cancellation)
    {
        if (HasDisposed) return;

        var lockId = $"{SchemaName}:outbox-recovery".GetDeterministicHashCode();

        await using var conn = CreateConnection();
        await conn.OpenAsync(cancellation);

        if (!await TryAttainLockAsync(lockId, conn, cancellation))
        {
            Logger.LogDebug("Skipping outbox recovery for {Database}: another recovery pass holds the lock", Name);
            return;
        }

        try
        {
            var operations = new List<IDatabaseOperation>();
            if (Durability.OutboxStaleTime.HasValue)
            {
                operations.Add(new BumpStaleOutgoingEnvelopesOperation(
                    this.DbObjectNameFor(DatabaseConstants.OutgoingTable), Durability, DateTimeOffset.UtcNow));
            }

            operations.Add(new CheckRecoverableOutgoingMessagesOperation(this, runtime, Logger));

            var commands = await new DatabaseOperationBatch(this, operations.ToArray())
                .ExecuteAsync(runtime, cancellation);

            await executeAsync(commands, runtime, cancellation);
        }
        finally
        {
            await ReleaseLockAsync(lockId, conn, CancellationToken.None);
        }
    }

    private static async Task executeAsync(AgentCommands commands, IWolverineRuntime runtime,
        CancellationToken cancellation)
    {
        foreach (var command in commands)
        {
            cancellation.ThrowIfCancellationRequested();
            var next = await command.ExecuteAsync(runtime, cancellation);
            await executeAsync(next, runtime, cancellation);
        }
    }
}
