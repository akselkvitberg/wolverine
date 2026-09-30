using JasperFx.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Wolverine.Persistence.Durability;
using Wolverine.Runtime;

namespace Wolverine.Persistence.Durability
{
    /// <summary>
    ///     Calls <see cref="OutboxRecoveryExtensions.RecoverOutboxAsync(IWolverineRuntime, CancellationToken)" /> on
    ///     a fixed interval. Registered only by <c>AddWolverineOutboxRecoveryWorker()</c>, for a dedicated, always-on
    ///     process that recovers the outbox on behalf of hosts that do no background work themselves.
    /// </summary>
    internal class OutboxRecoveryWorker : BackgroundService
    {
        private readonly IWolverineRuntime _runtime;
        private readonly IHostApplicationLifetime _lifetime;
        private readonly TimeSpan _interval;
        private readonly ILogger<OutboxRecoveryWorker> _logger;

        public OutboxRecoveryWorker(IWolverineRuntime runtime, IHostApplicationLifetime lifetime,
            OutboxRecoveryWorkerSettings settings, ILogger<OutboxRecoveryWorker> logger)
        {
            _runtime = runtime;
            _lifetime = lifetime;
            _interval = settings.Interval;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // Wolverine's own hosted service has to have started its transports before anything can be sent.
            var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await using (_lifetime.ApplicationStarted.Register(() => started.TrySetResult()))
            await using (stoppingToken.Register(() => started.TrySetCanceled(stoppingToken)))
            {
                try
                {
                    await started.Task;
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }

            using var timer = new PeriodicTimer(_interval);
            do
            {
                try
                {
                    await _runtime.RecoverOutboxAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Outbox recovery pass failed; retrying in {Interval}", _interval);
                }
            } while (await timer.WaitForNextTickAsync(stoppingToken));
        }
    }

    internal record OutboxRecoveryWorkerSettings(TimeSpan Interval);
}

namespace Wolverine
{
    public static class OutboxRecoveryWorkerExtensions
    {
        /// <summary>
        ///     Run outbox recovery in the background of this host every <paramref name="interval" /> (default five
        ///     seconds). Use it in a separate, always-on worker process that shares the message store with API hosts
        ///     using <c>UseDurableInlineOutbox()</c>. Recovery passes are serialized across processes, so running
        ///     several workers is safe.
        /// </summary>
        public static IServiceCollection AddWolverineOutboxRecoveryWorker(this IServiceCollection services,
            TimeSpan? interval = null)
        {
            services.AddSingleton(new OutboxRecoveryWorkerSettings(interval ?? 5.Seconds()));
            services.AddHostedService<OutboxRecoveryWorker>();
            return services;
        }
    }
}
