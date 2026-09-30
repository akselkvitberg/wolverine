using System.Diagnostics.CodeAnalysis;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Wolverine.Runtime;

namespace Wolverine.Http;

public static class OutboxRecoveryEndpointExtensions
{
    /// <summary>
    ///     Map a POST endpoint that makes one outbox recovery pass and returns 204 once every recovered message has
    ///     been sent or released again. Point a scheduler (e.g. Cloud Scheduler) at it when the host runs without a
    ///     durability agent and sends through <c>UseDurableInlineOutbox()</c>. The endpoint is not secured by
    ///     Wolverine; add authorization to the returned builder as the host requires.
    /// </summary>
    public static RouteHandlerBuilder MapWolverineOutboxRecovery(this IEndpointRouteBuilder endpoints,
        [StringSyntax("Route")] string pattern = "/_wolverine/outbox/recover")
    {
        return endpoints.MapPost(pattern, async (IWolverineRuntime runtime, CancellationToken cancellation) =>
        {
            await runtime.RecoverOutboxAsync(cancellation);
            return Results.NoContent();
        });
    }
}
