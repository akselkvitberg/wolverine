using JasperFx.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Wolverine.Pubsub.Push;
using Wolverine.Runtime;

namespace Wolverine.Pubsub.AspNetCore;

public static class PubsubPushEndpointRouteBuilderExtensions
{
    /// <summary>
    /// Map the single POST route that receives Google Cloud Pub/Sub push requests for every endpoint configured
    /// with UsePushDelivery(). The route is ConfigurePushDelivery()'s RoutePrefix + "/{endpointName}"
    /// </summary>
    public static IEndpointConventionBuilder MapWolverinePubsubPush(this IEndpointRouteBuilder endpoints)
    {
        var services = endpoints.ServiceProvider;
        var runtime = services.GetRequiredService<IWolverineRuntime>();
        var push = runtime.Options.Transports.GetOrCreate<PubsubTransport>().Push;
        if (push.IsRouteMapped)
        {
            throw new InvalidOperationException("MapWolverinePubsubPush() was already called; it maps one route for every push endpoint");
        }

        push.IsRouteMapped = true;

        var processor = services.GetRequiredService<PubsubPushProcessor>();
        services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping.Register(processor.LatchAll);

        var validator = services.GetService<IPubsubPushTokenValidator>() ?? new GooglePubsubPushTokenValidator();
        var authenticator = new PubsubPushAuthenticator(validator,
            services.GetRequiredService<ILogger<PubsubPushAuthenticator>>());
        var logger = services.GetRequiredService<ILoggerFactory>().CreateLogger("Wolverine.Pubsub.Push");

        var prefix = "/" + push.RoutePrefix.Trim('/');
        return endpoints.MapPost(prefix + "/{endpointName}", async (HttpContext context, string endpointName) =>
        {
            if (!processor.TryFindEndpoint(endpointName, out var endpoint))
            {
                context.Response.StatusCode = 404;
                return;
            }

            var denied = await authenticator.AuthenticateAsync(context.Request, endpoint!);
            if (denied.HasValue)
            {
                context.Response.StatusCode = denied.Value;
                return;
            }

            var body = await context.Request.Body.ReadAllBytesAsync();
            if (!PubsubPushRequest.TryParse(body, out var request, out var error))
            {
                logger.LogWarning("Rejected Pub/Sub push request for {EndpointName}: {Error}", endpointName, error);
                context.Response.StatusCode = 400;
                return;
            }

            var result = await processor.ProcessAsync(endpointName, request!, context.RequestAborted);
            if (!result.IsAck)
            {
                logger.LogInformation("Pub/Sub push request for {EndpointName} answered {StatusCode}: {Reason}",
                    endpointName, result.StatusCode, result.Reason);
            }

            context.Response.StatusCode = result.StatusCode;
        });
    }
}
