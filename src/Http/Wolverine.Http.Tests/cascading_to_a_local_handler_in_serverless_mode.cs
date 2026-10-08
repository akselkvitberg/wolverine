using Alba;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Wolverine.Runtime.Routing;
using Xunit;

namespace Wolverine.Http.Tests;

// Serverless removes the local queues, so a message cascaded from an HTTP endpoint to a handler in the
// same application has no route. The request must fail rather than return success and drop the message.
public class cascading_to_a_local_handler_in_serverless_mode
{
    [Fact]
    public async Task the_request_fails_and_explains_why_the_message_cannot_be_delivered()
    {
        var builder = WebApplication.CreateBuilder([]);
        builder.Host.UseWolverine(opts =>
        {
            opts.Durability.Mode = DurabilityMode.Serverless;
            opts.Discovery.DisableConventionalDiscovery().IncludeType(typeof(ServerlessCascadedHandler));
            opts.Discovery.IncludeAssembly(typeof(cascading_to_a_local_handler_in_serverless_mode).Assembly);
        });
        builder.Services.AddWolverineHttp();

        await using var host = await AlbaHost.For(builder, app =>
        {
            app.MapWolverineEndpoints(opts => opts.CustomizeHttpEndpointDiscovery(q =>
                q.Excludes.WithCondition("Not the Serverless endpoint",
                    type => type != typeof(ServerlessCascadingEndpoint))));
        });

        var ex = await Should.ThrowAsync<IndeterminateRoutesException>(() =>
            host.Scenario(x => x.Post.Url("/serverless/cascade")));

        ex.Message.ShouldContain(nameof(DurabilityMode.Serverless));
        ex.Message.ShouldContain(typeof(ServerlessCascaded).FullName!);
    }
}

public record ServerlessCascaded(Guid Id);

public static class ServerlessCascadedHandler
{
    public static void Handle(ServerlessCascaded message)
    {
    }
}

public static class ServerlessCascadingEndpoint
{
    [WolverinePost("/serverless/cascade")]
    public static (string, ServerlessCascaded) Post() => ("ok", new ServerlessCascaded(Guid.NewGuid()));
}
