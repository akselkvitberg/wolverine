# Wolverine and Serverless

::: tip
No telling when this would happen, but there is an "ultra efficient" serverless model planned for Wolverine
that will lean even heavier into code generation as a way to optimize its usage within serverless functions. Track that [forthcoming
work on GitHub](https://github.com/JasperFx/wolverine/issues/34).
:::

Wolverine was very much originally envisioned for usage in long running processes, and as such, wasn't initially well suited to
serverless technologies like [Azure Functions](https://azure.microsoft.com/en-us/products/functions) or [AWS Lambda functions](https://aws.amazon.com/pm/lambda).

If you're choosing to use Wolverine HTTP endpoints or message handling as part of a serverless function, we have three
main suggestions about making Wolverine be more successful:

1. Make any outgoing [message endpoints](/guide/runtime.html#endpoint-types) be *Inline* so that messages are sent immediately
2. Utilize the new *Serverless* optimized mode
3. Absolutely take advantage of [pre-generated types](/guide/codegen.html#generating-code-ahead-of-time) to cut down the all important cold start problem with serverless functions

## Serverless Mode

::: tip
Wolverine's [Transactional Inbox/Outbox](/guide/durability/) is very unsuitable for usage within serverless functions, so you'll definitely
want to disable it through the mode shown below
:::

First off, let's say that you want to use the transactional
middleware for either Marten or EF Core within your serverless functions. That's all good, but you will want to turn off
all of Wolverine's transactional inbox/outbox functionality with this setting that was added in 1.10.0:

<!-- snippet: sample_configuring_the_serverless_mode -->
<a id='snippet-sample_configuring_the_serverless_mode'></a>
```cs
using var host = await Host.CreateDefaultBuilder()
    .UseWolverine(opts =>
    {
        opts.Services.AddMarten("some connection string")

            // This adds quite a bit of middleware for
            // Marten
            .IntegrateWithWolverine();

        // You want this maybe!
        opts.Policies.AutoApplyTransactions();

        // But wait! Optimize Wolverine for usage within Serverless
        // and turn off the heavy duty, background processes
        // for the transactional inbox/outbox
        opts.Durability.Mode = DurabilityMode.Serverless;
    }).StartAsync();
```
<sup><a href='https://github.com/JasperFx/wolverine/blob/main/src/Samples/DocumentationSamples/DurabilityModes.cs#L12-L32' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_configuring_the_serverless_mode' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Serverless mode also removes Wolverine's [local queues](/guide/messaging/transports/local). That has two consequences:

1. A message that is handled in the same application can only be executed inline with `IMessageBus.InvokeAsync()`. Publishing,
   sending, or scheduling it, cascading it from a handler or HTTP endpoint, or returning it as a [saga timeout](/guide/durability/sagas.html#timeout-messages)
   throws an `IndeterminateRoutesException` unless the message is also routed to an external transport.
2. The local durable queue that holds a scheduled message until it is due, when the destination transport cannot schedule
   it natively, is not available. In this mode, a [scheduled message](/guide/messaging/message-bus.html#scheduling-message-delivery-or-execution)
   can only be sent to an endpoint that supports native scheduled delivery for that message, like the [Azure Service Bus transport](/guide/messaging/transports/azureservicebus/scheduled).
   Scheduling a message to any other endpoint throws an `InvalidOperationException` when it is sent.

## Pre-Generate All Types

The runtime code generation that Wolverine does comes with a potentially non-trivial "cold start" problem with its first
usage. In serverless architectures, that's probably intolerable. With Wolverine, you can bypass that cold start problem
by opting into [pre-generated types](/guide/codegen.html#generating-code-ahead-of-time).

## Use Inline Endpoints

If you are using Wolverine to send cascading messages from handlers in serverless functions, you will want to use
*Inline* endpoints where the messages are sent immediately without any background processing as would be normal with *Buffered* or *Durable*
endpoints:

<!-- snippet: sample_usage_of_send_inline -->
<a id='snippet-sample_usage_of_send_inline'></a>
```cs
.UseWolverine(opts =>
{
    opts.UseRabbitMq().AutoProvision().AutoPurgeOnStartup();
    opts
        .PublishAllMessages()
        .ToRabbitQueue(queueName)

        // This option is important inside of Serverless functions
        .SendInline();
})
```
<sup><a href='https://github.com/JasperFx/wolverine/blob/main/src/Transports/RabbitMQ/Wolverine.RabbitMQ.Tests/Bugs/Bug_189_fails_if_there_are_many_messages_in_queue_on_startup.cs#L21-L33' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_usage_of_send_inline' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->
