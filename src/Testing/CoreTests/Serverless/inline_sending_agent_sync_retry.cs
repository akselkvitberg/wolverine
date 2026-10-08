using JasperFx.Core;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Wolverine;
using Wolverine.Configuration;
using Wolverine.Logging;
using Wolverine.Runtime;
using Wolverine.Transports;
using Wolverine.Transports.Sending;
using Xunit;

namespace CoreTests.Serverless;

public class inline_sending_agent_sync_retry
{
    [Fact]
    public async Task final_send_failure_is_rethrown_to_the_caller_when_sync_retry_is_on()
    {
        var sender = Substitute.For<ISender>();
        sender.Destination.Returns(new Uri("failing://one"));
        sender.SendAsync(Arg.Any<Envelope>())
            .Returns(_ => ValueTask.FromException(new DivideByZeroException()));

        var agent = new InlineSendingAgent(NullLogger.Instance, sender, new FailingEndpoint(),
            Substitute.For<IMessageTracker>(), new DurabilitySettings { UseSyncRetryBlock = true });

        await Should.ThrowAsync<DivideByZeroException>(async () =>
            await agent.StoreAndForwardAsync(new Envelope { Message = new object() }));

        // RetryBlockSync's default Pauses has four entries: one attempt plus three retries, all in the caller
        await sender.Received(4).SendAsync(Arg.Any<Envelope>());
    }

    private class FailingEndpoint : Endpoint
    {
        public FailingEndpoint() : base(new Uri("failing://one"), EndpointRole.Application)
        {
        }

        public override ValueTask<IListener> BuildListenerAsync(IWolverineRuntime runtime, IReceiver receiver)
            => throw new NotSupportedException();

        protected override ISender CreateSender(IWolverineRuntime runtime) => throw new NotSupportedException();

        protected override bool supportsMode(EndpointMode mode) => true;
    }
}
