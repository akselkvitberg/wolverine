using System.Net.Sockets;
using Google.Api.Gax;

namespace Wolverine.Pubsub.Tests;

public static class TestingExtensions
{
    public const string EmulatorHost = EmulatorAddress + ":8085";

    // The IPv4 literal, not "localhost". CI Linux runners publish the container port on IPv4 only,
    // where [::1]:8085 is unreachable (#3191), and on Windows with WSL "localhost" resolves to ::1
    // first, where wslrelay can hold the port and abort the connection.
    private const string EmulatorAddress = "127.0.0.1";

    public static PubsubConfiguration UsePubsubTesting(this WolverineOptions options)
    {
        Environment.SetEnvironmentVariable("PUBSUB_EMULATOR_HOST", EmulatorHost);
        Environment.SetEnvironmentVariable("PUBSUB_PROJECT_ID", "wolverine");

        return options
            .UsePubsub("wolverine")
            .UseEmulatorDetection(EmulatorDetection.EmulatorOnly);
    }

    /// <summary>
    /// Register an additional, named Pub/Sub broker pointed at a second project on the same emulator. The emulator
    /// accepts arbitrary project ids at request time, so no compose change is needed — just a distinct project id.
    /// </summary>
    public static PubsubConfiguration AddNamedPubsubBrokerTesting(this WolverineOptions options, BrokerName name,
        string projectId)
    {
        Environment.SetEnvironmentVariable("PUBSUB_EMULATOR_HOST", EmulatorHost);

        return options
            .AddNamedPubsubBroker(name, projectId)
            .UseEmulatorDetection(EmulatorDetection.EmulatorOnly);
    }

    /// <summary>
    /// True when the Pub/Sub emulator TCP port is reachable. Integration tests skip-guard on this so the suite is
    /// green when Docker/the emulator is not running.
    /// </summary>
    public static async Task<bool> IsEmulatorAvailable()
    {
        try
        {
            using var client = new TcpClient();
            var connect = client.ConnectAsync(EmulatorAddress, 8085);
            var completed = await Task.WhenAny(connect, Task.Delay(TimeSpan.FromSeconds(2)));
            return completed == connect && client.Connected;
        }
        catch
        {
            return false;
        }
    }
}
