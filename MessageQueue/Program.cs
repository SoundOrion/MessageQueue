using MessageQueue.Roll;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace MessageQueue;

public static class Program
{
    public static async Task Main(string[] args)
    {
        // 使い方:
        // dotnet run -- leader 5000
        // dotnet run -- worker 127.0.0.1 5000 job.assign.*
        // dotnet run -- client 127.0.0.1 5000 clientA

        if (args.Length == 0)
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("  leader <port>");
            Console.WriteLine("  worker <host> <port> [pattern]");
            Console.WriteLine("  client <host> <port> <clientId>");
            return;
        }

        var cts = new CancellationTokenSource();

        switch (args[0].ToLowerInvariant())
        {
            case "leader":
                {
                    int port = int.Parse(args[1]);
                    var leader = new Leader(port);
                    await leader.RunAsync(cts.Token);
                    break;
                }
            case "worker":
                {
                    var host = args[1];
                    int port = int.Parse(args[2]);
                    var pattern = args.Length > 3 ? args[3] : "job.assign.*";
                    var w = new Worker(host, port, pattern);
                    await w.RunAsync(cts.Token);
                    break;
                }
            case "client":
                {
                    var host = args[1];
                    int port = int.Parse(args[2]);
                    var clientId = args[3];
                    var cli = new Client(host, port, clientId);
                    await cli.RunAsync(cts.Token);
                    break;
                }
        }
    }
}
