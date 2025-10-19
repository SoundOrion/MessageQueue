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
        // dotnet run -- client 127.0.0.1 5000 clientA [desiredParallelism]

        if (args.Length == 0)
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("  leader <port>");
            Console.WriteLine("  worker <host> <port> [pattern]");
            Console.WriteLine("  client <host> <port> <clientId> [desiredParallelism]");
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
                    int? desired = null;
                    if (args.Length > 4 && int.TryParse(args[4], out var d)) desired = d;
                    var cli = new Client(host, port, clientId, desired);
                    await cli.RunAsync(cts.Token);
                    break;
                }
            case "clientx":
                {
                    // 例: dotnet run -- clientx clientA 127.0.0.1:5000 127.0.0.1:5001 127.0.0.1:5002 [desiredTotalParallelism]
                    if (args.Length < 4)
                    {
                        Console.WriteLine("Usage:");
                        Console.WriteLine("  clientx <clientId> <host1:port1> <host2:port2> ... [desiredTotalParallelism]");
                        return;
                    }

                    var clientId = args[1];

                    // 末尾に整数があれば desiredTotal として解釈
                    int desiredTotal = 4;
                    int endpointCount = args.Length - 2;
                    if (int.TryParse(args[^1], out var d))
                    {
                        desiredTotal = Math.Max(1, d);
                        endpointCount = args.Length - 3;
                    }

                    var endpoints = new (string host, int port)[endpointCount];
                    for (int i = 0; i < endpointCount; i++)
                    {
                        var hp = args[2 + i].Split(':', 2);
                        endpoints[i] = (hp[0], int.Parse(hp[1]));
                    }

                    var cliX = new ClientMulti(clientId, endpoints, desiredTotal);
                    await cliX.RunAsync(cts.Token);
                    break;
                }

        }
    }
}
