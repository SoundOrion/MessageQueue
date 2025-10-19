using MessageQueue.Roll;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace MessageQueue;

public static class Program
{
    public static async Task Main()
    {
        var cts = new CancellationTokenSource();

        var leader = new Leader(port: 5000);
        _ = Task.Run(() => leader.RunAsync(cts.Token));

        await Task.Delay(200);

        // 複数ワーカー起動
        for (int i = 0; i < 3; i++)
        {
            _ = Task.Run(async () =>
            {
                var w = new Worker("127.0.0.1", 5000);
                await w.RunAsync(cts.Token);
            });
        }

        await Task.Delay(400);

        var client = new Client("127.0.0.1", 5000);
        await client.RunAsync(cts.Token);

        Console.WriteLine("Press ENTER to stop...");
        Console.ReadLine();
        cts.Cancel();

        Console.WriteLine("Press D=Dump DLQ, R=Requeue DLQ, or ENTER to stop");
        while (true)
        {
            var key = Console.ReadKey(true).Key;
            if (key == ConsoleKey.Enter) break;
            if (key == ConsoleKey.D) leader.DumpDlq();
            if (key == ConsoleKey.R) leader.RequeueDlq();
        }
        cts.Cancel();

    }
}

