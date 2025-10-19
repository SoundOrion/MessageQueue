using MessageQueue.Roll;

namespace MessageQueue;

//使い方メモ
//起動すると Leader(5000) → Worker(A×2, B×1) → Client が A/B にそれぞれ 8 件ずつ投入します。
//Leader は Subject=グループ でキューを分け、同じグループに属するワーカーへだけ割り当てます。
//ACK が来ないと 指数バックオフで再送、上限で DLQ に隔離。
//Program のコンソールで D（DLQ一覧）、R（DLQ再投入） が使えます。

//機能は以下を含みます：
//Subject 付きメッセージ（Subject 文字列）
//グループ別キュー（_queues["group"]）
//複数ワーカー（各ワーカーは所属グループを名乗る）
//クレジット制
//再送（指数バックオフ＋最大試行回数）
//再送上限で DLQ（メモリ） に隔離（必要なら後で永続化へ拡張可能）
//重複排除（Submit重複／Assign重複）

public static class Program
{
    public static async Task Main()
    {
        var cts = new CancellationTokenSource();

        var leader = new Leader(port: 5000);
        _ = Task.Run(() => leader.RunAsync(cts.Token));

        await Task.Delay(200);

        // グループAにワーカー2台、グループBに1台
        _ = Task.Run(async () => { var w = new Worker("127.0.0.1", 5000, "A"); await w.RunAsync(cts.Token); });
        _ = Task.Run(async () => { var w = new Worker("127.0.0.1", 5000, "A"); await w.RunAsync(cts.Token); });
        _ = Task.Run(async () => { var w = new Worker("127.0.0.1", 5000, "B"); await w.RunAsync(cts.Token); });

        await Task.Delay(400);

        // クライアントは A / B それぞれにジョブ投入
        var clientA = new Client("127.0.0.1", 5000, "A");
        var clientB = new Client("127.0.0.1", 5000, "B");
        await clientA.RunAsync(cts.Token);
        await clientB.RunAsync(cts.Token);

        Console.WriteLine("Press D=Dump DLQ, R=Requeue DLQ, ENTER=stop");
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

