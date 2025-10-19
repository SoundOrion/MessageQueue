using MessageQueue;

var cts = new CancellationTokenSource();

var leader = new Leader(port: 5000);
_ = Task.Run(() => leader.RunAsync(cts.Token));

// 少し待ってからWorker/Client起動
await Task.Delay(200);

var worker = new Worker("127.0.0.1", 5000);
_ = Task.Run(() => worker.RunAsync(cts.Token));

await Task.Delay(200);

var client = new Client("127.0.0.1", 5000);
await client.RunAsync(cts.Token);

// Ctrl+C 等で終了させる想定
Console.ReadLine();
cts.Cancel();
