using MessageQueue.Common;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
namespace MessageQueue.Roll;

public sealed class Leader
{
    private readonly TcpListener _listener;
    private readonly ConcurrentQueue<byte[]> _queue = new();
    private readonly List<NetworkStream> _workers = new();
    private readonly object _lock = new();

    public Leader(int port) => _listener = new TcpListener(IPAddress.Any, port);

    public async Task RunAsync(CancellationToken ct)
    {
        _listener.Start();
        Console.WriteLine("Leader listening...");
        while (!ct.IsCancellationRequested)
        {
            var client = await _listener.AcceptTcpClientAsync(ct);
            _ = Task.Run(() => HandleClientAsync(client, ct));
        }
    }

    private async Task HandleClientAsync(TcpClient c, CancellationToken ct)
    {
        using var _ = c;
        using var ns = c.GetStream();

        // 役割宣言を待つ
        var hello = await Codec.ReadAsync(ns, ct);
        if (hello is null) return;

        if (hello.Type == MsgType.HelloClient)
        {
            Console.WriteLine("Client connected");
            await HandleSubmitterAsync(ns, ct);
        }
        else if (hello.Type == MsgType.HelloWorker)
        {
            Console.WriteLine("Worker connected");
            lock (_lock) _workers.Add(ns);
            await HandleWorkerAsync(ns, ct);
            lock (_lock) _workers.Remove(ns);
        }
    }

    private async Task HandleSubmitterAsync(NetworkStream ns, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var m = await Codec.ReadAsync(ns, ct);
            if (m is null) break;

            if (m.Type == MsgType.SubmitJob)
            {
                _queue.Enqueue(m.Payload);
                Console.WriteLine($"Enqueued job len={m.Payload.Length}");
            }
        }
    }

    private async Task HandleWorkerAsync(NetworkStream ns, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var m = await Codec.ReadAsync(ns, ct);
            if (m is null) break;

            if (m.Type == MsgType.Credit)
            {
                // 受け入れ枠が来たら配信（最初は常に1想定）
                if (_queue.TryDequeue(out var payload))
                {
                    await Codec.WriteAsync(ns, new Message { Type = MsgType.AssignJob, Payload = payload }, ct);
                    Console.WriteLine($"Assigned job len={payload.Length}");
                }
            }
            else if (m.Type == MsgType.AckJob)
            {
                Console.WriteLine("Ack received from worker");
            }
        }
    }
}
