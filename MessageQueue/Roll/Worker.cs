using MessageQueue.Common;
using System.Buffers.Binary;
using System.Net.Sockets;

namespace MessageQueue.Roll;

public sealed class Worker
{
    private readonly string _host; private readonly int _port;
    private readonly DedupCache _dedup = new(TimeSpan.FromMinutes(10));

    public Worker(string host, int port) { _host = host; _port = port; }

    public async Task RunAsync(CancellationToken ct)
    {
        using var client = new TcpClient();
        await client.ConnectAsync(_host, _port, ct);
        using var ns = client.GetStream();

        await Codec.WriteAsync(ns, new Message { Type = MsgType.HelloWorker }, ct);

        // 最初のクレジット
        await SendCreditAsync(ns, 1, ct);

        while (!ct.IsCancellationRequested)
        {
            var m = await Codec.ReadAsync(ns, ct);
            if (m is null) break;

            if (m.Type == MsgType.AssignJob)
            {
                var jobId = m.MsgId; // = JobId
                if (_dedup.Contains(jobId))
                {
                    // すでに処理済み → 即AckでOK
                    await Codec.WriteAsync(ns, new Message { Type = MsgType.AckJob, CorrId = jobId }, ct);
                    continue;
                }

                // 実処理（ここはアプリ依存）
                await Task.Delay(500, ct);

                _dedup.TryAdd(jobId);
                await Codec.WriteAsync(ns, new Message { Type = MsgType.AckJob, CorrId = jobId }, ct);

                // 次のクレジット
                await SendCreditAsync(ns, 1, ct);
                _dedup.Sweep();
            }
        }
    }

    private static async Task SendCreditAsync(NetworkStream ns, int credit, CancellationToken ct)
    {
        Span<byte> buf = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(buf, credit);
        await Codec.WriteAsync(ns, new Message { Type = MsgType.Credit, MsgId = Guid.NewGuid(), Payload = buf.ToArray() }, ct);
    }
}
