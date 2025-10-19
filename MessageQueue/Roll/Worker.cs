using MessageQueue.Common;
using System.Buffers.Binary;
using System.Net.Sockets;

namespace MessageQueue.Roll;

public sealed class Worker
{
    private readonly string _host;
    private readonly int _port;
    private readonly string _group;
    private readonly Guid _workerId = Guid.NewGuid();
    private readonly DedupCache _dedup = new(TimeSpan.FromMinutes(10));

    public Worker(string host, int port, string group)
    {
        _host = host; 
        _port = port;
        _group = string.IsNullOrWhiteSpace(group) ? "default" : group;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        using var client = new TcpClient();
        await client.ConnectAsync(_host, _port, ct);
        using var ns = client.GetStream();

        // 役割宣言（MsgId = workerId, Subject = group）
        await Codec.WriteAsync(ns, new Message { Type = MsgType.HelloWorker, MsgId = _workerId, Subject = _group }, ct);

        // 最初のクレジット（1）
        await SendCreditAsync(ns, 1, ct);

        Console.WriteLine($"[Worker {_workerId}] started (group '{_group}')");

        while (!ct.IsCancellationRequested)
        {
            var m = await Codec.ReadAsync(ns, ct);
            if (m is null) break;

            if (m.Type == MsgType.AssignJob)
            {
                var jobId = m.MsgId;

                if (_dedupContains(jobId))
                {
                    await Codec.WriteAsync(ns, new Message { Type = MsgType.AckJob, CorrId = jobId, Subject = _group }, ct);
                    continue;
                }

                // 実処理（ダミー）
                await Task.Delay(300, ct);

                _dedup.TryAdd(jobId);
                await Codec.WriteAsync(ns, new Message { Type = MsgType.AckJob, CorrId = jobId, Subject = _group }, ct);

                // 次のクレジット（1）
                await SendCreditAsync(ns, 1, ct);
                _dedup.Sweep();
            }
        }
        Console.WriteLine($"[Worker {_workerId}] stopped (group '{_group}')");
    }

    private bool _dedupContains(Guid id) => _dedup.Contains(id);

    private static async Task SendCreditAsync(NetworkStream ns, int credit, CancellationToken ct)
    {
        var buf = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(buf, credit);
        await Codec.WriteAsync(ns, new Message { Type = MsgType.Credit, MsgId = Guid.NewGuid(), Payload = buf }, ct);
    }
}


