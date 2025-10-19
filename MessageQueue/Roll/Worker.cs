using MessageQueue.Common;
using System.Buffers.Binary;
using System.Net.Sockets;

namespace MessageQueue.Roll;

public sealed class Worker
{
    private readonly string _host;
    private readonly int _port;

    public Worker(string host, int port) { _host = host; _port = port; }

    public async Task RunAsync(CancellationToken ct)
    {
        using var client = new TcpClient();
        await client.ConnectAsync(_host, _port, ct);
        using var ns = client.GetStream();

        await Codec.WriteAsync(ns, new Message { Type = MsgType.HelloWorker }, ct);

        // 最初のクレジット（1）
        await SendCreditAsync(ns, 1, ct);

        while (!ct.IsCancellationRequested)
        {
            var m = await Codec.ReadAsync(ns, ct);
            if (m is null) break;

            if (m.Type == MsgType.AssignJob)
            {
                // ダミー処理（本来は実際のジョブ実行）
                await Task.Delay(500, ct);
                await Codec.WriteAsync(ns, new Message { Type = MsgType.AckJob }, ct);

                // 次のクレジット
                await SendCreditAsync(ns, 1, ct);
            }
        }
    }

    private static async Task SendCreditAsync(NetworkStream ns, int credit, CancellationToken ct)
    {
        var buf = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(buf, credit);
        await Codec.WriteAsync(ns, new Message { Type = MsgType.Credit, Payload = buf.ToArray() }, ct);
    }
}
