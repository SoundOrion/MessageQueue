using MessageQueue.Common;
using System.Net.Sockets;
using System.Text;

namespace MessageQueue.Roll;

public sealed class Client
{
    private readonly string _host; private readonly int _port;
    public Client(string host, int port) { _host = host; _port = port; }

    public async Task RunAsync(CancellationToken ct)
    {
        using var client = new TcpClient();
        await client.ConnectAsync(_host, _port, ct);
        using var ns = client.GetStream();

        await Codec.WriteAsync(ns, new Message { Type = MsgType.HelloClient }, ct);

        for (int i = 0; i < 10; i++)
        {
            var jobId = Guid.NewGuid();
            var payload = Encoding.UTF8.GetBytes($"job-{i}");
            await Codec.WriteAsync(ns, new Message { Type = MsgType.SubmitJob, MsgId = jobId, Payload = payload }, ct);
            Console.WriteLine($"[Client] Submitted {jobId} ({payload.Length}B)");
        }
    }
}

