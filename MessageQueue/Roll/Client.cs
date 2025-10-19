using MessageQueue.Common;
using System.Net.Sockets;
using System.Text;

namespace MessageQueue.Roll;

public sealed class Client
{
    private readonly string _host; private readonly int _port; private readonly string _group;
    public Client(string host, int port, string group)
    { _host = host; _port = port; _group = string.IsNullOrWhiteSpace(group) ? "default" : group; }

    public async Task RunAsync(CancellationToken ct)
    {
        using var client = new TcpClient();
        await client.ConnectAsync(_host, _port, ct);
        using var ns = client.GetStream();

        await Codec.WriteAsync(ns, new Message { Type = MsgType.HelloClient }, ct);

        for (int i = 0; i < 8; i++)
        {
            var jobId = Guid.NewGuid();
            var payload = Encoding.UTF8.GetBytes($"job-{_group}-{i}");
            await Codec.WriteAsync(ns, new Message
            {
                Type = MsgType.SubmitJob,
                MsgId = jobId,
                Subject = _group,
                Payload = payload
            }, ct);
            Console.WriteLine($"[Client({_group})] Submitted {jobId} ({payload.Length}B)");
        }
    }
}


