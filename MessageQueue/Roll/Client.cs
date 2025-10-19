using MessageQueue.Common;
using System;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace MessageQueue.Roll;

public sealed class Client
{
    private readonly string _host;
    private readonly int _port;
    private readonly string _clientId;
    private readonly int _desiredParallelism;

    // desiredParallelismは引数で指定。nullの場合は環境変数CLIENT_DESIRED_PAR、さらに無ければ4。
    public Client(string host, int port, string clientId, int? desiredParallelism = null)
    {
        _host = host; _port = port; _clientId = clientId;
        if (desiredParallelism.HasValue) _desiredParallelism = Math.Max(1, desiredParallelism.Value);
        else if (int.TryParse(Environment.GetEnvironmentVariable("CLIENT_DESIRED_PAR"), out var p)) _desiredParallelism = Math.Max(1, p);
        else _desiredParallelism = 4;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        using var cli = new TcpClient();
        await cli.ConnectAsync(_host, _port, ct);
        using var ns = cli.GetStream();

        // ★ Hello（SubjectにClientId, PayloadにDesiredParallelismを入れて名乗る）
        var cfg = new ClientConfig(_clientId, _desiredParallelism);
        await Codec.WriteAsync(ns, new Message
        {
            Type = MsgType.HelloClient,
            Subject = _clientId,
            Payload = JsonSerializer.SerializeToUtf8Bytes(cfg)
        }, ct);

        // サンプル：calcA に 3 ジョブ投げる
        for (int i = 0; i < 3; i++)
        {
            var job = new JobRequest(
                JobId: Guid.NewGuid(),
                ClientId: _clientId,
                ExecName: "calcA", // /opt/grid/exe/calcA.exe
                Args: new() { "--mode=fast", $"--seed={i}" },
                Files: new()
                {
                    new InputFile("input.txt", null, Encoding.UTF8.GetBytes($"hello-{i}"))
                }
            );

            await Codec.WriteAsync(ns, new Message
            {
                Type = MsgType.SubmitJob,
                MsgId = job.JobId,
                Subject = $"job.submit.{job.ExecName}",
                Payload = JsonSerializer.SerializeToUtf8Bytes(job)
            }, ct);

            Console.WriteLine($"[Client {_clientId}] submitted {job.JobId} -> {job.ExecName}");
        }

        // 結果待ち（この簡易サンプルでは受信してログに出すだけ）
        int received = 0;
        while (received < 3 && !ct.IsCancellationRequested)
        {
            var m = await Codec.ReadAsync(ns, ct);
            if (m is null) break;
            if (m.Type == MsgType.Result)
            {
                try
                {
                    var res = JsonSerializer.Deserialize<JobResult>(m.Payload)!;
                    Console.WriteLine($"[Client {_clientId}] RESULT {res.JobId} status={res.Status} stdout={res.Stdout.Trim()} stderr={res.Stderr.Trim()} archive={(res.OutputArchive?.Length ?? 0)}B");
                    received++;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Client {_clientId}] bad result payload: {ex.Message}");
                }
            }
        }
    }
}
