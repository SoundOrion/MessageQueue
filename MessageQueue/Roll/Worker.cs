using MessageQueue.Common;
using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace MessageQueue.Roll;

public sealed class Worker
{
    private readonly string _host;
    private readonly int _port;
    private readonly string _pattern;  // 例: "job.assign.*" or "job.assign.calcA.*"
    private readonly Guid _workerId = Guid.NewGuid();
    private readonly DedupCache _dedup = new(TimeSpan.FromMinutes(30));

    // 固定パス
    private const string ExeDir = "/opt/grid/exe";
    private const string WorkRoot = "/tmp/jobs";
    private const string CacheDir = "/opt/grid/cache";

    public Worker(string host, int port, string subjectPattern = "job.assign.*")
    { _host = host; _port = port; _pattern = string.IsNullOrWhiteSpace(subjectPattern) ? "job.assign.*" : subjectPattern; }

    public async Task RunAsync(CancellationToken ct)
    {
        Directory.CreateDirectory(ExeDir);
        Directory.CreateDirectory(WorkRoot);
        Directory.CreateDirectory(CacheDir);

        using var cli = new TcpClient();
        await cli.ConnectAsync(_host, _port, ct);
        using var ns = cli.GetStream();

        // Hello
        await Codec.WriteAsync(ns, new Message { Type = MsgType.HelloWorker, MsgId = _workerId, Subject = _pattern }, ct);

        // 初回クレジット
        await SendCreditAsync(ns, 1, ct);
        Console.WriteLine($"[Worker {_workerId}] started, pattern={_pattern}");

        while (!ct.IsCancellationRequested)
        {
            var m = await Codec.ReadAsync(ns, ct);
            if (m is null) break;

            if (m.Type == MsgType.AssignJob)
            {
                var req = JsonSerializer.Deserialize<JobRequest>(m.Payload)!;
                if (_dedup.Contains(req.JobId))
                {
                    // 既に処理済み → 結果は再生成不可なのでACKのみ（設計次第ではResult再送キャッシュ）
                    await Codec.WriteAsync(ns, new Message { Type = MsgType.AckJob, CorrId = req.JobId }, ct);
                    continue;
                }

                var (status, stdout, stderr, outZip) = await ExecuteJobAsync(req, ct);

                var res = new JobResult(req.JobId, req.ClientId, req.ExecName, status, stdout, stderr, outZip);
                await Codec.WriteAsync(ns, new Message
                {
                    Type = MsgType.AckJob,
                    CorrId = req.JobId,
                    Payload = JsonSerializer.SerializeToUtf8Bytes(res)
                }, ct);

                _dedup.TryAdd(req.JobId);
                _dedup.Sweep();

                await SendCreditAsync(ns, 1, ct);
            }
        }
        Console.WriteLine($"[Worker {_workerId}] stopped");
    }

    private static async Task SendCreditAsync(NetworkStream ns, int credit, CancellationToken ct)
    {
        var buf = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(buf, credit);
        await Codec.WriteAsync(ns, new Message { Type = MsgType.Credit, MsgId = Guid.NewGuid(), Payload = buf }, ct);
    }

    private async Task<(string status, string stdout, string stderr, byte[]? zip)> ExecuteJobAsync(JobRequest req, CancellationToken ct)
    {
        var jobDir = Path.Combine(WorkRoot, req.JobId.ToString("N"));
        Directory.CreateDirectory(jobDir);

        // exe をジョブディレクトリへコピー
        var exeSrc = Path.Combine(ExeDir, req.ExecName + ".exe");
        var exeDst = Path.Combine(jobDir, req.ExecName + ".exe");
        if (!File.Exists(exeSrc))
            return ("FAILED", "", $"Executable not found: {exeSrc}", null);

        File.Copy(exeSrc, exeDst, overwrite: true);
        try { new FileInfo(exeDst).IsReadOnly = false; } catch { /* ignore */ }

        // 入力展開
        foreach (var f in req.Files)
        {
            var path = Path.Combine(jobDir, f.Name);
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            if (f.Content is { Length: > 0 })
                await File.WriteAllBytesAsync(path, f.Content, ct);
            else if (!string.IsNullOrEmpty(f.CacheId))
            {
                var cachePath = Path.Combine(CacheDir, f.CacheId);
                if (!File.Exists(cachePath))
                    return ("FAILED", "", $"Cache miss: {f.CacheId}", null);
                File.Copy(cachePath, path, overwrite: true);
            }
        }

        // 実行
        var psi = new ProcessStartInfo
        {
            FileName = exeDst,
            WorkingDirectory = jobDir,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };
        foreach (var a in req.Args) psi.ArgumentList.Add(a);

        string stdout = "", stderr = "";
        int exit;
        try
        {
            using var proc = Process.Start(psi)!;
            stdout = await proc.StandardOutput.ReadToEndAsync();
            stderr = await proc.StandardError.ReadToEndAsync();
            await proc.WaitForExitAsync(ct);
            exit = proc.ExitCode;
        }
        catch (Exception ex)
        {
            return ("FAILED", "", $"Process error: {ex.Message}", null);
        }

        // 結果を zip
        var zipPath = Path.Combine(jobDir, "result.zip");
        if (File.Exists(zipPath)) File.Delete(zipPath);
        try
        {
            ZipFile.CreateFromDirectory(jobDir, zipPath, CompressionLevel.Optimal, includeBaseDirectory: false);
            var bytes = await File.ReadAllBytesAsync(zipPath, ct);
            return (exit == 0 ? "OK" : "FAILED", stdout, stderr, bytes);
        }
        catch (Exception ex)
        {
            return (exit == 0 ? "OK" : "FAILED", stdout, stderr + $" | zip error: {ex.Message}", null);
        }
        finally
        {
            // 作業掃除は運用ポリシー次第（ここでは残す）
        }
    }
}
