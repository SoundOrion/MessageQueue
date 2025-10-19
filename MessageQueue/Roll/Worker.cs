using MessageQueue.Common;
using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Net.Sockets;
using System.Runtime.InteropServices;
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

    // 固定パス（必要なら開発用に相対へ変更可）
    private const string ExeDir = "/opt/grid/exe";
    private const string WorkRoot = "/tmp/jobs";
    private const string CacheDir = "/opt/grid/cache";

    // ★ 並列上限（プロセス本数） & 送信直列化
    private readonly int _maxParallel;
    private readonly SemaphoreSlim _slots;
    private readonly SemaphoreSlim _sendLock = new(1, 1);

    // ★ CPUスロットル（100%張り付き回避）
    private readonly double _cpuCap; // 0.75 = 75%
    private readonly CpuMonitor _cpu;

    public Worker(string host, int port, string subjectPattern = "job.assign.*")
    {
        _host = host; _port = port;
        _pattern = string.IsNullOrWhiteSpace(subjectPattern) ? "job.assign.*" : subjectPattern;

        // 環境変数で上限/CPU閾値を調整可能
        if (!int.TryParse(Environment.GetEnvironmentVariable("WORKER_MAX_PAR"), out _maxParallel))
            _maxParallel = 4;
        _maxParallel = Math.Max(1, _maxParallel);

        if (!double.TryParse(Environment.GetEnvironmentVariable("WORKER_CPU_CAP"), out _cpuCap))
            _cpuCap = 0.75; // 75%
        _cpuCap = Math.Clamp(_cpuCap, 0.10, 0.95);

        _slots = new SemaphoreSlim(_maxParallel, _maxParallel);
        _cpu = new CpuMonitor();
    }

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

        // ★ 初回クレジット：並列上限ぶん
        await SendCreditSafeAsync(ns, _maxParallel, ct);
        Console.WriteLine($"[Worker {_workerId}] started, pattern={_pattern}, maxParallel={_maxParallel}, cpuCap={(int)(_cpuCap * 100)}%");

        while (!ct.IsCancellationRequested)
        {
            var m = await Codec.ReadAsync(ns, ct);
            if (m is null) break;

            if (m.Type == MsgType.AssignJob)
            {
                JobRequest req;
                try { req = JsonSerializer.Deserialize<JobRequest>(m.Payload)!; }
                catch { continue; }

                // ★ 非同期で処理（受信ループはすぐ次へ）
                _ = Task.Run(() => ProcessOneAsync(ns, req, ct), ct);
            }
        }
        Console.WriteLine($"[Worker {_workerId}] stopped");
    }

    private async Task ProcessOneAsync(NetworkStream ns, JobRequest req, CancellationToken ct)
    {
        await _slots.WaitAsync(ct);
        try
        {
            if (_dedup.Contains(req.JobId))
            {
                var resDup = new JobResult(req.JobId, req.ClientId, req.ExecName, "DUPLICATE", "", "", null);
                await SendAsync(ns, new Message
                {
                    Type = MsgType.AckJob,
                    CorrId = req.JobId,
                    Payload = JsonSerializer.SerializeToUtf8Bytes(resDup)
                }, ct);
                _dedup.Sweep();

                // ★ CPUスロットル考慮のクレジット返却
                await WaitCpuUnderCapAsync(ct);
                await SendCreditSafeAsync(ns, 1, ct);
                return;
            }

            var (status, stdout, stderr, outZip) = await ExecuteJobAsync(req, ct);
            var res = new JobResult(req.JobId, req.ClientId, req.ExecName, status, stdout, stderr, outZip);

            await SendAsync(ns, new Message
            {
                Type = MsgType.AckJob,
                CorrId = req.JobId,
                Payload = JsonSerializer.SerializeToUtf8Bytes(res)
            }, ct);

            _dedup.TryAdd(req.JobId);
            _dedup.Sweep();

            // ★ CPUスロットル考慮のクレジット返却
            await WaitCpuUnderCapAsync(ct);
            await SendCreditSafeAsync(ns, 1, ct);
        }
        catch (Exception ex)
        {
            try
            {
                var res = new JobResult(req.JobId, req.ClientId, req.ExecName, "FAILED", "", $"worker error: {ex.Message}", null);
                await SendAsync(ns, new Message
                {
                    Type = MsgType.AckJob,
                    CorrId = req.JobId,
                    Payload = JsonSerializer.SerializeToUtf8Bytes(res)
                }, ct);

                await WaitCpuUnderCapAsync(ct);
                await SendCreditSafeAsync(ns, 1, ct);
            }
            catch { /* 二重故障は諦める */ }
        }
        finally
        {
            _slots.Release();
        }
    }

    private async Task SendCreditSafeAsync(NetworkStream ns, int credit, CancellationToken ct)
    {
        var buf = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(buf, credit);
        await SendAsync(ns, new Message { Type = MsgType.Credit, MsgId = Guid.NewGuid(), Payload = buf }, ct);
    }

    private async Task SendAsync(NetworkStream ns, Message msg, CancellationToken ct)
    {
        await _sendLock.WaitAsync(ct);
        try { await Codec.WriteAsync(ns, msg, ct); }
        finally { _sendLock.Release(); }
    }

    // CPU使用率が高いときは少し待ってからクレジット返却（Linuxではシステム全体%を使う）
    private async Task WaitCpuUnderCapAsync(CancellationToken ct)
    {
        // まずは短い休止でバーストを抑える
        await Task.Delay(10, ct);

        // Linuxでは /proc/stat からシステムCPU% を測る（他OSはノーオペor軽い待機）
        if (_cpu.CanMeasureSystem)
        {
            for (int i = 0; i < 10; i++)
            {
                var usage = await _cpu.GetSystemCpuUsageAsync(ct); // 0.0..1.0
                if (usage <= _cpuCap) break;
                await Task.Delay(50, ct);
            }
        }
        else
        {
            // 非Linuxは軽い待機だけ（必要なら環境依存実装を追加）
            await Task.Delay(10, ct);
        }
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

        // 入力展開（簡易パス検証）
        foreach (var f in req.Files)
        {
            if (string.IsNullOrEmpty(f.Name) || f.Name.Contains("..") || f.Name.Contains('\\') || f.Name.Contains('/'))
                return ("FAILED", "", "invalid file name", null);

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
    }

    // ==== 簡易CPUモニタ（Linux: /proc/stat、他OS: 測定不可として待機のみ） ====
    private sealed class CpuMonitor
    {
        private long _prevIdle = -1, _prevTotal = -1;
        public bool CanMeasureSystem => RuntimeInformation.IsOSPlatform(OSPlatform.Linux);

        public async Task<double> GetSystemCpuUsageAsync(CancellationToken ct)
        {
            if (!CanMeasureSystem) return 0.0;
            // /proc/stat の1行目: cpu  user nice system idle iowait irq softirq steal guest guest_nice
            static (long idle, long total) Read()
            {
                using var sr = new StreamReader("/proc/stat");
                var line = sr.ReadLine();
                if (line == null || !line.StartsWith("cpu ")) return (0, 0);
                var parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                // user nice system idle iowait irq softirq steal ...
                long user = long.Parse(parts[1]);
                long nice = long.Parse(parts[2]);
                long system = long.Parse(parts[3]);
                long idle = long.Parse(parts[4]);
                long iowait = parts.Length > 5 ? long.Parse(parts[5]) : 0;
                long irq = parts.Length > 6 ? long.Parse(parts[6]) : 0;
                long softirq = parts.Length > 7 ? long.Parse(parts[7]) : 0;
                long steal = parts.Length > 8 ? long.Parse(parts[8]) : 0;
                long total = user + nice + system + idle + iowait + irq + softirq + steal;
                return (idle, total);
            }

            var (idle1, total1) = Read();
            await Task.Delay(80, ct);
            var (idle2, total2) = Read();

            long idle = idle2 - idle1;
            long total = total2 - total1;
            if (total <= 0) return 0.0;
            double busy = 1.0 - (double)idle / total; // 0..1
            return Math.Clamp(busy, 0.0, 1.0);
        }
    }
}
