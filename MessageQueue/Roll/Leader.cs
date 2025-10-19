using MessageQueue.Common;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Timers;

namespace MessageQueue.Roll;

public sealed class Leader
{
    private readonly TcpListener _listener;

    // ClientId -> connection
    private readonly ConcurrentDictionary<string, ClientConn> _clients = new();

    // WorkerId -> connection
    // ワーカー一覧は複数スレッドから読むのでロック保護
    private readonly Dictionary<Guid, WorkerConn> _workers = new();
    private readonly object _lock = new();

    // ExecName -> queue
    private readonly ConcurrentDictionary<string, ConcurrentQueue<JobEnvelope>> _execQueues = new();
    private readonly ConcurrentQueue<string> _execRound = new();
    private readonly ConcurrentDictionary<string, byte> _execInRound = new();

    // Submit 重複排除（スレッドセーフに）
    // 値はダミーの byte。TryAdd で原子的に検査+追加。
    private readonly ConcurrentDictionary<Guid, byte> _submitted = new();

    // In-flight
    private readonly ConcurrentDictionary<Guid, Inflight> _inflight = new();

    // 再送パラメータ
    private static readonly TimeSpan InitialAckTimeout = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxAckTimeout = TimeSpan.FromSeconds(30);
    private const double BackoffFactor = 2.0;
    private const double JitterRate = 0.20;
    private const int MaxAttempts = 6;
    private static readonly ThreadLocal<Random> _rng = new(() => new Random());

    // DLQ（簡易）
    private readonly ConcurrentQueue<JobEnvelope> _dlq = new();

    private readonly System.Timers.Timer _pumpTimer;

    public Leader(int port)
    {
        _listener = new TcpListener(IPAddress.Any, port);

        // 定期チェック（タイムアウト監視 + スケジューリング）
        _pumpTimer = new System.Timers.Timer(200);
        _pumpTimer.AutoReset = true;
        _pumpTimer.Elapsed += (_, __) =>
        {
            try
            {
                CheckTimeouts();
                PumpAllExec();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Leader] pump error: {ex.Message}");
            }
        };
    }

    public async Task RunAsync(CancellationToken ct)
    {
        _listener.Start();
        _pumpTimer.Start();
        Console.WriteLine("[Leader] listening...");

        try
        {
            while (!ct.IsCancellationRequested)
            {
                var c = await _listener.AcceptTcpClientAsync(ct);
                _ = Task.Run(() => HandleConnAsync(c, ct), ct);
            }
        }
        finally
        {
            _pumpTimer.Stop();
            _listener.Stop();
        }
    }

    private async Task HandleConnAsync(TcpClient tcp, CancellationToken ct)
    {
        using var tcpLease = tcp;
        using var ns = tcp.GetStream();

        var hello = await Codec.ReadAsync(ns, ct);
        if (hello is null) return;

        if (hello.Type == MsgType.HelloClient)
        {
            // ClientId は Subject に入れて名乗る
            var clientId = string.IsNullOrWhiteSpace(hello.Subject) ? Guid.NewGuid().ToString("N") : hello.Subject;
            var cc = new ClientConn(clientId, ns);
            _clients[clientId] = cc;
            Console.WriteLine($"[Leader] Client connected: {clientId}");

            try
            {
                await HandleClientAsync(cc, ct);
            }
            finally
            {
                cc.Stop(); // 送信ループ停止
                _clients.TryRemove(clientId, out _);
                Console.WriteLine($"[Leader] Client disconnected: {clientId}");
            }
        }
        else if (hello.Type == MsgType.HelloWorker)
        {
            var workerId = hello.MsgId;
            var pattern = string.IsNullOrWhiteSpace(hello.Subject) ? "job.assign.*" : hello.Subject;
            var wc = new WorkerConn(workerId, ns, pattern);

            lock (_lock) _workers[workerId] = wc;
            Console.WriteLine($"[Leader] Worker {workerId} joined ({pattern})");

            try
            {
                await HandleWorkerAsync(wc, ct);
            }
            finally
            {
                lock (_lock) _workers.Remove(workerId);
                wc.Stop(); // 送信ループ停止
                Console.WriteLine($"[Leader] Worker disconnected: {workerId}");

                // in-flight の回収（担当者が落ちた分を再投入）
                foreach (var kv in _inflight.Where(kv => ReferenceEquals(kv.Value.Owner, wc)).ToArray())
                {
                    if (_inflight.TryRemove(kv.Key, out var inf))
                    {
                        Enqueue(inf.Job.ExecName, inf.Job);
                        Console.WriteLine($"[Leader] Requeued {inf.Job.JobId} (owner down)");
                    }
                }
            }
        }
    }

    // ===== Clients =====
    private async Task HandleClientAsync(ClientConn cc, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var m = await Codec.ReadAsync(cc.Stream, ct);
            if (m is null) break;

            if (m.Type == MsgType.SubmitJob)
            {
                // 原子的に重複チェック + 追加
                if (!_submitted.TryAdd(m.MsgId, 0))
                {
                    Console.WriteLine("[Leader] dup submit ignored");
                    continue;
                }

                var req = JsonSerializer.Deserialize<JobRequest>(m.Payload)!;
                var exec = req.ExecName;

                var env = new JobEnvelope(req.JobId, req.ClientId, exec, m.Payload);
                Enqueue(exec, env);

                Console.WriteLine($"[Leader] Enqueued {req.JobId} exec={exec} from client={req.ClientId}");
                PumpAllExec();
            }
        }
    }

    private void Enqueue(string exec, JobEnvelope job)
    {
        var q = _execQueues.GetOrAdd(exec, _ => new ConcurrentQueue<JobEnvelope>());
        q.Enqueue(job);

        // ラウンドロビン対象に登録（重複抑止は _execInRound で）
        if (_execInRound.TryAdd(exec, 1))
            _execRound.Enqueue(exec);
    }

    // ===== Workers =====
    private async Task HandleWorkerAsync(WorkerConn wc, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var m = await Codec.ReadAsync(wc.Stream, ct);
            if (m is null) break;

            switch (m.Type)
            {
                case MsgType.Credit:
                    wc.Credit++;
                    PumpAllExec();
                    break;

                case MsgType.AckJob:
                    if (_inflight.TryRemove(m.CorrId, out var inf))
                    {
                        //wc.Credit++;
                        wc.Running--;

                        // Worker から結果が届いた場合は Client へ中継
                        if (_clients.TryGetValue(inf.Job.ClientId, out var client))
                        {
                            try
                            {
                                var resMsg = new Message
                                {
                                    Type = MsgType.Result,
                                    MsgId = inf.Job.JobId,
                                    CorrId = inf.Job.JobId,
                                    Subject = $"job.result.{inf.Job.ClientId}.{inf.Job.JobId:N}",
                                    Payload = m.Payload
                                };

                                // 中継は送信キューへ投入（接続単位で直列送信）
                                await client.EnqueueAsync(resMsg, ct);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[Leader] failed to forward result to client {inf.Job.ClientId}: {ex.Message}");
                                // 必要ならここで client 用 DLQ などへ退避可能
                            }
                        }

                        Console.WriteLine($"[Leader] Ack {m.CorrId} from {wc.WorkerId}");
                    }
                    break;
            }
        }
    }

    // ===== Scheduling =====
    private void PumpAllExec()
    {
        // ラウンドロビンで exec ごとに割り当てを試みる
        int n = _execRound.Count;
        for (int i = 0; i < n; i++)
        {
            if (!_execRound.TryDequeue(out var exec))
                break;

            TryAssignExec(exec);

            if (_execQueues.TryGetValue(exec, out var q) && !q.IsEmpty)
                _execRound.Enqueue(exec);
            else
                _execInRound.TryRemove(exec, out _);
        }
    }

    private void TryAssignExec(string exec)
    {
        if (!_execQueues.TryGetValue(exec, out var queue))
            return;

        while (!queue.IsEmpty)
        {
            WorkerConn? target;
            lock (_lock)
            {
                target = _workers.Values
                    .Where(w => w.Credit > 0 && SubjectMatcher.Match(w.SubjectPattern, $"job.assign.{exec}"))
                    .OrderBy(w => w.Running)
                    .ThenByDescending(w => w.Credit)
                    .FirstOrDefault();
            }
            if (target is null) break;

            if (queue.TryDequeue(out var job))
            {
                // 非同期送信（例外は SendAssignAsync 内で捕捉）
                _ = SendAssignAsync(target, job, attempt: 1);
            }
            else
            {
                break;
            }
        }
    }

    // Task を返し、内部で例外捕捉。送信は専用キューへ投入し、接続ごとの送信ループが直列送信する。
    private async Task SendAssignAsync(WorkerConn wc, JobEnvelope job, int attempt)
    {
        try
        {
            var msg = new Message
            {
                Type = MsgType.AssignJob,
                MsgId = job.JobId,
                Subject = $"job.assign.{job.ExecName}.{wc.WorkerId}",
                Payload = job.RawPayload
            };

            // 送信は直書きせず、キューに投入（満杯なら自然に待機→バックプレッシャー）
            await wc.EnqueueAsync(msg, CancellationToken.None);

            wc.Credit--;
            wc.Running++;

            var timeout = attempt == 1 ? InitialAckTimeout : NextTimeoutFor(attempt - 1, InitialAckTimeout);
            _inflight[job.JobId] = new Inflight(job, wc, DateTime.UtcNow + timeout, timeout, attempt);

            Console.WriteLine($"[Leader] Assigned {job.JobId} -> {wc.WorkerId} exec={job.ExecName} try={attempt}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Leader] assign failed: {ex.Message} -> requeue");
            Enqueue(job.ExecName, job);
        }
    }

    private void CheckTimeouts()
    {
        var now = DateTime.UtcNow;

        foreach (var kv in _inflight.ToArray())
        {
            var inf = kv.Value;
            if (now < inf.DueAt) continue;
            if (!_inflight.TryRemove(inf.Job.JobId, out _)) continue;

            if (inf.Attempt >= MaxAttempts)
            {
                Console.WriteLine($"[Leader] {inf.Job.JobId} exceeded attempts -> DLQ");
                _dlq.Enqueue(inf.Job);
                continue;
            }

            WorkerConn? target;
            lock (_lock)
            {
                target = _workers.Values
                    .Where(w => w.Credit > 0
                                && !ReferenceEquals(w, inf.Owner)
                                && SubjectMatcher.Match(w.SubjectPattern, $"job.assign.{inf.Job.ExecName}"))
                    .OrderBy(w => w.Running)
                    .ThenByDescending(w => w.Credit)
                    .FirstOrDefault()
                    ?? inf.Owner; // 代替が無ければ元のオーナーへ再送
            }

            _ = SendAssignAsync(target!, inf.Job, inf.Attempt + 1);
        }
    }

    private static TimeSpan NextTimeout(TimeSpan prev)
    {
        var baseMs = Math.Min(prev.TotalMilliseconds * BackoffFactor, MaxAckTimeout.TotalMilliseconds);
        var jitter = 1.0 + ((_rng.Value!.NextDouble() * 2 - 1) * JitterRate);
        return TimeSpan.FromMilliseconds(Math.Max(1, baseMs * jitter));
    }

    private static TimeSpan NextTimeoutFor(int n, TimeSpan initial)
    {
        var t = initial;
        for (int i = 0; i < n; i++) t = NextTimeout(t);
        return t;
    }

    // ==== inner types ====
    private sealed record JobEnvelope(Guid JobId, string ClientId, string ExecName, byte[] RawPayload);
    private sealed record Inflight(JobEnvelope Job, WorkerConn Owner, DateTime DueAt, TimeSpan Timeout, int Attempt);

    private sealed class ClientConn
    {
        public string ClientId { get; }
        public NetworkStream Stream { get; }
        public Channel<Message> Outbox { get; }
        public Task SendLoop { get; }
        private readonly CancellationTokenSource _cts = new();

        public ClientConn(string id, NetworkStream s)
        {
            ClientId = id;
            Stream = s;
            // バックプレッシャーを効かせるため bounded（例: 1024）
            Outbox = Channel.CreateBounded<Message>(
                new BoundedChannelOptions(1024) { FullMode = BoundedChannelFullMode.Wait });
            SendLoop = Task.Run(() => RunSendLoopAsync(_cts.Token));
        }

        private async Task RunSendLoopAsync(CancellationToken ct)
        {
            try
            {
                await foreach (var m in Outbox.Reader.ReadAllAsync(ct))
                {
                    await Codec.WriteAsync(Stream, m, ct); // 1本のループで直列送信
                }
            }
            catch (OperationCanceledException) { /* normal on shutdown */ }
            catch (Exception ex)
            {
                Console.WriteLine($"[Leader] client sender error({ClientId}): {ex.Message}");
            }
        }

        public async Task EnqueueAsync(Message m, CancellationToken ct)
            => await Outbox.Writer.WriteAsync(m, ct);

        public void Stop()
        {
            try { Outbox.Writer.TryComplete(); } catch { }
            _cts.Cancel();
        }
    }

    private sealed class WorkerConn
    {
        public Guid WorkerId { get; }
        public string SubjectPattern { get; }
        public NetworkStream Stream { get; }
        public int Credit;
        public int Running;

        public Channel<Message> Outbox { get; }
        public Task SendLoop { get; }
        private readonly CancellationTokenSource _cts = new();

        public WorkerConn(Guid id, NetworkStream s, string pattern)
        {
            WorkerId = id;
            Stream = s;
            SubjectPattern = pattern;
            Credit = 0;
            Running = 0;

            Outbox = Channel.CreateBounded<Message>(
                new BoundedChannelOptions(1024) { FullMode = BoundedChannelFullMode.Wait });
            SendLoop = Task.Run(() => RunSendLoopAsync(_cts.Token));
        }

        private async Task RunSendLoopAsync(CancellationToken ct)
        {
            try
            {
                await foreach (var m in Outbox.Reader.ReadAllAsync(ct))
                {
                    await Codec.WriteAsync(Stream, m, ct); // 1本のループで直列送信
                }
            }
            catch (OperationCanceledException) { /* normal on shutdown */ }
            catch (Exception ex)
            {
                Console.WriteLine($"[Leader] worker sender error({WorkerId}): {ex.Message}");
            }
        }

        public async Task EnqueueAsync(Message m, CancellationToken ct)
            => await Outbox.Writer.WriteAsync(m, ct);

        public void Stop()
        {
            try { Outbox.Writer.TryComplete(); } catch { }
            _cts.Cancel();
        }
    }
}
