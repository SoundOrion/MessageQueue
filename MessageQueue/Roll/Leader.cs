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

    // ==== 永続化 ====
    private readonly Persistence _store;                 // WAL + Snapshot
    private DateTime _lastSnapAt = DateTime.UtcNow;      // スナップ最終時刻

    // ClientId -> connection
    private readonly ConcurrentDictionary<string, ClientConn> _clients = new();

    // WorkerId -> connection
    private readonly Dictionary<Guid, WorkerConn> _workers = new();
    private readonly object _lock = new();

    // ExecName -> queue
    private readonly ConcurrentDictionary<string, ConcurrentQueue<JobEnvelope>> _execQueues = new();
    private readonly ConcurrentQueue<string> _execRound = new();
    private readonly ConcurrentDictionary<string, byte> _execInRound = new();

    // Submit 重複排除
    private readonly ConcurrentDictionary<Guid, byte> _submitted = new();

    // In-flight
    private readonly ConcurrentDictionary<Guid, Inflight> _inflight = new();

    // ★ 追加: Clientごとの希望上限と現在のin-flight数
    private readonly ConcurrentDictionary<string, int> _clientCap = new();
    private readonly ConcurrentDictionary<string, int> _clientInflight = new();

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
        _store = new Persistence("state"); // 追加：永続化ストア初期化

        // 定期チェック（タイムアウト監視 + スケジューリング）
        _pumpTimer = new System.Timers.Timer(200);
        _pumpTimer.AutoReset = true;
        _pumpTimer.Elapsed += (_, __) =>
        {
            try
            {
                CheckTimeouts();
                PumpAllExec();

                // 追加：軽量スナップショット（1分ごと）
                if ((DateTime.UtcNow - _lastSnapAt) > TimeSpan.FromMinutes(1))
                {
                    _lastSnapAt = DateTime.UtcNow;
                    var snap = BuildSnapshot();
                    _ = _store.SnapshotAsync(snap);
                }
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

        // 起動時復旧はバックグラウンドで実行（待たない）
        _ = Task.Run(() => RecoverAsync(ct), ct);

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

            // ★ 希望並列数の取得（payloadがあれば）
            int cap = 4; // 既定
            try
            {
                if (hello.Payload is { Length: > 0 })
                {
                    var cfg = JsonSerializer.Deserialize<ClientConfig>(hello.Payload);
                    if (cfg != null && !string.IsNullOrWhiteSpace(cfg.ClientId) && cfg.ClientId == clientId)
                        cap = Math.Max(1, cfg.DesiredParallelism);
                }
            }
            catch { /* 無効なpayloadは既定にフォールバック */ }
            _clientCap[clientId] = cap;
            _clientInflight[clientId] = 0;

            var cc = new ClientConn(clientId, ns);
            _clients[clientId] = cc;
            Console.WriteLine($"[Leader] Client connected: {clientId} cap={cap}");

            try
            {
                await HandleClientAsync(cc, ct);
            }
            finally
            {
                cc.Stop(); // 送信ループ停止
                _clients.TryRemove(clientId, out _);
                _clientCap.TryRemove(clientId, out _);
                _clientInflight.TryRemove(clientId, out _);
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
                        // ★ Client inflight をデクリメントして再チャレンジできるように
                        _clientInflight.AddOrUpdate(inf.Job.ClientId, 0, (_, v) => Math.Max(0, v - 1));
                        Console.WriteLine($"[Leader] Requeued {inf.Job.JobId} (owner down)");
                        // WAL: worker_down_requeue（ベストエフォート）
                        _ = _store.AppendAsync(new WalWorkerDownRequeue("worker_down_requeue", DateTime.UtcNow, inf.Job.JobId, wc.WorkerId), durable: false);
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
                // WAL: enqueue（重要→durable）
                await _store.AppendAsync(new WalEnqueue("enqueue", DateTime.UtcNow,
                    new JobWire(env.JobId, env.ClientId, env.ExecName, env.RawPayload)), durable: true, ct);

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
                    {
                        // --- 強化版 Credit 反映 ---
                        int delta = 1;
                        if (m.Payload is { Length: 4 })
                        {
                            delta = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(m.Payload);
                        }
                        else if (m.Payload is { Length: > 0 })
                        {
                            Console.WriteLine($"[Leader] invalid credit payload len={m.Payload.Length} from {wc.WorkerId}");
                            break;
                        }

                        const int MaxCreditPerMessage = 10_000;
                        if (delta <= 0) break;
                        if (delta > MaxCreditPerMessage) delta = MaxCreditPerMessage;

                        int after = System.Threading.Interlocked.Add(ref wc.Credit, delta);
                        if (after < 0)
                        {
                            System.Threading.Interlocked.Exchange(ref wc.Credit, 0);
                            Console.WriteLine($"[Leader] credit underflow fixed for {wc.WorkerId}");
                        }

                        PumpAllExec();
                    }
                    break;

                case MsgType.AckJob:
                    if (_inflight.TryRemove(m.CorrId, out var inf))
                    {
                        wc.Running--;

                        // Client側へ結果中継
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
                                await client.EnqueueAsync(resMsg, ct);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[Leader] failed to forward result to client {inf.Job.ClientId}: {ex.Message}");
                            }
                        }

                        // ★ Clientごとのin-flightをデクリメント
                        _clientInflight.AddOrUpdate(inf.Job.ClientId, 0, (_, v) => Math.Max(0, v - 1));

                        Console.WriteLine($"[Leader] Ack {m.CorrId} from {wc.WorkerId}");
                        // WAL: ack（重要→durable）
                        await _store.AppendAsync(new WalAck("ack", DateTime.UtcNow, inf.Job.JobId, inf.Job.ClientId), durable: true, ct);

                        PumpAllExec(); // 次の割り当てを早める
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

        int spinGuard = 0; // 無限ループ防止
        while (!queue.IsEmpty && spinGuard++ < 1000)
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

            if (!queue.TryDequeue(out var job)) break;

            // ★ Client別の同時実行上限をチェック
            var cap = _clientCap.GetValueOrDefault(job.ClientId, 4);
            var cur = _clientInflight.GetValueOrDefault(job.ClientId, 0);
            if (cur >= cap)
            {
                // 上限超過：末尾へ戻す & 他のジョブを当てる
                queue.Enqueue(job);
                break;
            }

            // 非同期送信（例外は SendAssignAsync 内で捕捉）
            _ = SendAssignAsync(target, job, attempt: 1);
        }
    }

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

            await wc.EnqueueAsync(msg, CancellationToken.None);

            wc.Credit--;
            wc.Running++;

            // ★ Client in-flight をインクリメント
            _clientInflight.AddOrUpdate(job.ClientId, 1, (_, v) => v + 1);

            var timeout = attempt == 1 ? InitialAckTimeout : NextTimeoutFor(attempt - 1, InitialAckTimeout);
            _inflight[job.JobId] = new Inflight(job, wc, DateTime.UtcNow + timeout, timeout, attempt);

            Console.WriteLine($"[Leader] Assigned {job.JobId} -> {wc.WorkerId} exec={job.ExecName} try={attempt}");

            // WAL: assign（観測可能副作用後にFlushでもOK）
            await _store.AppendAsync(new WalAssign("assign", DateTime.UtcNow, job.JobId, wc.WorkerId, attempt), durable: true);
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
                // ★ in-flight戻し
                _clientInflight.AddOrUpdate(inf.Job.ClientId, 0, (_, v) => Math.Max(0, v - 1));
                // WAL: dlq（重要→durable）
                _ = _store.AppendAsync(new WalDlq("dlq", DateTime.UtcNow, inf.Job.JobId), durable: true);
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
            // WAL: timeout_requeue（頻発可→非durableでOK）
            _ = _store.AppendAsync(new WalTimeoutRequeue("timeout_requeue", DateTime.UtcNow, inf.Job.JobId, inf.Attempt), durable: false);
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

    // ====== Snapshotビルド ======
    private LeaderStateSnapshot BuildSnapshot()
    {
        var snap = new LeaderStateSnapshot();

        foreach (var kv in _execQueues)
        {
            var list = new List<JobWire>();
            foreach (var job in kv.Value.ToArray())
                list.Add(new JobWire(job.JobId, job.ClientId, job.ExecName, job.RawPayload));
            snap.Queues[kv.Key] = list;
        }

        foreach (var kv in _inflight)
        {
            var inf = kv.Value;
            snap.Inflight[kv.Key] = new InflightWire(
                new JobWire(inf.Job.JobId, inf.Job.ClientId, inf.Job.ExecName, inf.Job.RawPayload),
                OwnerWorker: inf.Owner?.WorkerId,
                DueAt: inf.DueAt, Timeout: inf.Timeout, Attempt: inf.Attempt
            );
        }

        snap.Dlq.AddRange(_dlq.ToArray().Select(j => new JobWire(j.JobId, j.ClientId, j.ExecName, j.RawPayload)));
        foreach (var kv in _clientCap) snap.ClientCap[kv.Key] = kv.Value;
        foreach (var kv in _clientInflight) snap.ClientInflight[kv.Key] = kv.Value;
        return snap;
    }

    // ====== 起動時復旧 ======
    private async Task RecoverAsync(CancellationToken ct)
    {
        try
        {
            var (snap, lines) = await _store.LoadAsync(ct);

            // 1) スナップショット適用
            if (snap is not null)
            {
                foreach (var kv in snap.Queues)
                {
                    var q = _execQueues.GetOrAdd(kv.Key, _ => new ConcurrentQueue<JobEnvelope>());
                    foreach (var w in kv.Value)
                        q.Enqueue(new JobEnvelope(w.JobId, w.ClientId, w.ExecName, w.RawPayload));
                    if (!q.IsEmpty && _execInRound.TryAdd(kv.Key, 1))
                        _execRound.Enqueue(kv.Key);
                }
                foreach (var kv in snap.Inflight)
                {
                    var w = kv.Value;
                    _inflight[kv.Key] = new Inflight(
                        new JobEnvelope(w.Job.JobId, w.Job.ClientId, w.Job.ExecName, w.Job.RawPayload),
                        Owner: null, // Workerは再接続まで null 扱い
                        DueAt: w.DueAt, Timeout: w.Timeout, Attempt: w.Attempt
                    );
                }
                foreach (var w in snap.Dlq)
                    _dlq.Enqueue(new JobEnvelope(w.JobId, w.ClientId, w.ExecName, w.RawPayload));

                foreach (var kv in snap.ClientCap) _clientCap[kv.Key] = kv.Value;
                foreach (var kv in snap.ClientInflight) _clientInflight[kv.Key] = kv.Value;
            }

            // 2) WAL リプレイ（最新スナップ後の差分）
            foreach (var line in lines)
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                using var doc = JsonDocument.Parse(line);
                if (!doc.RootElement.TryGetProperty("type", out var tProp)) continue;
                var type = tProp.GetString();

                switch (type)
                {
                    case "enqueue":
                        var enq = JsonSerializer.Deserialize<WalEnqueue>(line)!;
                        var job = enq.job;
                        Enqueue(job.ExecName, new JobEnvelope(job.JobId, job.ClientId, job.ExecName, job.RawPayload));
                        break;

                    case "assign":
                        // in-flight に反映（ownerは再接続待ち）
                        var asg = JsonSerializer.Deserialize<WalAssign>(line)!;
                        if (_inflight.TryGetValue(asg.jobId, out var inf))
                        {
                            _inflight[asg.jobId] = inf; // 必要ならAttempt調整
                        }
                        break;

                    case "ack":
                        var ack = JsonSerializer.Deserialize<WalAck>(line)!;
                        _inflight.TryRemove(ack.jobId, out _);
                        _clientInflight.AddOrUpdate(ack.clientId, 0, (_, v) => Math.Max(0, v - 1));
                        break;

                    case "timeout_requeue":
                    case "worker_down_requeue":
                        var r = type == "timeout_requeue"
                            ? JsonSerializer.Deserialize<WalTimeoutRequeue>(line)!.jobId
                            : JsonSerializer.Deserialize<WalWorkerDownRequeue>(line)!.jobId;
                        if (_inflight.TryRemove(r, out var inf2))
                        {
                            Enqueue(inf2.Job.ExecName, inf2.Job);
                            _clientInflight.AddOrUpdate(inf2.Job.ClientId, 0, (_, v) => Math.Max(0, v - 1));
                        }
                        break;

                    case "dlq":
                        var d = JsonSerializer.Deserialize<WalDlq>(line)!.jobId;
                        if (_inflight.TryRemove(d, out var inf3))
                            _dlq.Enqueue(inf3.Job);
                        break;
                }
            }

            Console.WriteLine("[Leader] recovery completed.");
            PumpAllExec();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Leader] recovery failed: {ex.Message}");
        }
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
                    await Codec.WriteAsync(Stream, m, ct);
                }
            }
            catch (OperationCanceledException) { }
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
                    await Codec.WriteAsync(Stream, m, ct);
                }
            }
            catch (OperationCanceledException) { }
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
