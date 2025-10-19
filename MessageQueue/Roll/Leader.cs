using MessageQueue.Common;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace MessageQueue.Roll;

public sealed class Leader
{
    // === 基本構成 ===
    private readonly TcpListener _listener;

    // グループ別キュー: group -> queue
    private readonly ConcurrentDictionary<string, ConcurrentQueue<JobEnvelope>> _queues = new();

    // 送信済みでACK待ち
    private readonly ConcurrentDictionary<Guid, Inflight> _inflight = new();

    // Submit重複排除
    private readonly HashSet<Guid> _submitted = new();
    private readonly object _lock = new();

    // 接続中ワーカー: workerId -> conn
    private readonly Dictionary<Guid, WorkerConn> _workers = new();

    // 再送監視タイマ
    private readonly System.Timers.Timer _retransmitTimer;

    // === 再送設定（指数バックオフ + ジッタ + 最大試行） ===
    private static readonly TimeSpan InitialAckTimeout = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxAckTimeout = TimeSpan.FromSeconds(30);
    private const double BackoffFactor = 2.0;
    private const double JitterRate = 0.20; // ±20%
    private const int MaxAttempts = 6;    // 初回含む
    private static readonly ThreadLocal<Random> _rng = new(() => new Random());

    // Dead Letter Queue（メモリ）
    private readonly ConcurrentQueue<JobEnvelope> _dlq = new();

    public Leader(int port)
    {
        _listener = new TcpListener(IPAddress.Any, port);
        _retransmitTimer = new System.Timers.Timer(200);
        _retransmitTimer.Elapsed += (_, __) => CheckTimeouts();
        _retransmitTimer.AutoReset = true;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        _listener.Start();
        _retransmitTimer.Start();
        Console.WriteLine("[Leader] listening...");
        while (!ct.IsCancellationRequested)
        {
            var client = await _listener.AcceptTcpClientAsync(ct);
            _ = Task.Run(() => HandleClientAsync(client, ct));
        }
    }

    private async Task HandleClientAsync(TcpClient c, CancellationToken ct)
    {
        using var _ = c;
        using var ns = c.GetStream();

        var hello = await Codec.ReadAsync(ns, ct);
        if (hello is null) return;

        if (hello.Type == MsgType.HelloClient)
        {
            Console.WriteLine("[Leader] Client connected");
            await HandleSubmitterAsync(ns, ct);
        }
        else if (hello.Type == MsgType.HelloWorker)
        {
            // Worker は Subject に group、MsgId に workerId を入れて名乗る
            var group = string.IsNullOrEmpty(hello.Subject) ? "default" : hello.Subject;
            var workerId = hello.MsgId;

            var wc = new WorkerConn(workerId, ns, group);
            lock (_lock) _workers[workerId] = wc;

            Console.WriteLine($"[Leader] Worker {workerId} joined group '{group}'");

            try
            {
                await HandleWorkerAsync(wc, ct);
            }
            finally
            {
                // 切断
                lock (_lock) _workers.Remove(workerId);

                // そのWorkerが持っていた in-flight を再キュー
                foreach (var kv in _inflight.Where(kv => ReferenceEquals(kv.Value.Owner, wc)).ToArray())
                {
                    if (_inflight.TryRemove(kv.Key, out var inf))
                    {
                        Enqueue(inf.Job.Group, inf.Job);
                        Console.WriteLine($"[Leader] Requeued {inf.Job.JobId} (owner disconnected)");
                    }
                }
            }
        }
    }

    // ========== Client（Submitter） ==========
    private async Task HandleSubmitterAsync(NetworkStream ns, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var m = await Codec.ReadAsync(ns, ct);
            if (m is null) break;
            if (m.Type != MsgType.SubmitJob) continue;

            lock (_lock)
            {
                if (_submitted.Contains(m.MsgId))
                {
                    Console.WriteLine($"[Leader] Duplicate submit ignored: {m.MsgId}");
                    continue;
                }
                _submitted.Add(m.MsgId);
            }

            var group = string.IsNullOrEmpty(m.Subject) ? "default" : m.Subject;
            var job = new JobEnvelope(m.MsgId, m.Payload, group);
            Enqueue(group, job);
            Console.WriteLine($"[Leader] Enqueued job {m.MsgId} to group '{group}', len={m.Payload.Length}");

            TryAssign(group); // すぐ配る
        }
    }

    private void Enqueue(string group, JobEnvelope job)
    {
        var q = _queues.GetOrAdd(group, _ => new ConcurrentQueue<JobEnvelope>());
        q.Enqueue(job);
    }

    // ========== Worker ==========
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
                    TryAssign(wc.Group); // 同じグループのキューを配る
                    break;

                case MsgType.AckJob:
                    if (_inflight.TryRemove(m.CorrId, out var inf))
                    {
                        wc.Credit++;
                        wc.Running--;
                        Console.WriteLine($"[Leader] Ack {m.CorrId} from {wc.WorkerId} (group '{wc.Group}')");
                    }
                    break;
            }
        }
        Console.WriteLine($"[Leader] Worker disconnected: {wc.WorkerId} ({wc.Group})");
    }

    // ========== 割当（グループ単位） ==========
    private void TryAssign(string group)
    {
        if (!_queues.TryGetValue(group, out var queue)) return;

        while (!queue.IsEmpty)
        {
            WorkerConn? target;
            lock (_lock)
            {
                target = _workers.Values
                    .Where(w => w.Group == group && w.Credit > 0)
                    .OrderByDescending(w => w.Credit)
                    .ThenBy(w => w.Running)
                    .FirstOrDefault();
            }
            if (target is null) break;

            if (queue.TryDequeue(out var job))
            {
                SendAssign(target, job, attempt: 1);
            }
            else break;
        }
    }

    private async void SendAssign(WorkerConn wc, JobEnvelope job, int attempt)
    {
        try
        {
            var msg = new Message
            {
                Type = MsgType.AssignJob,
                MsgId = job.JobId,
                CorrId = Guid.Empty,
                Subject = $"job.assign.{wc.Group}.{wc.WorkerId}",
                Payload = job.Payload
            };
            await Codec.WriteAsync(wc.Stream, msg, CancellationToken.None);

            wc.Credit--;
            wc.Running++;

            var timeout = attempt == 1 ? InitialAckTimeout : NextTimeoutFor(attempt - 1, InitialAckTimeout);
            var due = DateTime.UtcNow + timeout;

            _inflight[job.JobId] = new Inflight(job, wc, due, timeout, attempt);
            Console.WriteLine($"[Leader] Assigned {job.JobId} -> {wc.WorkerId} (group '{wc.Group}', try={attempt}, timeout~{timeout.TotalMilliseconds:N0}ms)");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Leader] Assign failed: {ex.Message}. Requeue {job.JobId}");
            Enqueue(job.Group, job);
        }
    }

    // ========== 再送監視 ==========
    private void CheckTimeouts()
    {
        var now = DateTime.UtcNow;

        foreach (var kv in _inflight.ToArray())
        {
            var inf = kv.Value;
            if (now < inf.DueAt) continue; // まだACK待ち

            if (!_inflight.TryRemove(inf.Job.JobId, out _)) continue;

            if (inf.Attempt >= MaxAttempts)
            {
                Console.WriteLine($"[Leader] Job {inf.Job.JobId} exceeded {MaxAttempts} attempts → DLQ (group '{inf.Job.Group}')");
                _dlq.Enqueue(inf.Job);
                continue;
            }

            WorkerConn? target;
            lock (_lock)
            {
                target = _workers.Values
                    .Where(w => w.Group == inf.Job.Group && w.Credit > 0 && !ReferenceEquals(w, inf.Owner))
                    .OrderByDescending(w => w.Credit).ThenBy(w => w.Running)
                    .FirstOrDefault() ?? inf.Owner;
            }

            var nextAttempt = inf.Attempt + 1;
            Console.WriteLine($"[Leader] Retransmit {inf.Job.JobId} -> {target!.WorkerId} (group '{target.Group}', try={nextAttempt})");
            SendAssign(target, inf.Job, nextAttempt); // SendAssign 内で新しい due/timeout 設定
        }
    }

    private static TimeSpan NextTimeout(TimeSpan prev)
    {
        var baseMs = Math.Min(prev.TotalMilliseconds * BackoffFactor, MaxAckTimeout.TotalMilliseconds);
        var jitter = 1.0 + ((_rng.Value!.NextDouble() * 2.0 - 1.0) * JitterRate); // [0.8,1.2]
        return TimeSpan.FromMilliseconds(Math.Max(1, baseMs * jitter));
    }

    private static TimeSpan NextTimeoutFor(int attemptsAlready, TimeSpan initial)
    {
        var t = initial;
        for (int i = 0; i < attemptsAlready; i++) t = NextTimeout(t);
        return t;
    }

    // ========== DLQ 操作（オプション） ==========
    public void DumpDlq()
    {
        Console.WriteLine("==== Dead Letter Queue ====");
        foreach (var j in _dlq)
            Console.WriteLine($"Job {j.JobId} (group '{j.Group}'), size={j.Payload.Length} bytes");
        Console.WriteLine("===========================");
    }

    public void RequeueDlq()
    {
        int count = 0;
        while (_dlq.TryDequeue(out var job))
        {
            Enqueue(job.Group, job);
            count++;
        }
        Console.WriteLine($"[Leader] Requeued {count} jobs from DLQ.");
    }

    // ========== 内部型 ==========
    private sealed record JobEnvelope(Guid JobId, byte[] Payload, string Group);
    private sealed record Inflight(JobEnvelope Job, WorkerConn Owner, DateTime DueAt, TimeSpan Timeout, int Attempt);

    private sealed class WorkerConn
    {
        public Guid WorkerId { get; }
        public string Group { get; }
        public NetworkStream Stream { get; }
        public int Credit;
        public int Running;

        public WorkerConn(Guid workerId, NetworkStream stream, string group)
        {
            WorkerId = workerId;
            Stream = stream;
            Group = group;
            Credit = 0;
            Running = 0;
        }
    }
}

