using MessageQueue.Common;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace MessageQueue.Roll;

public sealed class Leader
{
    private readonly TcpListener _listener;
    private readonly ConcurrentQueue<JobEnvelope> _queue = new();
    private readonly ConcurrentDictionary<Guid, Inflight> _inflight = new();
    private readonly HashSet<Guid> _submitted = new();         // Submit重複排除
    private readonly Dictionary<Guid, WorkerConn> _workers = new();
    private readonly object _lock = new();

    private readonly System.Timers.Timer _retransmitTimer;

    // ACK タイムアウト → 再送に「指数バックオフ + 最大試行回数」
    private static readonly TimeSpan InitialAckTimeout = TimeSpan.FromSeconds(1);  // 初回待ち
    private static readonly TimeSpan MaxAckTimeout = TimeSpan.FromSeconds(30); // 上限
    private const double BackoffFactor = 2.0;                      // 指数倍
    private const double JitterRate = 0.20;                     // ±20%のジッタ
    private const int MaxAttempts = 6;                        // 送信総回数(初回含む)

    private static readonly ThreadLocal<Random> _rng = new(() => new Random());


    public Leader(int port)
    {
        _listener = new TcpListener(IPAddress.Any, port);
        _retransmitTimer = new System.Timers.Timer(200); // 200ms周期で監視
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
            var workerId = hello.MsgId;                 // Worker が名乗る GUID
            var wc = new WorkerConn(workerId, ns);
            lock (_lock) _workers[workerId] = wc;

            Console.WriteLine($"[Leader] Worker connected: {workerId}");

            try
            {
                await HandleWorkerAsync(wc, ct);
            }
            finally
            {
                // 切断
                lock (_lock) _workers.Remove(workerId);

                // そのWorkerが持っていたin-flightを再キュー
                foreach (var kv in _inflight.Where(kv => ReferenceEquals(kv.Value.Owner, wc)).ToArray())
                {
                    if (_inflight.TryRemove(kv.Key, out var inf))
                    {
                        _queue.Enqueue(inf.Job);
                        Console.WriteLine($"[Leader] Requeued {inf.Job.JobId} (owner disconnected)");
                    }
                }
            }
        }
    }

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

            _queue.Enqueue(new JobEnvelope(m.MsgId, m.Payload));
            Console.WriteLine($"[Leader] Enqueued job {m.MsgId} len={m.Payload.Length}");

            TryAssign(); // すぐ配る
        }
    }

    private async Task HandleWorkerAsync(WorkerConn wc, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var m = await Codec.ReadAsync(wc.Stream, ct);
            if (m is null) break;

            switch (m.Type)
            {
                case MsgType.Credit:
                    wc.Credit++;           // 細かく見るなら m.Payload の Int32 を加算
                    TryAssign();
                    break;

                case MsgType.AckJob:
                    if (_inflight.TryRemove(m.CorrId, out var inf))
                    {
                        wc.Credit++;
                        wc.Running--;
                        Console.WriteLine($"[Leader] Ack {m.CorrId} from {wc.WorkerId}");
                    }
                    break;
            }
        }
        Console.WriteLine($"[Leader] Worker disconnected: {wc.WorkerId}");
    }

    private void TryAssign()
    {
        while (!_queue.IsEmpty)
        {
            WorkerConn? target;
            lock (_lock)
            {
                target = _workers.Values
                    .Where(w => w.Credit > 0)
                    .OrderByDescending(w => w.Credit)
                    .ThenBy(w => w.Running)
                    .FirstOrDefault();
            }
            if (target is null) break;

            if (_queue.TryDequeue(out var job))
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
                Payload = job.Payload
            };
            await Codec.WriteAsync(wc.Stream, msg, CancellationToken.None);

            wc.Credit--;
            wc.Running++;

            var timeout = (attempt == 1) ? InitialAckTimeout : NextTimeoutFor(attempt - 1, InitialAckTimeout); // 再送時は前回から計算でもOK
            var due = DateTime.UtcNow + timeout;

            _inflight[job.JobId] = new Inflight(job, wc, due, timeout, attempt);
            Console.WriteLine($"[Leader] Assigned {job.JobId} to {wc.WorkerId} (try={attempt}, timeout={timeout.TotalMilliseconds:N0}ms)");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Leader] Assign failed: {ex.Message}. Requeue {job.JobId}");
            _queue.Enqueue(job);
        }
    }


    private void CheckTimeouts()
    {
        var now = DateTime.UtcNow;

        foreach (var kv in _inflight.ToArray())
        {
            var inf = kv.Value;
            if (now < inf.DueAt) continue; // まだ待つ

            // 期限到達 → 取り除く
            if (!_inflight.TryRemove(inf.Job.JobId, out _)) continue;

            // 最大試行に達したら DLQ または再キュー
            if (inf.Attempt >= MaxAttempts)
            {
                Console.WriteLine($"[Leader] Job {inf.Job.JobId} exceeded max attempts ({MaxAttempts}). Requeue/DLQ.");
                _queue.Enqueue(inf.Job);            // ここを DLQ に変えることも可能
                continue;
            }

            // 次の送信先（別Workerを優先）
            WorkerConn? target;
            lock (_lock)
            {
                target = _workers.Values
                    .Where(w => w.Credit > 0 && !ReferenceEquals(w, inf.Owner))
                    .OrderByDescending(w => w.Credit).ThenBy(w => w.Running)
                    .FirstOrDefault() ?? inf.Owner;
            }

            // 次のタイムアウト値（指数バックオフ + ジッタ）
            var nextTimeout = NextTimeout(inf.Timeout);
            var nextAttempt = inf.Attempt + 1;

            Console.WriteLine($"[Leader] Retransmit {inf.Job.JobId} -> {target!.WorkerId} (try={nextAttempt}, timeout={nextTimeout.TotalMilliseconds:N0}ms)");
            // 再送
            SendAssign(target, inf.Job, nextAttempt);
            // SendAssign 内で新しい timeout/due を設定します
        }
    }

    // 直前のTimeoutから次を計算（指数&ジッタ&上限）
    private TimeSpan NextTimeout(TimeSpan prev)
    {
        var baseMs = Math.Min(prev.TotalMilliseconds * BackoffFactor, MaxAckTimeout.TotalMilliseconds);
        var jitter = 1.0 + ((_rng.Value!.NextDouble() * 2.0 - 1.0) * JitterRate); // [0.8, 1.2]
        return TimeSpan.FromMilliseconds(Math.Max(1, baseMs * jitter));
    }

    // 初回から N 回目までを計算したい場合に使うヘルパ
    private TimeSpan NextTimeoutFor(int attemptsAlready, TimeSpan initial)
    {
        var t = initial;
        for (int i = 0; i < attemptsAlready; i++) t = NextTimeout(t);
        return t;
    }


    private sealed record JobEnvelope(Guid JobId, byte[] Payload);

    // 旧:
    // private sealed record Inflight(JobEnvelope Job, WorkerConn Owner, DateTime SentAt, int Attempt);

    // 新:
    private sealed record Inflight(
        JobEnvelope Job,
        WorkerConn Owner,
        DateTime DueAt,     // 次にタイムアウト判定する期限
        TimeSpan Timeout,   // 現在の待ち時間
        int Attempt    // 送信試行回数（1=初回）
    );

    private sealed class WorkerConn
    {
        public Guid WorkerId { get; }
        public NetworkStream Stream { get; }
        public int Credit;
        public int Running;

        public WorkerConn(Guid workerId, NetworkStream stream)
        {
            WorkerId = workerId;
            Stream = stream;
            Credit = 0;
            Running = 0;
        }
    }
}
