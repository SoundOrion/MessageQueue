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
    private readonly HashSet<Guid> _submitted = new(); // lockで保護
    private readonly object _lock = new();

    private readonly List<WorkerConn> _workers = new(); // 最小：単純管理
    private readonly System.Timers.Timer _retransmitTimer;

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
        Console.WriteLine("Leader listening...");
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
            await HandleSubmitterAsync(ns, ct);
        }
        else if (hello.Type == MsgType.HelloWorker)
        {
            var wc = new WorkerConn(ns);
            lock (_lock) _workers.Add(wc);
            try { await HandleWorkerAsync(wc, ct); }
            finally { lock (_lock) _workers.Remove(wc); }
        }
    }

    // Client -> SubmitJob（重複排除）
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
                    Console.WriteLine($"Duplicate submit ignored: {m.MsgId}");
                    continue;
                }
                _submitted.Add(m.MsgId);
            }
            _queue.Enqueue(new JobEnvelope(m.MsgId, m.Payload));
            TryAssign(); // すぐ割当を試みる
            Console.WriteLine($"Enqueued job {m.MsgId} len={m.Payload.Length}");
        }
    }

    // Worker -> Credit / Ack
    private async Task HandleWorkerAsync(WorkerConn wc, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var m = await Codec.ReadAsync(wc.Stream, ct);
            if (m is null) break;

            if (m.Type == MsgType.Credit)
            {
                wc.Credit++;
                TryAssign();
            }
            else if (m.Type == MsgType.AckJob)
            {
                // corrId = JobId
                if (_inflight.TryRemove(m.CorrId, out var inflight))
                {
                    Console.WriteLine($"Ack {m.CorrId} from worker");
                    inflight.Owner.Credit++;          // 枠回復（割り当て中に減らしている場合）
                    inflight.Owner.Running--;         // 実行数減少
                }
            }
        }
    }

    // 割当（最小：先頭ジョブを、最初に空いてるワーカーへ）
    private void TryAssign()
    {
        if (_queue.IsEmpty) return;
        WorkerConn? target = null;
        lock (_lock)
        {
            target = _workers.FirstOrDefault(w => w.Credit > 0);
        }
        if (target is null) return;

        if (_queue.TryDequeue(out var job))
        {
            SendAssign(target, job, attempt: 1);
        }
    }

    private async void SendAssign(WorkerConn wc, JobEnvelope job, int attempt)
    {
        try
        {
            var now = DateTime.UtcNow;
            var msg = new Message
            {
                Type = MsgType.AssignJob,
                MsgId = job.JobId,        // = JobId
                CorrId = Guid.Empty,
                Payload = job.Payload
            };
            await Codec.WriteAsync(wc.Stream, msg, CancellationToken.None);
            wc.Credit--;
            wc.Running++;

            _inflight[job.JobId] = new Inflight(job, wc, SentAt: now, Attempt: attempt);
            Console.WriteLine($"Assigned {job.JobId} (try={attempt})");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Assign failed: {ex.Message}. Requeue {job.JobId}");
            _queue.Enqueue(job);
        }
    }

    // タイムアウト監視 → 再送or再キュー
    private void CheckTimeouts()
    {
        var now = DateTime.UtcNow;
        foreach (var kv in _inflight.ToArray())
        {
            var inf = kv.Value;
            var elapsed = now - inf.SentAt;
            var timeout = TimeSpan.FromSeconds(5); // 仮のACK待ち時間
            if (elapsed < timeout) continue;

            if (_inflight.TryRemove(inf.Job.JobId, out _))
            {
                if (inf.Attempt >= 5)
                {
                    Console.WriteLine($"Job {inf.Job.JobId} exceeded retries. Requeue.");
                    _queue.Enqueue(inf.Job);
                }
                else
                {
                    // 同じワーカーでもよいが、空いている別ワーカーがいればそちらへ
                    WorkerConn? target;
                    lock (_lock)
                    {
                        target = _workers
                            .OrderByDescending(w => w.Credit) // 簡易
                            .FirstOrDefault(w => w.Credit > 0 && !object.ReferenceEquals(w, inf.Owner))
                            ?? inf.Owner; // fallback
                    }
                    SendAssign(target!, inf.Job, inf.Attempt + 1);
                }
            }
        }
    }

    private sealed record JobEnvelope(Guid JobId, byte[] Payload);
    private sealed record Inflight(JobEnvelope Job, WorkerConn Owner, DateTime SentAt, int Attempt);
    private sealed class WorkerConn
    {
        public NetworkStream Stream { get; }
        public int Credit;
        public int Running;
        public WorkerConn(NetworkStream s) { Stream = s; Credit = 0; Running = 0; }
    }
}
