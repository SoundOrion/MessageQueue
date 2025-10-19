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
                MsgId = job.JobId,        // = JobId
                CorrId = Guid.Empty,
                Payload = job.Payload
            };
            await Codec.WriteAsync(wc.Stream, msg, CancellationToken.None);

            wc.Credit--;
            wc.Running++;

            _inflight[job.JobId] = new Inflight(job, wc, SentAt: DateTime.UtcNow, Attempt: attempt);
            Console.WriteLine($"[Leader] Assigned {job.JobId} to {wc.WorkerId} (try={attempt})");
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
            if (now - inf.SentAt < TimeSpan.FromSeconds(5)) continue; // ACK待ち

            if (_inflight.TryRemove(inf.Job.JobId, out _))
            {
                // 別Workerを優先
                WorkerConn? target;
                lock (_lock)
                {
                    target = _workers.Values
                        .Where(w => w.Credit > 0 && !ReferenceEquals(w, inf.Owner))
                        .OrderByDescending(w => w.Credit).ThenBy(w => w.Running)
                        .FirstOrDefault() ?? inf.Owner;
                }
                Console.WriteLine($"[Leader] Retransmit {inf.Job.JobId} (next try={inf.Attempt + 1})");
                SendAssign(target!, inf.Job, inf.Attempt + 1);
            }
        }
    }

    private sealed record JobEnvelope(Guid JobId, byte[] Payload);
    private sealed record Inflight(JobEnvelope Job, WorkerConn Owner, DateTime SentAt, int Attempt);

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
