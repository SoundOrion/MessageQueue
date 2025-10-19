using MessageQueue.Common;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace MessageQueue.Roll;

public sealed class ClientMulti
{
    private readonly string _clientId;
    private readonly (string host, int port)[] _endpoints;
    private readonly int _desiredTotal;

    // 送信ポリシー：RR or InflightMin
    public enum SendPolicy { RoundRobin, InflightMin }
    private readonly SendPolicy _policy = SendPolicy.RoundRobin;

    // ===== フェイルオーバー関連パラメータ（おすすめ値） =====
    private readonly TimeSpan _failoverGrace = TimeSpan.FromSeconds(10);      // 接続断からの猶予
    private readonly TimeSpan _pendingPumpInterval = TimeSpan.FromMilliseconds(50);

    // in-flight（送信済み・未完了）管理: JobId -> (接続, 送信時刻, メッセージ)
    private readonly ConcurrentDictionary<Guid, (LeaderConn conn, DateTime sentAt, Message msg)> _inflight = new();

    // 未送信ペンディング（接続ダウン等で送り先がなかったもの）
    private readonly ConcurrentQueue<Message> _pending = new();

    public ClientMulti(string clientId, (string host, int port)[] endpoints, int desiredTotalParallelism)
    {
        _clientId = clientId;
        _endpoints = endpoints;
        _desiredTotal = Math.Max(1, desiredTotalParallelism);
    }

    public async Task RunAsync(CancellationToken ct)
    {
        // ===== 接続確立 & Hello（cap は均等割り） =====
        var conns = new LeaderConn[_endpoints.Length];
        int baseCap = Math.Max(1, _desiredTotal / _endpoints.Length);
        int remainder = _desiredTotal - baseCap * _endpoints.Length;

        for (int i = 0; i < _endpoints.Length; i++)
        {
            int cap = baseCap + (i < remainder ? 1 : 0);
            conns[i] = new LeaderConn(_clientId, _endpoints[i].host, _endpoints[i].port, cap);
            await conns[i].ConnectAndHelloAsync(ct);
        }

        Console.WriteLine($"[ClientX {_clientId}] connected to {conns.Length} leaders, totalCap={_desiredTotal}");

        // ===== Pending 再送ポンプ & フェイルオーバー回収タスク =====
        _ = Task.Run(() => PendingPumpAsync(conns, ct), ct);
        _ = Task.Run(() => FailoverRequeueLoopAsync(conns, ct), ct);

        // ====== デモ：複数ジョブ投入（既存 Client 相当で3件） ======
        int totalJobs = 3;
        for (int i = 0; i < totalJobs; i++)
        {
            var job = new JobRequest(
                JobId: Guid.NewGuid(),
                ClientId: _clientId,
                ExecName: (i % 2 == 0) ? "calcA" : "calcB",
                Args: new() { "--mode=fast", $"--seed={i}" },
                Files: new() { new InputFile("input.txt", null, Encoding.UTF8.GetBytes($"hello-{i}")) }
            );

            var msg = new Message
            {
                Type = MsgType.SubmitJob,
                MsgId = job.JobId,
                Subject = $"job.submit.{job.ExecName}",
                Payload = JsonSerializer.SerializeToUtf8Bytes(job)
            };

            await SendWithFailoverAsync(conns, msg, ct);
            Console.WriteLine($"[ClientX {_clientId}] submitted {job.JobId} -> {job.ExecName}");
        }

        // ====== 結果待ち（全接続の結果を集約） ======
        int received = 0;
        while (received < totalJobs && !ct.IsCancellationRequested)
        {
            foreach (var c in conns)
            {
                var res = await c.TryReadResultAsync(ct);
                if (res is null) continue;

                try
                {
                    var jr = JsonSerializer.Deserialize<JobResult>(res.Payload)!;
                    Console.WriteLine($"[ClientX {_clientId}] RESULT {jr.JobId} status={jr.Status} stdout={jr.Stdout.Trim()} stderr={jr.Stderr.Trim()} from {c.Name}");
                    _inflight.TryRemove(jr.JobId, out _);
                    c.InflightClientSide = Math.Max(0, c.InflightClientSide - 1);
                    received++;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ClientX {_clientId}] bad result payload: {ex.Message}");
                }
            }

            await Task.Delay(10, ct);
        }

        Console.WriteLine($"[ClientX {_clientId}] done: {received}/{totalJobs} results received");
    }

    // === 未送信安全振替つき送信 ===
    private async Task SendWithFailoverAsync(LeaderConn[] conns, Message msg, CancellationToken ct)
    {
        // 生存接続をポリシーで選択
        var target = SelectAliveConn(conns) ?? SelectAnyConn(conns); // どれもAliveでなければ暫定選択（失敗してpendingへ）
        try
        {
            await target.SendNowAsync(msg, ct);               // 直列送信（実ソケットへ即書き込み）
            target.InflightClientSide++;                      // クライアント視点のin-flight
            _inflight[msg.MsgId] = (target, DateTime.UtcNow, msg);
        }
        catch
        {
            // 送信直前で失敗：未送信扱いで pending へ
            _pending.Enqueue(msg);
        }
    }

    private LeaderConn? SelectAliveConn(LeaderConn[] conns)
    {
        var alive = conns.Where(c => c.IsAlive).ToArray();
        if (alive.Length == 0) return null;

        return _policy switch
        {
            SendPolicy.InflightMin => alive.OrderBy(c => c.InflightClientSide).First(),
            _ => RrUtil.RoundRobin(alive)
        };
    }

    private LeaderConn SelectAnyConn(LeaderConn[] conns)
    {
        // すべて死んでいる場合の暫定。RRで 0 本ではない前提
        return RrUtil.RoundRobin(conns);
    }

    private async Task PendingPumpAsync(LeaderConn[] conns, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            if (_pending.TryDequeue(out var msg))
            {
                var target = SelectAliveConn(conns);
                if (target is null)
                {
                    // まだ死んでいる→戻す
                    _pending.Enqueue(msg);
                    await Task.Delay(_pendingPumpInterval, ct);
                    continue;
                }

                try
                {
                    await target.SendNowAsync(msg, ct);
                    target.InflightClientSide++;
                    _inflight[msg.MsgId] = (target, DateTime.UtcNow, msg);
                }
                catch
                {
                    // 送信直前失敗→再び pending
                    _pending.Enqueue(msg);
                    await Task.Delay(_pendingPumpInterval, ct);
                }
            }
            else
            {
                await Task.Delay(_pendingPumpInterval, ct);
            }
        }
    }

    private async Task FailoverRequeueLoopAsync(LeaderConn[] conns, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            foreach (var kv in _inflight.ToArray())
            {
                var jobId = kv.Key;
                var (conn, sentAt, msg) = kv.Value;

                if (!conn.IsAlive && (DateTime.UtcNow - conn.DisconnectedAt) >= _failoverGrace)
                {
                    // フェイルオーバー：この in-flight を回収 → pending へ戻す
                    if (_inflight.TryRemove(jobId, out _))
                    {
                        _pending.Enqueue(msg);
                        Console.WriteLine($"[ClientX {_clientId}] failover requeue {jobId} (from {conn.Name})");
                    }
                }
            }
            await Task.Delay(100, ct);
        }
    }

    // ==== 内部：Leader接続ごとの送受信 & 再接続 ====
    private sealed class LeaderConn
    {
        private readonly string _clientId;
        private readonly string _host;
        private readonly int _port;
        private readonly int _cap;
        private TcpClient? _tcp;
        private NetworkStream? _ns;

        // 送信直列化（Channelではなく“実送直列”）：未送信エラーを即検出して安全振替
        private readonly SemaphoreSlim _sendLock = new(1, 1);

        // 受信結果キュー（各接続→親へ）
        private readonly ConcurrentQueue<Message> _inResults = new();

        // 再接続バックオフ
        private readonly TimeSpan _backoffMin = TimeSpan.FromMilliseconds(500);
        private readonly TimeSpan _backoffMax = TimeSpan.FromSeconds(30);
        private readonly Random _rng = new();

        public int InflightClientSide;
        public string Name => $"{_host}:{_port}";

        // 接続状態
        public volatile bool IsAlive;
        public DateTime DisconnectedAt { get; private set; } = DateTime.MinValue;

        public LeaderConn(string clientId, string host, int port, int cap)
        {
            _clientId = clientId; _host = host; _port = port; _cap = Math.Max(1, cap);
        }

        public async Task ConnectAndHelloAsync(CancellationToken ct)
        {
            await ConnectOnceAsync(ct);

            // 受信ループ（切断で抜ける）＋ 自動再接続ループ
            _ = Task.Run(() => RecvAndReconnectLoopAsync(ct), ct);
        }

        private async Task ConnectOnceAsync(CancellationToken ct)
        {
            _tcp = new TcpClient();
            await _tcp.ConnectAsync(_host, _port, ct);
            _ns = _tcp.GetStream();

            var cfg = new ClientConfig(_clientId, _cap);
            await Codec.WriteAsync(_ns, new Message
            {
                Type = MsgType.HelloClient,
                Subject = _clientId,
                Payload = JsonSerializer.SerializeToUtf8Bytes(cfg)
            }, ct);

            IsAlive = true;
            Console.WriteLine($"[ClientX {_clientId}] connected {Name} cap={_cap}");
        }

        private async Task RecvAndReconnectLoopAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    if (_ns is null) break;
                    var m = await Codec.ReadAsync(_ns, ct);
                    if (m is null) throw new Exception("disconnected");

                    if (m.Type == MsgType.Result)
                        _inResults.Enqueue(m);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch
                {
                    // 切断検知
                    MarkDead();

                    // 再接続
                    var backoff = _backoffMin;
                    while (!ct.IsCancellationRequested)
                    {
                        try
                        {
                            await Task.Delay(Jitter(backoff), ct);
                            await ConnectOnceAsync(ct);
                            break;
                        }
                        catch
                        {
                            backoff = TimeSpan.FromMilliseconds(Math.Min(backoff.TotalMilliseconds * 2, _backoffMax.TotalMilliseconds));
                        }
                    }
                }
            }
        }

        private void MarkDead()
        {
            try { _ns?.Dispose(); } catch { }
            try { _tcp?.Dispose(); } catch { }
            _ns = null; _tcp = null;
            if (IsAlive)
            {
                IsAlive = false;
                DisconnectedAt = DateTime.UtcNow;
                Console.WriteLine($"[ClientX {_clientId}] disconnected {Name}");
            }
        }

        private TimeSpan Jitter(TimeSpan t)
        {
            var f = 1.0 + ((_rng.NextDouble() * 2 - 1) * 0.20); // ±20%
            return TimeSpan.FromMilliseconds(Math.Max(1, t.TotalMilliseconds * f));
        }

        // 実送信（直列化）。失敗したら例外を上位へ → 「未送信」扱いで他接続へ振替できる
        public async Task SendNowAsync(Message m, CancellationToken ct)
        {
            await _sendLock.WaitAsync(ct);
            try
            {
                if (_ns is null || !IsAlive) throw new InvalidOperationException("not connected");
                await Codec.WriteAsync(_ns, m, ct);
            }
            finally
            {
                _sendLock.Release();
            }
        }

        public async Task<Message?> TryReadResultAsync(CancellationToken ct)
        {
            if (_inResults.TryDequeue(out var m)) return m;
            await Task.Yield();
            return null;
        }
    }

    // ===== ラウンドロビン（配列を回すだけの簡易実装）=====
    private static class RrUtil
    {
        private static int _rr;
        public static T RoundRobin<T>(T[] xs) where T : class
        {
            var i = (System.Threading.Interlocked.Increment(ref _rr) & int.MaxValue) % xs.Length;
            return xs[i];
        }
    }
}
