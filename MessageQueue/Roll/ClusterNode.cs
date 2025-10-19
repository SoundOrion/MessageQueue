// ClusterNode.cs
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using MessageQueue.Common; // Codec / Message
using static Message;      // MsgType

namespace MessageQueue.Roll;

/// <summary>
/// Raft-lite 実装（実用最小構成）:
/// - 過半数レプリケーション / コミット (nextIndex / matchIndex)
/// - ハートビート（空の AppendEntries）
/// - ログ不一致の巻き戻し（nextIndex 後退）
/// - ランダム選挙タイムアウト 250–500ms と選挙 (RequestVote)
/// - InstallSnapshot は未実装（必要時に拡張）
///
/// 想定配線：
///   var raft = new ClusterNode(groupId, listenPort, peers, OnCommittedAsync);
///   raft.Start();
///   // Leader のときだけ
///   await raft.AppendAndReplicateAsync(new WalEnqueue(...), ct);
///
/// 受信（Leader.cs 側で Codec を使ってメッセージ受信後に呼び出し）:
///   if (msg.Type == MsgType.AppendEntries) { Parse→ raft.OnAppendEntriesAsync(...); AppendResp を返す }
///   if (msg.Type == MsgType.RequestVote)   { Parse→ raft.OnRequestVote(...); VoteResp を返す }
/// </summary>
public sealed class ClusterNode : IAsyncDisposable
{
    public enum Role { Follower, Candidate, Leader }

    // ---- public API ---------------------------------------------------------
    public ClusterNode(string group, int selfPort, IEnumerable<string> peers, Func<object, Task> onCommit)
    {
        _group = group;
        _selfPort = selfPort;
        _self = $"{GetSelfIp()}:{selfPort}";
        _peers = peers?.Where(p => !string.IsNullOrWhiteSpace(p) && p != _self).Distinct().ToArray()
                 ?? Array.Empty<string>();
        _onCommit = onCommit ?? throw new ArgumentNullException(nameof(onCommit));
    }

    /// <summary>バックグラウンドの心拍・選挙ループを起動</summary>
    public void Start()
    {
        if (_bgLoop != null) return;
        _bgLoop = Task.Run(() => RunAsync(_cts.Token));
    }

    /// <summary>
    /// Leader のみ使用: WAL エントリを追加し、過半数に複製・コミットする。
    /// Commit された順に <see cref="_onCommit"/> が呼ばれる。
    /// </summary>
    public async Task AppendAndReplicateAsync(object walEntry, CancellationToken ct)
    {
        if (_role != Role.Leader) throw new InvalidOperationException("not leader");

        lock (_mu)
        {
            _log.Add(new LogEntry(_currentTerm, walEntry));
            _matchIndex[_self] = _log.Count; // 自ノードの match は常に末尾
        }

        await BroadcastAppendAsync(ct); // 直ちに送信（失敗は次心拍で再試行）
        TryAdvanceCommit();
        await ApplyCommittedAsync(ct);
    }

    /// <summary>現在のロール/推測リーダと任期（デバッグ等に）</summary>
    public (Role role, string? leaderHint, long term) SnapshotRole()
        => (_role, _role == Role.Leader ? _self : _lastKnownLeader, _currentTerm);

    // ---- RPC handlers (Leader.csから呼ばれる) ------------------------------
    /// <summary>Follower 側: AppendEntries を処理して応答。</summary>
    public async Task<(bool ok, long term, int matchIndex)> OnAppendEntriesAsync(
        long term, string leaderId, int prevLogIndex, long prevLogTerm,
        JsonElement? entryElement, int leaderCommit, CancellationToken ct)
    {
        if (term < _currentTerm)
        {
            return (false, _currentTerm, GetLastIndex());
        }

        if (term > _currentTerm)
        {
            BecomeFollower(term, leaderId);
        }
        else
        {
            _role = Role.Follower; // 心拍
            _lastKnownLeader = leaderId;
        }
        BumpElectionDeadline();

        lock (_mu)
        {
            // prev 整合性チェック
            if (prevLogIndex > 0)
            {
                if (_log.Count < prevLogIndex) return (false, _currentTerm, GetLastIndex());
                if (_log[prevLogIndex - 1].Term != prevLogTerm) return (false, _currentTerm, GetLastIndex());
            }

            // entry 追加（あれば）
            if (entryElement.HasValue && entryElement.Value.ValueKind != JsonValueKind.Null)
            {
                var entryObj = DeserializeWalFromJsonElement(entryElement.Value);
                while (_log.Count > prevLogIndex) _log.RemoveAt(_log.Count - 1); // 競合切り落とし
                _log.Add(new LogEntry(term, entryObj));
            }

            // commit に追随
            if (leaderCommit > _commitIndex)
            {
                _commitIndex = Math.Min(leaderCommit, _log.Count);
            }
        }

        await ApplyCommittedAsync(ct);
        return (true, _currentTerm, GetLastIndex());
    }

    /// <summary>Follower/Candidate: RequestVote に応答。</summary>
    public (bool voteGranted, long term) OnRequestVote(
        long term, string candidateId, int lastLogIndex, long lastLogTerm)
    {
        if (term < _currentTerm) return (false, _currentTerm);

        if (term > _currentTerm)
        {
            BecomeFollower(term, leaderHint: candidateId);
        }

        bool upToDate;
        lock (_mu)
        {
            if (_log.Count == 0) upToDate = true;
            else
            {
                var (myLastTerm, myLastIndex) = (_log[^1].Term, _log.Count);
                upToDate = (lastLogTerm > myLastTerm) ||
                           (lastLogTerm == myLastTerm && lastLogIndex >= myLastIndex);
            }
        }

        if ((_votedFor is null || _votedFor == candidateId) && upToDate)
        {
            _votedFor = candidateId;
            BumpElectionDeadline();
            return (true, _currentTerm);
        }

        return (false, _currentTerm);
    }

    // ---- 内部: Raft state ---------------------------------------------------
    private readonly string _group;
    private readonly int _selfPort;
    private readonly string _self; // "ip:port"
    private readonly string[] _peers;
    private readonly Func<object, Task> _onCommit;

    private readonly object _mu = new(); // ログ/コミットのロック

    private volatile Role _role = Role.Follower;
    private long _currentTerm = 0;
    private string? _votedFor = null;

    private readonly List<LogEntry> _log = new(); // index 1 起算（内部は0始まり）
    private int _commitIndex = 0;
    private int _lastApplied = 0;

    private readonly ConcurrentDictionary<string, int> _nextIndex = new();  // follower -> next index to send
    private readonly ConcurrentDictionary<string, int> _matchIndex = new(); // follower -> highest replicated index
    private string? _lastKnownLeader = null;

    // タイマ
    private readonly CancellationTokenSource _cts = new();
    private Task? _bgLoop;
    private DateTime _electionDeadline = DateTime.UtcNow + TimeSpan.FromMilliseconds(400);
    private DateTime _nextHeartbeatAt = DateTime.UtcNow;
    private readonly Random _rng = new();
    private int _votes = 0;

    // ---- 背景ループ ---------------------------------------------------------
    private async Task RunAsync(CancellationToken ct)
    {
        // 初期化
        lock (_mu)
        {
            _matchIndex[_self] = _log.Count;
        }
        foreach (var p in _peers)
        {
            _nextIndex[p] = Math.Max(1, _log.Count + 1);
            _matchIndex[p] = 0;
        }

        while (!ct.IsCancellationRequested)
        {
            try { await Task.Delay(50, ct); } catch { break; }

            var now = DateTime.UtcNow;

            if (_role == Role.Leader)
            {
                if (now >= _nextHeartbeatAt)
                {
                    _nextHeartbeatAt = now + TimeSpan.FromMilliseconds(150);
                    await BroadcastAppendAsync(ct, heartbeatOnly: true); // 空心拍
                }

                TryAdvanceCommit();
                await ApplyCommittedAsync(ct);
            }
            else
            {
                if (now >= _electionDeadline)
                {
                    BecomeCandidate();
                    BumpElectionDeadline();
                    await BroadcastRequestVoteAsync(ct);
                }
            }
        }
    }

    // ---- 状態遷移 -----------------------------------------------------------
    private void BecomeFollower(long term, string? leaderHint)
    {
        _role = Role.Follower;
        _currentTerm = term;
        _votedFor = null;
        _lastKnownLeader = leaderHint;
        BumpElectionDeadline();
    }

    private void BecomeCandidate()
    {
        _role = Role.Candidate;
        _currentTerm++;
        _votedFor = _self;
        _lastKnownLeader = null;
        _votes = 1; // 自票
    }

    private void BecomeLeader()
    {
        _role = Role.Leader;
        _lastKnownLeader = _self;

        var next = GetLastIndex() + 1;
        foreach (var p in _peers)
        {
            _nextIndex[p] = next;
            _matchIndex[p] = 0;
        }
        lock (_mu) { _matchIndex[_self] = _log.Count; }
        _nextHeartbeatAt = DateTime.UtcNow; // すぐ心拍
    }

    private void BumpElectionDeadline()
    {
        // 250–500ms
        _electionDeadline = DateTime.UtcNow + TimeSpan.FromMilliseconds(250 + _rng.Next(0, 251));
    }

    // ---- 送信: AppendEntries / RequestVote ----------------------------------
    private async Task BroadcastAppendAsync(CancellationToken ct, bool heartbeatOnly = false)
    {
        var tasks = new List<Task>(_peers.Length);

        foreach (var peer in _peers)
        {
            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    int sendPrevIdx;
                    long sendPrevTerm;
                    object? sendEntry = null;

                    lock (_mu)
                    {
                        var nextIdx = _nextIndex[peer];
                        if (heartbeatOnly || nextIdx > _log.Count)
                        {
                            sendPrevIdx = _log.Count;
                            sendPrevTerm = _log.Count == 0 ? 0 : _log[^1].Term;
                            sendEntry = null;
                        }
                        else
                        {
                            sendPrevIdx = nextIdx - 1;
                            sendPrevTerm = sendPrevIdx == 0 ? 0 : _log[sendPrevIdx - 1].Term;
                            sendEntry = _log[nextIdx - 1].Entry;
                        }
                    }

                    var payload = EncodeAppendEntriesPayload(
                        term: _currentTerm,
                        leader: _self,
                        prevLogIndex: sendPrevIdx,
                        prevLogTerm: sendPrevTerm,
                        entry: sendEntry,
                        leaderCommit: _commitIndex);

                    using var tcp = new TcpClient();
                    var parts = peer.Split(':');
                    await tcp.ConnectAsync(parts[0], int.Parse(parts[1]), ct);
                    using var ns = tcp.GetStream();

                    await Codec.WriteAsync(ns, new Message { Type = MsgType.AppendEntries, Payload = payload }, ct);

                    var resp = await Codec.ReadAsync(ns, ct);
                    if (resp?.Type != MsgType.AppendResp) return;

                    var doc = JsonDocument.Parse(resp.Payload);
                    var success = doc.RootElement.GetProperty("success").GetBoolean();
                    var peerTerm = doc.RootElement.GetProperty("term").GetInt64();
                    var peerMatch = doc.RootElement.TryGetProperty("matchIndex", out var mi) ? mi.GetInt32() : 0;

                    if (peerTerm > _currentTerm)
                    {
                        BecomeFollower(peerTerm, leaderHint: peer);
                        return;
                    }
                    if (_role != Role.Leader) return;

                    if (success)
                    {
                        _matchIndex[peer] = peerMatch;
                        _nextIndex[peer] = Math.Max(_nextIndex[peer], peerMatch + 1);
                    }
                    else
                    {
                        _nextIndex[peer] = Math.Max(1, _nextIndex[peer] - 1); // 巻き戻し
                    }
                }
                catch
                {
                    // ネットワーク失敗は次の心拍で再試行
                }
            }, ct));
        }

        if (tasks.Count > 0) await Task.WhenAll(tasks);
    }

    private byte[] EncodeAppendEntriesPayload(long term, string leader, int prevLogIndex, long prevLogTerm, object? entry, int leaderCommit)
    {
        object? entryPayload = null;
        if (entry != null)
        {
            var json = JsonSerializer.Serialize(entry, _entryJsonOptions);
            entryPayload = JsonDocument.Parse(json).RootElement.Clone(); // object? として詰める
        }

        return JsonSerializer.SerializeToUtf8Bytes(new
        {
            group = _group,
            term,
            leader,
            prevLogIndex,
            prevLogTerm,
            entry = entryPayload,   // null か JsonElement
            leaderCommit
        });
    }

    private async Task BroadcastRequestVoteAsync(CancellationToken ct)
    {
        var last = GetLastIndexAndTerm();
        var tasks = new List<Task>(_peers.Length);

        foreach (var p in _peers)
        {
            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    using var tcp = new TcpClient();
                    var parts = p.Split(':');
                    await tcp.ConnectAsync(parts[0], int.Parse(parts[1]), ct);
                    using var ns = tcp.GetStream();

                    var payload = JsonSerializer.SerializeToUtf8Bytes(new
                    {
                        group = _group,
                        term = _currentTerm,
                        candidate = _self,
                        lastLogIndex = last.index,
                        lastLogTerm = last.term
                    });

                    await Codec.WriteAsync(ns, new Message { Type = MsgType.RequestVote, Payload = payload }, ct);

                    var resp = await Codec.ReadAsync(ns, ct);
                    if (resp?.Type != MsgType.VoteResp) return;

                    var doc = JsonDocument.Parse(resp.Payload);
                    var granted = doc.RootElement.GetProperty("granted").GetBoolean();
                    var peerTerm = doc.RootElement.GetProperty("term").GetInt64();

                    if (peerTerm > _currentTerm)
                    {
                        BecomeFollower(peerTerm, leaderHint: p);
                        return;
                    }
                    if (_role != Role.Candidate) return;

                    if (granted)
                    {
                        var got = Interlocked.Increment(ref _votes);
                        if (got > (_peers.Length + 1) / 2)
                        {
                            BecomeLeader();
                        }
                    }
                }
                catch
                {
                    // 失敗は無視（再投票や再選挙に期待）
                }
            }, ct));
        }

        if (tasks.Count > 0) await Task.WhenAll(tasks);
    }

    // ---- commit/apply --------------------------------------------------------
    private void TryAdvanceCommit()
    {
        if (_role != Role.Leader) return;

        var matches = _matchIndex.Values.Concat(new[] { GetLastIndex() }).ToArray();
        Array.Sort(matches);
        var majorityIndex = matches[(matches.Length - 1) / 2]; // 中央値 ≒ 過半数が到達

        lock (_mu)
        {
            if (majorityIndex > _commitIndex)
            {
                // 厳密には log[N].term == currentTerm を要求するが、最小実装では緩和
                _commitIndex = majorityIndex;
            }
        }
    }

    private async Task ApplyCommittedAsync(CancellationToken ct)
    {
        List<object> toApply = new();

        lock (_mu)
        {
            while (_lastApplied < _commitIndex)
            {
                _lastApplied++;
                toApply.Add(_log[_lastApplied - 1].Entry);
            }
        }

        foreach (var e in toApply)
        {
            await _onCommit(e);
        }
    }

    // ---- ユーティリティ ------------------------------------------------------
    private int GetLastIndex()
    {
        lock (_mu) return _log.Count;
    }

    private (int index, long term) GetLastIndexAndTerm()
    {
        lock (_mu)
        {
            if (_log.Count == 0) return (0, 0);
            return (_log.Count, _log[^1].Term);
        }
    }

    private static string GetSelfIp()
    {
        try
        {
            var host = Dns.GetHostEntry(Dns.GetHostName());
            var ip = host.AddressList.First(a => a.AddressFamily == AddressFamily.InterNetwork);
            return ip.ToString();
        }
        catch { return "127.0.0.1"; }
    }

    // ---- WAL 逆シリアライズ --------------------------------------------------
    private static readonly JsonSerializerOptions _entryJsonOptions = new(JsonSerializerDefaults.Web)
    {
        PropertyNameCaseInsensitive = true,
        NumberHandling = JsonNumberHandling.AllowReadingFromString
    };

    private static object DeserializeWalFromJsonElement(JsonElement e)
    {
        // 期待: e は { "type": "...", ... } の JSON か、string(JSON) の場合あり
        if (e.ValueKind == JsonValueKind.String)
        {
            var inner = JsonDocument.Parse(e.GetString()!).RootElement;
            return DeserializeWalFromJsonElement(inner);
        }

        if (!e.TryGetProperty("type", out var tProp))
            throw new InvalidOperationException("entry missing 'type'");

        var type = tProp.GetString();
        return type switch
        {
            "enqueue" => e.Deserialize<WalEnqueue>(_entryJsonOptions)!,
            "assign" => e.Deserialize<WalAssign>(_entryJsonOptions)!,
            "ack" => e.Deserialize<WalAck>(_entryJsonOptions)!,
            "timeout_requeue" => e.Deserialize<WalTimeoutRequeue>(_entryJsonOptions)!,
            "dlq" => e.Deserialize<WalDlq>(_entryJsonOptions)!,
            "worker_down_requeue" => e.Deserialize<WalWorkerDownRequeue>(_entryJsonOptions)!,
            _ => throw new NotSupportedException($"unknown wal type: {type}")
        };
    }

    // ---- 型 ------------------------------------------------------------------
    private sealed class LogEntry
    {
        public long Term { get; }
        public object Entry { get; }
        public LogEntry(long term, object entry) { Term = term; Entry = entry; }
    }

    // ---- IDisposable ---------------------------------------------------------
    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        if (_bgLoop != null)
        {
            try { await _bgLoop; } catch { }
        }
    }
}
