# 全体像（1リーダー・Nワーカー・複数クライアント）

```
Clients (Submitters)  ──(TCP)──►  Leader (Broker/Scheduler)  ──(TCP)──►  Workers
   └─ SubmitJob                                └─ AssignJob                       └─ AckJob
                                              ▲ └─ 受信: Credit(枠)              ▲
                                              └───── HelloClient/HelloWorker ─────┘
```

* **Leader**：ジョブを受け取り、キューに積み、**枠(Credit)** を持つ Worker に**割当(Assign)**。
* **Client**：ジョブを**送信(Submit)**。
* **Worker**：手が空いたら **Credit** を申告 → **Assign** を受けて処理 → **Ack** 返す。

---

# 役割（コードの責務）

## Leader（`Leader.cs`）

* `TcpListener` で接続待ち → 各接続を `HandleClientAsync` に渡す
* 最初の1通で**役割判別**（`HelloClient` / `HelloWorker`）

  * `HelloClient`：`HandleSubmitterAsync` に入り、`SubmitJob` を受けて **ConcurrentQueue** に `Enqueue`
  * `HelloWorker`：`HandleWorkerAsync` に入り、`Credit` を受けるたびにキューから取り出して **`AssignJob` を送信**
* キー構造

  * `_queue : ConcurrentQueue<byte[]>` … 未割当ジョブ
  * `_workers : List<NetworkStream>` … ワーカー接続（最小版では単純管理）
* メッセージ I/O は共通の `Codec.WriteAsync / ReadAsync` を使用

## Client（`Client.cs`）

* Leader に接続 → `HelloClient` 送信
* ループで `SubmitJob` を送る（今回は文字列 → 後で数十MBに差し替え可）
* 応答待ちは最小版ではなし（次のステップで `Response` を追加可能）

## Worker（`Worker.cs`）

* Leader に接続 → `HelloWorker` 送信
* すぐに `Credit(1)` を送る ＝ **「1件受けられます」**
* `AssignJob` を受け取ったらダミー処理（`Task.Delay`）→ `AckJob` 送信 → 再度 `Credit(1)`
* ＝ **クレジット制の最小サイクル**を実装

## Codec（`Codec.cs`）

* 最小フレーム：

  ```
  [length:4B LE][type:1B][payloadLen:4B LE][payload:payloadLen]
  ```
* `MsgType`（列挙）でメッセージ種別を表現：
  `SubmitJob / AssignJob / AckJob / Credit / HelloClient / HelloWorker`

---

# メッセージの流れ（時系列）

## 1. 接続と役割宣言

```
Client ── HelloClient ──► Leader
Worker ── HelloWorker ──► Leader
```

## 2. Worker が枠を申告（クレジット制）

```
Worker ── Credit(1) ──► Leader   // 受け入れ可能数を伝える
```

## 3. Client がジョブ投入

```
Client ── SubmitJob(payload) ──► Leader
Leader: _queue.Enqueue(payload)
```

## 4. Leader が割り当て

```
Leader: _queue.TryDequeue(out payload)
Leader ── AssignJob(payload) ──► Worker
```

## 5. Worker が実行してAck

```
Worker: run(payload)  // 実処理（ここはアプリ依存）
Worker ── AckJob ──► Leader
Worker ── Credit(1) ──► Leader   // 次のジョブを受ける準備完了
```

> この 2〜5 が回り続け、**クレジット**でフロー制御されます。

---

# 依存関係（呼び出し関係の簡易図）

```
Program.cs
  ├─ new Leader().RunAsync()
  │    ├─ AcceptTcpClientAsync()
  │    ├─ HandleClientAsync()
  │    │    ├─ Codec.ReadAsync()  // Hello
  │    │    ├─ HandleSubmitterAsync()  // SubmitJob -> _queue
  │    │    └─ HandleWorkerAsync()     // Credit -> AssignJob, Ack受信
  │    └─ Codec.WriteAsync()
  ├─ new Worker().RunAsync()
  │    ├─ Codec.WriteAsync(HelloWorker)
  │    ├─ Codec.WriteAsync(Credit)
  │    ├─ Codec.ReadAsync(AssignJob)
  │    └─ Codec.WriteAsync(AckJob)
  └─ new Client().RunAsync()
       ├─ Codec.WriteAsync(HelloClient)
       └─ Codec.WriteAsync(SubmitJob)
```

---

# よくある質問（運用の勘所）

* **Q. なぜクレジット制？**
  A. Worker 側の処理能力に合わせて「受け入れ可能数」を明示するため。リーダーが勝手に押し込むと詰まります。
* **Q. メッセージ順序は保証される？**
  A. TCP 接続ごとの順序は保証されますが、複数 Worker へ配ると**完了順**は入れ替わります。必要なら `msgId/corrId` を導入します。
* **Q. 再送/重複排除は？**
  A. 最小版では未実装。次ステップで **`msgId` + LRU（重複排除）**、**Assign の Ack タイムアウト→再キュー** を入れます。
* **Q. 大きいペイロード（数十MB）は？**
  A. `payloadLen` は Int32 なのでOK。送信時は **FileStream→CopyToAsync** で大きな配列を作らない、必要なら**圧縮**（小さくなれば）を追加。

---

# 次の一歩（段階的拡張）

1. **相関ID（msgId/corrId）** と **Ack のタイムアウト再送**
2. **複数 Worker** + 簡易スコアリング（CPU / RunningJobs / Credit）
3. **圧縮（gzip/brotli）** と **ヘッダ領域**（方式・メタ）
4. **観測**（キュー長、配信遅延、再送数）
5. **グループ分割 & Subject**（`job.submit.{group}` / `job.assign.{group}.{workerId}`）

ナイス！！🔥
ついに来たね、「**グループ分割 + Subject ベースのルーティング**」。
これは RabbitMQ の *routing key* や NATS の *subject* に相当する概念で、
**分散計算の中核的アーキテクチャ要素** になります。

ここから、Leader が「どのジョブをどのグループに配るか」を制御できるようになります。
以下、全体の設計 → 具体コード案 → 拡張ポイントまで、分かりやすく整理します👇

---

## 🧩 まずコンセプトの整理

### 🧠 今の構造（Before）

```
Client → Leader → Worker
```

* 全ジョブを単一キュー `_queue` で管理
* Worker も全員が同じ種類の仕事を処理

👉 小規模ならOKだけど、大規模分散だと「どのWorkerが何をするか」が曖昧。

---

### 🚀 これから（After）

```
Client → Leader → WorkerGroup[n] → Worker
```

* `Client` は job に **Subject**（＝「宛先トピック」）を指定して送信
* `Leader` は Subject の prefix を解析して、対応するグループのキューに入れる
* 各 `Worker` は **自分が属するグループだけ購読**

---

## 🧱 Subjectとは？

NATS 風にすると：

| Subject 名               | 意味                       |
| ----------------------- | ------------------------ |
| `job.submit.A`          | クライアントが「グループA」にジョブ送信     |
| `job.submit.B`          | グループB向けのジョブ              |
| `job.assign.A.worker-1` | Leader → worker-1（グループA） |
| `job.assign.B.worker-9` | Leader → worker-9（グループB） |

つまり Subject ＝ **メッセージのルーティングキー**。

---

## ⚙️ 実装設計

### ① メッセージ構造を拡張

`Message` に `Subject` プロパティを追加。

```csharp
public sealed class Message
{
    public MsgType Type { get; init; }
    public Guid MsgId { get; init; }
    public Guid CorrId { get; init; }
    public string Subject { get; init; } = string.Empty;
    public byte[] Payload { get; init; } = Array.Empty<byte>();
}
```

---

### ② Leader がグループ単位のキューを持つ

```csharp
private readonly ConcurrentDictionary<string, ConcurrentQueue<JobEnvelope>> _queues = new();
```

これで：

```csharp
_queues["groupA"]
_queues["groupB"]
```

のようにグループごとに独立キューを持てる。

---

### ③ Worker が所属グループを宣言して接続

Worker 側の Hello を変更：

```csharp
await Codec.WriteAsync(ns, new Message {
    Type = MsgType.HelloWorker,
    MsgId = _workerId,
    Subject = "groupA" // 所属グループ名
}, ct);
```

Leader 側は：

```csharp
if (hello.Type == MsgType.HelloWorker)
{
    var group = hello.Subject;
    var wc = new WorkerConn(workerId, ns, group);
    _workers[workerId] = wc;
    Console.WriteLine($"Worker {workerId} joined group {group}");
}
```

---

### ④ Client 側がジョブ送信時に Subject を指定

```csharp
await Codec.WriteAsync(ns, new Message {
    Type = MsgType.SubmitJob,
    MsgId = Guid.NewGuid(),
    Subject = "groupA",
    Payload = payload
}, ct);
```

---

### ⑤ Leader がルーティング

`HandleSubmitterAsync()` でこうする：

```csharp
var group = string.IsNullOrEmpty(m.Subject) ? "default" : m.Subject;
var queue = _queues.GetOrAdd(group, _ => new ConcurrentQueue<JobEnvelope>());
queue.Enqueue(new JobEnvelope(m.MsgId, m.Payload, group));

Console.WriteLine($"[Leader] Enqueued job {m.MsgId} to group {group}");
TryAssign(group);
```

---

### ⑥ TryAssign() をグループ別に動かす

```csharp
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
            SendAssign(target, job, attempt: 1);
        else break;
    }
}
```

---

### ⑦ WorkerConn に Group を追加

```csharp
private sealed class WorkerConn
{
    public Guid WorkerId { get; }
    public string Group { get; }
    public NetworkStream Stream { get; }
    public int Credit;
    public int Running;

    public WorkerConn(Guid id, NetworkStream s, string group)
    {
        WorkerId = id;
        Stream = s;
        Group = group;
        Credit = 0;
        Running = 0;
    }
}
```

---

### ⑧ JobEnvelope もグループを保持

```csharp
private sealed record JobEnvelope(Guid JobId, byte[] Payload, string Group);
```

---

## 🧭 全体の流れ（例）

1. **Worker1(group=A)**, **Worker2(group=B)** が接続
2. Clientが `Subject="groupA"` のジョブ送信
3. Leaderが `_queues["groupA"]` に投入
4. `TryAssign("groupA")` で Worker1 に配信
5. Worker2 には全く届かない

---

## ✅ メリット

| 項目          | 効果                                            |
| ----------- | --------------------------------------------- |
| 🔀 負荷分散     | グループ単位でジョブを並列化できる                             |
| 🎯 精密ルーティング | 特定のグループに限定して配信                                |
| 🔒 独立障害     | グループAのWorkerが落ちてもBは影響なし                       |
| 🔧 拡張性      | 将来的に Subject をワイルドカード対応可能に（例: `job.submit.*`） |

---

## 🔮 次のステップ候補

1. **Subject にワイルドカード（`*`, `>`）対応**
2. **各グループにリーダー1人＋フォロワーN人**（Group内再分配）
3. **Jobルーティングポリシー（RoundRobin / LeastLoad / Sticky）**
4. **統計情報（グループ別キュー長・再送数）**

---

次やるなら：

* 「`TryAssign` が全グループを自動的に回す」
* 「Subject にワイルドカードを導入する」
  このどっちかが良いステップです。

どっちからいく？

最高のタイミングです👏
あなたが今作っているのは「**NATS風の軽量メッセージングシステム**」で、
Subject（トピック）ルーティングとワイルドカード対応を実装した時点で、
立派な**分散メッセージブローカーの基礎**ができあがっています。

以下に、**「READMEとしてそのまま使える」完全版**を書きました。
コードの意味・機能・設計意図・使い方をすべて丁寧に説明しています👇

---

# 📨 MessageQueue: Minimal NATS-like Message Broker in C#

## 概要

`MessageQueue` は C# と `TcpClient` / `NetworkStream` ベースで構築された
**軽量分散メッセージングシステム**です。

Leader（ブローカー）・Worker（消費者）・Client（送信者）の3ロール構成で、
Subject（サブジェクト）ベースのルーティング、ワイルドカード購読（`*` / `>`）、
再送・重複排除（Dedup）まで備えています。

---

## 🌐 システム構成

```
Client  --->  Leader  --->  Worker(s)
                 │
                 ├─ job.assign.A.* → WorkerA
                 ├─ job.assign.B.> → WorkerB
                 └─ job.assign.common.> → 全Worker
```

### 各ロールの役割

| 役割         | 説明                                           |
| ---------- | -------------------------------------------- |
| **Client** | ジョブを送信する側。`Subject` を指定して Leader に投げる。       |
| **Leader** | メッセージブローカー。ジョブを受け、Subjectパターンで対応するWorkerへ配送。 |
| **Worker** | 対象Subject（購読パターン）にマッチするジョブを受信・処理・ACK返信。      |

---

## ⚙️ コード構成

```
MessageQueue/
├─ Common/
│  ├─ Message.cs          // メッセージ構造定義
│  ├─ Codec.cs            // [len][payload] 形式の送受信ユーティリティ
│  ├─ DedupCache.cs       // 重複排除キャッシュ
│  └─ SubjectMatcher.cs   // NATS風ワイルドカードマッチ（*, >対応）
│
├─ Roll/
│  ├─ Leader.cs           // メインブローカー
│  ├─ Worker.cs           // ワーカー実行ノード
│  └─ Client.cs           // ジョブ送信クライアント
│
└─ Program.cs             // 起動テスト・エントリポイント
```

---

## 📬 SubjectMatcher.cs（ワイルドカード対応の要）

```csharp
using System;

namespace MessageQueue.Common;

public static class SubjectMatcher
{
    public static bool Match(string pattern, string subject)
    {
        if (pattern == ">") return true;

        var pSegs = pattern.Split('.', StringSplitOptions.RemoveEmptyEntries);
        var sSegs = subject.Split('.', StringSplitOptions.RemoveEmptyEntries);

        for (int i = 0; i < pSegs.Length; i++)
        {
            if (pSegs[i] == ">")
                return true; // 以降の階層をすべて許可

            if (i >= sSegs.Length)
                return false; // subjectが短すぎる

            if (pSegs[i] == "*")
                continue; // 任意の1階層OK

            if (!pSegs[i].Equals(sSegs[i], StringComparison.OrdinalIgnoreCase))
                return false;
        }

        return sSegs.Length == pSegs.Length;
    }
}
```

### 🧠 動作例

| パターン             | サブジェクト                | 結果 |
| ---------------- | --------------------- | -- |
| `job.assign.A.*` | `job.assign.A.1`      | ✅  |
| `job.assign.A.>` | `job.assign.A.1.task` | ✅  |
| `job.assign.A.B` | `job.assign.A.B`      | ✅  |
| `job.assign.B.*` | `job.assign.A.1`      | ❌  |

---

## 🧩 Worker.cs（ジョブ受信とACK返信）

```csharp
using MessageQueue.Common;
using System.Buffers.Binary;
using System.Net.Sockets;

namespace MessageQueue.Roll;

public sealed class Worker
{
    private readonly string _host;
    private readonly int _port;
    private readonly string _pattern; // ← Subjectパターン（購読対象）
    private readonly Guid _workerId = Guid.NewGuid();
    private readonly DedupCache _dedup = new(TimeSpan.FromMinutes(10));

    public Worker(string host, int port, string subjectPattern)
    {
        _host = host; 
        _port = port;
        _pattern = string.IsNullOrWhiteSpace(subjectPattern) ? "job.assign.>" : subjectPattern;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        using var client = new TcpClient();
        await client.ConnectAsync(_host, _port, ct);
        using var ns = client.GetStream();

        // Hello: 自分が購読するSubjectパターンを送信
        await Codec.WriteAsync(ns, new Message { 
            Type = MsgType.HelloWorker, 
            MsgId = _workerId, 
            Subject = _pattern 
        }, ct);

        // 最初のクレジット
        await SendCreditAsync(ns, 1, ct);

        Console.WriteLine($"[Worker {_workerId}] started ({_pattern})");

        while (!ct.IsCancellationRequested)
        {
            var m = await Codec.ReadAsync(ns, ct);
            if (m is null) break;

            if (m.Type == MsgType.AssignJob)
            {
                var jobId = m.MsgId;

                if (_dedup.Contains(jobId))
                {
                    await Codec.WriteAsync(ns, new Message { Type = MsgType.AckJob, CorrId = jobId }, ct);
                    continue;
                }

                // ダミー処理（実ジョブ実行部）
                await Task.Delay(300, ct);

                _dedup.TryAdd(jobId);
                await Codec.WriteAsync(ns, new Message { Type = MsgType.AckJob, CorrId = jobId }, ct);

                // クレジット再送
                await SendCreditAsync(ns, 1, ct);
                _dedup.Sweep();
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
}
```

---

## 🧠 Leader.cs の Subject マッチング箇所

ジョブをキューから取り出して、どのワーカーへ送るかを決める部分。

```csharp
private void TryAssign(string group)
{
    if (!_queues.TryGetValue(group, out var queue)) return;

    while (!queue.IsEmpty)
    {
        WorkerConn? target;
        lock (_lock)
        {
            target = _workers.Values
                .Where(w =>
                    w.Credit > 0 &&
                    SubjectMatcher.Match(w.SubjectPattern, $"job.assign.{group}")
                )
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
```

### 🔍 ここでやっていること

1. すべてのWorkerの `SubjectPattern` をチェック。
2. `job.assign.{group}` にマッチしたWorkerだけ候補にする。
3. その中から最もクレジットがあるWorkerを選び、ジョブを送信。

---

## ✨ Subjectベース配信の例

### Worker 登録

```csharp
// Group A Worker
new Worker("localhost", 7000, "job.assign.A.*").RunAsync(ct);

// Group B Worker
new Worker("localhost", 7000, "job.assign.B.>").RunAsync(ct);
```

### Client 送信

```csharp
await Codec.WriteAsync(ns, new Message {
    Type = MsgType.SubmitJob,
    MsgId = Guid.NewGuid(),
    Subject = "job.assign.A.1",
    Payload = Encoding.UTF8.GetBytes("process this")
}, ct);
```

結果：
→ `"job.assign.A.1"` は `"job.assign.A.*"` にマッチするので、**GroupAのWorkerだけ**が受信します。

---

## 📊 機能一覧

| 機能              | 説明                                  |
| --------------- | ----------------------------------- |
| ✅ メッセージフレーミング   | `[len:4B][payload]` によるTCPストリーム境界管理 |
| ✅ Subjectルーティング | `"job.assign.A.*"` 形式で宛先を制御         |
| ✅ ワイルドカード対応     | `*`（1階層）, `>`（多階層）対応                |
| ✅ DedupCache    | 再送時のジョブ重複排除                         |
| ✅ Ack / Credit  | ワーカーのフロー制御                          |
| ✅ 再送＆指数バックオフ    | ACK未達時の再試行制御                        |
| ⚙️ 圧縮送信（オプション）  | Brotli圧縮付きFileSender対応              |

---

## 💬 開発メモ

* **SubjectMatcher.Match()** は MQTT / NATS / AMQP と互換的に設計
* **WorkerConn.SubjectPattern** により、Workerごとの受信対象を柔軟に設定可能
* Leader側は「複数購読」や「Pub/Sub配信」への拡張も容易

---

## 🚀 今後の拡張案

| 機能              | 内容                               |
| --------------- | -------------------------------- |
| 🕸️ Pub/Sub モード | 同一Subjectを複数Workerが購読（全員に配信）     |
| 💾 永続化          | 送信キューをファイルまたはSQLiteに書き出し         |
| 🧠 ワイルドカード最適化   | トライ木（prefix tree）でのSubjectマッチ高速化 |
| 🧩 Chunk転送      | 大型ファイルを複数フレームに分割して送信             |
| 📡 Gossipベース分散  | Leaderを複数立ててフェイルオーバー対応           |

---

## ✅ まとめ

この実装でできること：

* 🎯 「`job.assign.*`」のようなSubject指定ルーティング
* 🔁 再送制御と重複排除
* 🧵 ワーカーごとの購読パターン管理
* ⚙️ Leaderがメッセージを動的ルーティング

つまり、**RabbitMQ / NATS の基礎を自作した**状態です。
そこから信頼性・分散・フェイルオーバーを積み上げていけば、
あなた専用の「軽量メッセージング基盤」が完成します。

---

💡 次にやるなら：

* **複数Subject購読（Workerが `"job.assign.A.*"` と `"job.assign.common.>"` の両方を受ける）**
  これを入れると一気に NATS Pub/Sub の世界になります。

やる？

いい質問！
ここでいう「**キャッシュの投入 API**」は、**クライアントが先に入力ファイル群を“キャッシュID”付きでワーカーへ配布（プリウォーム）**できる仕組みです。ジョブ投入時は実ファイルを送らず、**`CacheId` を参照**するだけで OK になります。大量のジョブで同じデータを使い回す際に効きます。

以下、**仕様 → プロトコル → コード差分（最小）→ 使い方**の順でまとめます。

---

# 🎯 目的（何ができる？）

* 事前に `/opt/grid/cache` へ **任意のファイル（複数可）**を配布（全ワーカー or 対象ワーカー）
* 配布済みファイルは **`CacheId`** で参照し、`JobRequest.Files[].CacheId` に指定
* ワーカー側はジョブ実行時、`CacheId` からローカルファイルを解決して使う（中身は送らない）
* 重複投入は **冪等**（同じ `CacheId` は上書きせずスキップ／ACKのみ）

---

# 📡 プロトコル（メッセージ種別）

`Message.Type` に 2種を追加：

* `CachePut`（Client → Leader → Worker）… キャッシュ配布要求
* `CacheAck`（Worker → Leader → Client）… 配布完了通知（結果）

**Subject** の例：

* 送信（クライアント→リーダー）: `cache.put.job.assign.*`
  （← どのワーカーに配るかを *対象パターン* として埋め込み。例：全ワーカーは `job.assign.*`、calcA 専用は `job.assign.calcA.*`）
* 中継（リーダー→ワーカー）: `cache.put` 固定（中継時は中身で対象判断済みのため）

---

# 📦 データモデル（追加）

`Common/Models.cs` に追記：

```csharp
public record CacheEntry(string Name, byte[] Content);
public record CachePutRequest(
    string CacheId,                 // 例: "dataset-20251019-v1"
    List<CacheEntry> Entries,       // { "a.bin", bytes[] }, { "b.cfg", bytes[] } ...
    string TargetPattern            // 例: "job.assign.*" / "job.assign.calcA.*"
);

public record CachePutResult(
    string CacheId,
    string Status,                  // "OK" | "SKIPPED" | "FAILED"
    string Message                  // 補足メッセージ
);
```

`Common/Message.cs` の列挙体に追加：

```csharp
public enum MsgType : byte
{
    SubmitJob = 1, AssignJob = 2, AckJob = 3, Credit = 4, Result = 5,
    CachePut  = 6, CacheAck  = 7,                 // 👈 追加
    HelloClient = 9, HelloWorker = 10
}
```

---

# 🧠 ルール（冪等 & 競合回避）

* ワーカーは `/opt/grid/cache/<CacheId>/...` にファイルを書き込みます。
  既にフォルダが存在すれば **既存と同名ファイルはスキップ**（上書きしない）→ `SKIPPED` を返す。
* すべて新規で格納できれば `OK`。例外発生は `FAILED`。

---

# 🧩 コード差分（最小）

## 1) Leader：CachePut を受けて、対象ワーカーへファンアウト

`Roll/Leader.cs` の `HandleClientAsync` に分岐を追加：

```csharp
if (m.Type == MsgType.CachePut)
{
    var req = JsonSerializer.Deserialize<CachePutRequest>(m.Payload)!;

    List<Guid> targets;
    lock (_lock)
    {
        targets = _workers.Values
            .Where(w => SubjectMatcher.Match(req.TargetPattern, w.SubjectPattern))
            .Select(w => w.WorkerId)
            .ToList();
    }

    Console.WriteLine($"[Leader] CachePut {req.CacheId} -> targets={targets.Count} pattern={req.TargetPattern}");

    foreach (var wid in targets)
    {
        WorkerConn wc;
        lock (_lock) wc = _workers[wid];
        await Codec.WriteAsync(wc.Stream, new Message {
            Type = MsgType.CachePut,
            Subject = "cache.put",
            Payload = m.Payload,          // そのまま転送
            CorrId = m.MsgId              // クライアントの要求と相関
        }, ct);
    }

    continue;
}
```

同じく `HandleWorkerAsync` に **CacheAck** の中継を追加：

```csharp
case MsgType.CacheAck:
{
    // Worker からの応答をクライアントへ中継
    var result = JsonSerializer.Deserialize<CachePutResult>(m.Payload)!;
    Console.WriteLine($"[Leader] CacheAck {result.CacheId} {result.Status} - {result.Message}");

    // ここではブロードキャスト元のクライアントを特定できないため、簡易に「全クライアントへ通知」か、
    // もしくは CorrId を使って直近送信者を記録しておく簡易テーブルを作る。
    // 最小実装として全クライアントに流す：
    foreach (var cli in _clients.Values)
    {
        await Codec.WriteAsync(cli.Stream, new Message {
            Type = MsgType.CacheAck,
            Subject = $"cache.ack.{result.CacheId}",
            Payload = m.Payload,
            CorrId = m.CorrId
        }, ct);
    }
    break;
}
```

> ※ 本気でやるなら、**CachePut の送信元 ClientId → CorrId** を記録し、該当クライアントにのみ戻すテーブルを持つのが綺麗です（ここは最小で中継）。

---

## 2) Worker：cache.put を受けて保存、結果を返す

`Roll/Worker.cs` のメインループに処理を追加：

```csharp
if (m.Type == MsgType.CachePut)
{
    var req = JsonSerializer.Deserialize<CachePutRequest>(m.Payload)!;
    var (status, msg) = await PutCacheAsync(req, ct);

    var ack = new CachePutResult(req.CacheId, status, msg);
    await Codec.WriteAsync(ns, new Message {
        Type = MsgType.CacheAck,
        Subject = $"cache.ack.{req.CacheId}",
        Payload = JsonSerializer.SerializeToUtf8Bytes(ack),
        CorrId = m.CorrId
    }, ct);

    // クレジットは変えない（キャッシュ投入は処理スロットを消費しないポリシー）
    continue;
}
```

Worker にヘルパーを追加：

```csharp
private async Task<(string status, string message)> PutCacheAsync(CachePutRequest req, CancellationToken ct)
{
    try
    {
        var dir = Path.Combine(CacheDir, req.CacheId);
        Directory.CreateDirectory(dir);

        int added = 0, skipped = 0;
        foreach (var e in req.Entries)
        {
            var path = Path.Combine(dir, e.Name);
            var parent = Path.GetDirectoryName(path)!;
            Directory.CreateDirectory(parent);
            if (File.Exists(path)) { skipped++; continue; }

            await File.WriteAllBytesAsync(path, e.Content, ct);
            added++;
        }

        if (added > 0 && skipped == 0) return ("OK", $"added={added}");
        if (added == 0 && skipped > 0) return ("SKIPPED", $"already-present={skipped}");
        return ("OK", $"added={added}, skipped={skipped}");
    }
    catch (Exception ex)
    {
        return ("FAILED", ex.Message);
    }
}
```

> これで、**同じ `CacheId` の再投入はスキップ**され、冪等に動きます。

---

## 3) Client：CachePut の送信 API

`Roll/Client.cs` に送信メソッドを追加：

```csharp
public async Task PutCacheAsync(string cacheId, (string name, byte[] content)[] files, string targetPattern, CancellationToken ct)
{
    using var cli = new TcpClient();
    await cli.ConnectAsync(_host, _port, ct);
    using var ns = cli.GetStream();

    await Codec.WriteAsync(ns, new Message { Type = MsgType.HelloClient, Subject = _clientId }, ct);

    var req = new CachePutRequest(
        CacheId: cacheId,
        Entries: files.Select(f => new CacheEntry(f.name, f.content)).ToList(),
        TargetPattern: targetPattern
    );

    var mid = Guid.NewGuid();
    await Codec.WriteAsync(ns, new Message {
        Type = MsgType.CachePut,
        MsgId = mid,
        Subject = $"cache.put.{targetPattern}",
        Payload = JsonSerializer.SerializeToUtf8Bytes(req)
    }, ct);

    // 任意: ACK を待つ（ここでは 1 件のACKを待ったら返す簡易版）
    while (!ct.IsCancellationRequested)
    {
        var m = await Codec.ReadAsync(ns, ct);
        if (m is null) break;
        if (m.Type == MsgType.CacheAck)
        {
            var res = JsonSerializer.Deserialize<CachePutResult>(m.Payload)!;
            Console.WriteLine($"[Client {_clientId}] CacheAck {res.CacheId} {res.Status} {res.Message}");
            break;
        }
    }
}
```

> 実務では **全ワーカーからの ACK 集約**や **タイムアウト／未達の再投入**が必要ですが、まずはミニマムで。

---

# 🧪 使い方（例）

1. 先にキャッシュを投入（全ワーカーへ）

```csharp
var files = new[] {
    ("data/a.bin", File.ReadAllBytes("a.bin")),
    ("cfg/b.cfg",  File.ReadAllBytes("b.cfg"))
};
await client.PutCacheAsync(
    cacheId: "dataset-20251019-v1",
    files: files,
    targetPattern: "job.assign.*",
    ct: cts.Token
);
```

2. ジョブでは **`CacheId` を参照**するだけ：

```csharp
var job = new JobRequest(
    JobId: Guid.NewGuid(),
    ClientId: "clientA",
    ExecName: "calcA",
    Args: new() { "--mode=fast" },
    Files: new()
    {
        new InputFile("data/a.bin", "dataset-20251019-v1", null), // 👈 キャッシュ参照
        new InputFile("cfg/b.cfg",  "dataset-20251019-v1", null),
        new InputFile("input.txt",  null, Encoding.UTF8.GetBytes("small inline")) // 小さいならインラインOK
    }
);
```

---

# 📝 運用のヒント

* **ターゲット選択**：特定アプリだけなら `targetPattern = "job.assign.calcA.*"` にして配送量を節約。
* **大容量対応**：必要に応じて **チャンク分割**（`partNo/partCount`）を `CachePutRequest` に足すと安全。
* **検証**：キャッシュ整合性（SHA-256）を `CacheEntry` に持たせると改竄や破損検知が可能。
* **集約ACK**：Leader 側で「送った相手数・返ってきたACK数」をトラッキングし、一定割合で成功扱いにするなども現実的。

---

必要なら、上の差分を**あなたのプロジェクトにマージ済みの“全文版”**として出せます（置き換えコピペ用に整理）。
次は **チャンク転送** or **ACK集約** のどちらを先に入れる？



これらのC#ファイル群は、**簡易的な分散ジョブキューシステム（MessageQueue）**を実装しています。
クライアント（Client）、リーダー（Leader）、ワーカー（Worker）の3者がTCP通信を通じてジョブを送受信・実行します。
全体的に **軽量なRPC風メッセージプロトコル** と **リトライ／再送／重複排除** が組み込まれています。

以下で、各コンポーネントと処理の流れを日本語で詳しく解説します。

---

## 🧩 システム全体構成

```
Client → Leader → Worker
   ↑         ↓
   └── Result (戻り)
```

* **Client**: ジョブを送信する。
* **Leader**: ジョブをキューイングし、空いているWorkerに割り当てる。結果をClientへ返す。
* **Worker**: 指定されたジョブを実行し、結果をLeaderに返す。

---

## 📦 共通ライブラリ（MessageQueue.Common）

### 1. Codec.cs

**メッセージのシリアライズ／デシリアライズ処理**
TCPストリームに送るときの形式：

```
[len:4byte LE][type:1][msgId:16][corrId:16]
[subjectLen:2][subject:utf8][payloadLen:4][payload]
```

* `WriteAsync()`：Messageオブジェクトをこのバイナリ形式にエンコードして送信。
* `ReadAsync()`：受信したバイト列からMessageを復元。
* `ReadExactAsync()`：指定バイト数だけ正確に読み込む安全な読取関数。

👉 このレイヤーでネットワークI/Oの細かい処理を吸収しています。

---

### 2. Message.cs

メッセージの定義

```csharp
public enum MsgType { SubmitJob, AssignJob, AckJob, Credit, Result, HelloClient, HelloWorker }
public sealed class Message { ... }
```

* **MsgType**：通信の種類（ジョブ提出、ジョブ割り当て、ACK、結果、中継など）。
* **Message**：送受信の単位オブジェクト。Subjectはルーティング用の文字列。

---

### 3. Models.cs

ジョブ内容と結果のデータ構造

```csharp
record JobRequest(Guid JobId, string ClientId, string ExecName, List<string> Args, List<InputFile> Files);
record JobResult(Guid JobId, string ClientId, string ExecName, string Status, string Stdout, string Stderr, byte[]? OutputArchive);
```

---

### 4. DedupCache.cs

ワーカーが同じジョブを重複実行しないためのキャッシュ

* `TryAdd()`：IDを追加（重複があればfalse）。
* `Sweep()`：一定時間（TTL）経過した古いIDを削除。

---

### 5. SubjectMatcher.cs

ワーカーの購読パターンマッチ（`*`, `>` ワイルドカード対応）
例：

* `job.assign.calcA.*` → calcAジョブのみ受け取る
* `job.assign.*` → すべてのジョブを受け取る

---

## 🧠 Leader.cs（ジョブブローカー）

リーダーはシステムの中枢。以下の責務があります：

1. **Client／Worker接続受付**

   * `HelloClient` または `HelloWorker` メッセージで識別。
2. **ジョブキュー管理**

   * `SubmitJob` 受信 → ExecNameごとにキューへ格納。
3. **スケジューリング**

   * `_execRound` でラウンドロビン割り当て。
   * Workerの `Credit` と `Running` 状態を考慮。
4. **再送制御**

   * `AckJob` が来ないとき、指数バックオフ＋ジッターで再送。
   * 最大 `MaxAttempts=6` 回試行、失敗時は DLQ（Dead Letter Queue）へ。
5. **結果転送**

   * Workerの `AckJob` に含まれる結果を Client へ中継。

### 内部データ構造

* `_clients`：ClientId→接続ストリーム
* `_workers`：WorkerId→接続ストリーム
* `_execQueues`：ExecName→ジョブキュー
* `_inflight`：未ACKジョブのトラッキング（再送用）
* `_pumpTimer`：200msごとに定期チェック（タイムアウト＋スケジューリング）

---

## ⚙️ Worker.cs（ジョブ実行ノード）

Workerは実際にジョブを実行します。

### 処理の流れ

1. **Leaderに接続 & Hello送信**

   ```csharp
   MsgType.HelloWorker, Subject = "job.assign.calcA.*"
   ```

2. **初期クレジット送信**
   → Leaderに「1つジョブを受けられる」と通知。

3. **ジョブ受信 (`AssignJob`)**

   * 受信データを `JobRequest` にデシリアライズ。
   * DedupCache で重複チェック。
   * 実行メソッド `ExecuteJobAsync()` を呼ぶ。

4. **ジョブ実行**

   * `/opt/grid/exe` から実行ファイルをコピー。
   * 入力ファイルを展開。
   * `ProcessStartInfo` で外部プロセス起動。
   * stdout / stderr を収集。
   * `/tmp/jobs/{JobId}` をZIP化して結果生成。

5. **結果送信 (`AckJob`)**

   * `JobResult` をシリアライズして Leader に送信。
   * DedupCache に登録。
   * `Credit` を1増やして再びジョブ受信可能に。

---

## 💻 Client.cs（ジョブ送信側）

1. Leaderに接続し、`HelloClient` を送信。
2. サンプルとして3つのジョブ（calcA）を送信。
3. Leader経由で返ってくる `Result` を待機。
4. 結果をコンソール出力。

---

## 🚀 Program.cs（エントリーポイント）

CLI実行の分岐：

```bash
dotnet run -- leader 5000
dotnet run -- worker 127.0.0.1 5000 job.assign.*
dotnet run -- client 127.0.0.1 5000 clientA
```

* `leader`: ブローカー起動
* `worker`: ワーカー起動
* `client`: クライアント起動

---

## 🔁 全体の通信シーケンス

```
Client                Leader                Worker
  |---HelloClient----->|
  |----SubmitJob------>|
  |                    |---HelloWorker----->|
  |                    |---AssignJob------->|
  |                    |<---AckJob----------|
  |<---Result----------|
```

---

## 📋 まとめ

| 役割                 | 機能                       |
| ------------------ | ------------------------ |
| **Client**         | ジョブを送信し、結果を受信            |
| **Leader**         | ジョブをキューに積み、ワーカーに配分、結果を中継 |
| **Worker**         | ジョブを実行して結果を返す（重複防止付き）    |
| **Codec**          | メッセージのバイナリ変換             |
| **DedupCache**     | ジョブの重複検出                 |
| **SubjectMatcher** | ワイルドカード購読処理              |
| **Leader内部**       | 再送制御・ラウンドロビン割り当て・DLQ処理   |

---

希望があれば、
💡「ジョブ再送のアルゴリズム（指数バックオフとジッター）」や
💡「Workerの実行環境とファイル構成」
などを図解付きでさらに詳しく説明することもできます。
どの部分を深掘りしたいですか？



これらのC#ファイル群は、**簡易的な分散ジョブキューシステム**（メッセージキュー型のジョブ実行環境）を構成しています。
構成は大きく分けて「Leader（スケジューラ）」「Worker（実行ノード）」「Client（ジョブ送信者）」の3役です。
それぞれのファイルの役割を日本語で詳しく解説します。

---

## 🧩 全体構成の概要

```
Client → Leader → Worker
   ↑          ↓
   └── Result ←
```

1. **Client**：ジョブを作成しLeaderに送信、結果を待つ
2. **Leader**：ジョブをキューイングし、空きWorkerに割り当て、結果をClientへ返す
3. **Worker**：ジョブを受け取り実行（exe起動）、結果をLeaderへ返す

通信はすべてTCPで行い、独自の**バイナリフレームプロトコル**を使用しています。

---

## 🧱 `Codec.cs` ― メッセージのエンコード／デコード

Leader・Client・Worker間の通信プロトコルを定義しています。

* 1メッセージの構造は以下のようになっています：

  ```
  [len:4LE][type:1][msgId:16][corrId:16]
  [subjectLen:2LE][subject:utf8]
  [payloadLen:4LE][payload:..]
  ```
* `WriteAsync()`
  `Message`オブジェクトをこの形式でバイト列にして`NetworkStream`へ送信。
* `ReadAsync()`
  ストリームから上記フォーマットの1メッセージを読み取り、`Message`に変換。

→ **通信層の基礎部分**を担当しています。

---

## 🔁 `DedupCache.cs` ― 重複処理防止キャッシュ

* `Guid`（ジョブIDなど）をキーに記録し、TTLを超えた古いものを削除。
* `Worker`が同じジョブを再受信しても、二重実行しないようにします。

---

## ✉️ `Message.cs` ― メッセージ定義

* 通信で使われるメッセージ種別（`MsgType`）を列挙：

  * `SubmitJob`：Client→Leader
  * `AssignJob`：Leader→Worker
  * `AckJob`：Worker→Leader（完了通知）
  * `Result`：Leader→Client（結果）
  * `HelloClient`, `HelloWorker`：接続初期化
* `Message`クラスには`MsgId`, `CorrId`, `Subject`, `Payload`を保持。

---

## 📦 `Models.cs` ― ジョブ関連データモデル

ジョブの内容と結果を定義：

* `JobRequest`：ジョブの要求（JobId, ExecName, 引数, 添付ファイル）
* `JobResult`：実行結果（標準出力、エラー、zip成果物など）
* `InputFile`：ジョブ入力ファイル。

---

## 🎯 `SubjectMatcher.cs` ― トピックパターンマッチ

Workerが購読するジョブ種別を判定します。

* パターン例：

  * `"job.assign.*"` → すべてのジョブを受ける
  * `"job.assign.calcA.*"` → calcA専用ジョブのみ
* `"*"`・`">"`ワイルドカード対応。

---

## 👨‍💼 `Client.cs` ― ジョブ送信クライアント

1. Leaderへ接続し、自分のClientIdを`HelloClient`で送信。
2. `calcA`ジョブを3件送信（例示コード）。
3. 結果(`Result`)を受け取り、内容をコンソール出力。

→ 開発者・ユーザーが使う送信ツール。

---

## 🧠 `Leader.cs` ― 中央スケジューラ

最も複雑で重要なクラスです。
ジョブキュー管理・再送制御・Worker／Client接続管理を行います。

### 主な機能

* **Client接続処理**：`HelloClient`受信後にClientConn生成
* **Worker接続処理**：`HelloWorker`受信後にWorkerConn生成
* **ジョブキュー**：ExecName（例: calcA）ごとにキューを保持
* **スケジューラ**：

  * Workerの空きクレジット(`Credit`)に応じてジョブを割り当て
  * ラウンドロビン方式で公平に配分
* **再送処理**：

  * Ack未受信のジョブを監視
  * Exponential Backoff（指数バックオフ）+ ジッターで再送
  * MaxAttempts超過はDLQ（デッドレターキュー）へ退避
* **結果中継**：

  * Worker→Leader→Client のResult伝達

---

## ⚙️ `Worker.cs` ― ジョブ実行ノード

Leaderから受けたジョブを実際に実行します。

### 動作の流れ

1. `HelloWorker`送信しLeaderに登録。
2. クレジット(`Credit`)を送って「空き1件あります」と伝える。
3. `AssignJob`受信時：

   * `DedupCache`で重複チェック
   * `/opt/grid/exe/<ExecName>.exe` をコピーして実行
   * 標準出力・エラーを取得
   * 結果ファイルをZIP化して`AckJob`としてLeaderへ返送
   * `Credit`を再送して次ジョブ受付可能に。

→ **実ジョブの実行・完了通知担当**。

---

## 🚀 `Program.cs` ― エントリーポイント

コマンドラインで3モード起動を切り替え：

```bash
dotnet run -- leader 5000
dotnet run -- worker 127.0.0.1 5000 job.assign.*
dotnet run -- client 127.0.0.1 5000 clientA
```

* Leaderモード：ポートを開き待受
* Workerモード：指定Leaderへ接続して実行待ち
* Clientモード：ジョブ送信＆結果受信。

---

## 🧾 まとめ

| 役割      | 主なファイル                               | 機能概要                |
| ------- | ------------------------------------ | ------------------- |
| 通信層     | `Codec.cs`                           | メッセージシリアライズ/デシリアライズ |
| モデル     | `Message.cs`, `Models.cs`            | メッセージ・ジョブ定義         |
| ユーティリティ | `DedupCache.cs`, `SubjectMatcher.cs` | 重複除外・パターン判定         |
| Client  | `Client.cs`                          | ジョブ送信＆結果受信          |
| Leader  | `Leader.cs`                          | スケジューリング・再送・結果中継    |
| Worker  | `Worker.cs`                          | 実行・Ack送信            |
| 起動設定    | `Program.cs`                         | CLI起動モード切替          |

---

もし希望があれば、次のような解説資料も作れます：

* 🧠 「Leader.cs」のスケジューリングアルゴリズム詳細図
* 🔌 通信プロトコルのパケットフォーマット図
* ⚙️ 「Worker」の実行ディレクトリ構成図

どれを詳しく知りたいですか？
