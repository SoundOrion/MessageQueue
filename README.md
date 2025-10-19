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
