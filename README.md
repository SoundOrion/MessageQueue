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
