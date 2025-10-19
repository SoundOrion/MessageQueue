# README

分散ジョブ実行の最小フレームワーク（Leader–Worker–Client 構成）。
**TCP 長さ付きフレーミング**で安全にメッセージをやり取りし、**クレジット制御（受け入れ枠）**と**再送/タイムアウト/簡易DLQ**で、シンプルながら実運用に近い挙動を再現します。
また、**ClientMulti** により **複数の Leader（= グループ）へ同時接続して横断分散投入**と**障害時フェイルオーバ**を実現します。
（Windows サービス化を想定しても、1 プロセス内で複数ジョブの並行実行が可能）

---

## 目次

* [アーキテクチャ](#アーキテクチャ)
* [プロトコル / フレーミング](#プロトコル--フレーミング)
* [各ソースコードの役割](#各ソースコードの役割)
* [動作概要（メッセージフロー）](#動作概要メッセージフロー)
* [障害時の挙動 / フェイルオーバ](#障害時の挙動--フェイルオーバ)
* [スロットリング / 背圧](#スロットリング--背圧)
* [制限・セキュリティ](#制限-セキュリティ)
* [ビルドと実行](#ビルドと実行)
* [マルチグループ分散投入（ClientMulti）](#マルチグループ分散投入clientmulti)
* [設定（環境変数）](#設定環境変数)
* [今後の拡張TODO](#今後の拡張todo)

---

## アーキテクチャ

```
Client / ClientMulti
    │ SubmitJob / HelloClient
    ▼
Leader  ──────→  Worker (N台)
  └─ Result → Client  （結果を中継）
```

* **Leader**: クライアントからのジョブ受付、ワーカーへの割当、ACK/再送、簡易DLQ、クライアント別 in-flight 制御。 
* **Worker**: `ExecName.exe` をジョブごとに独立ディレクトリで起動。並列上限・CPUスロットル・重複排除（プロセス内）。完了時に `AckJob` と結果を返送。 
* **Client**: 単一 Leader へ送信する基本クライアント。希望並列（cap）を名乗る。 
* **ClientMulti**: 複数 Leader に同時接続し、ラウンドロビン/負荷ベースで横断分散投入。接続断後の**猶予（grace）**経過で in-flight 回収→他接続へ再送、指数バックオフ再接続。 

---

## プロトコル / フレーミング

### フレーム構造

* `[len:4LE] [type:1] [msgId:16] [corrId:16] [subjectLen:2LE] [subject:utf8] [payloadLen:4LE] [payload]`
* 4MB 上限で異常サイズを拒否。読み取りは **ReadExact** で境界を厳守（混線防止）。 

### メッセージ種別（抜粋）

* `HelloClient` / `HelloWorker` — 参加宣言（Client は cap、Worker はサブジェクトパターンを伝達）
* `SubmitJob` — Client→Leader
* `AssignJob` — Leader→Worker
* `AckJob` — Worker→Leader（結果ペイロード）
* `Credit` — Worker→Leader（受け入れ枠増）
* `Result` — Leader→Client（結果中継） 

### モデル

* `JobRequest(JobId, ClientId, ExecName, Args, Files)`
* `JobResult(JobId, ClientId, ExecName, Status, Stdout, Stderr, OutputArchive?)`
* `ClientConfig(ClientId, DesiredParallelism)`（cap 宣言） 

---

## 各ソースコードの役割

### `Codec.cs` — バイナリフレーミング I/O

* 長さ付きの安全なフレーム書き込み/読み取り、4MB まで。
* `ReadExactAsync` で指定バイト数を必ず読み切る。 

### `Message.cs` — メッセージと種別

* メッセージ種別の定義と、`Message` コンテナ。相関ID で ACK/結果をひも付け。 

### `Models.cs` — ジョブ/結果/設定モデル

* `JobRequest` / `JobResult` / `ClientConfig` を JSON でシリアライズしてペイロードに格納。 

### `SubjectMatcher.cs` — NATS 風パターンマッチ

* `job.assign.<exec>` とワイルドカード (`*`, `>`) でルーティング。 

### `DedupCache.cs` — Worker 内の重複実行防止

* プロセス内 TTL つきの JobId キャッシュで再実行を弾く。Sweep あり。 

### `Leader.cs` — ジョブ受付・割当・再送・DLQ・cap/in-flight 制御

* Client/Worker 接続処理、キュー管理（Exec ごと）、ラウンドロビンで割当。
* **Credit 制御**（Worker 申告の受け入れ枠）、**再送（指数バックオフ）**, **DLQ**, **クライアント別 in-flight** を持つ。
* `HelloClient` で **cap** を受領し `_clientCap` に保持。実割当時は `_clientInflight` を増減し上限を尊重。 

### `Worker.cs` — 実行エージェント

* 並列上限（`WORKER_MAX_PAR`）、CPU スロットル（`WORKER_CPU_CAP`）。
* ジョブごとに `/tmp/jobs/<JobId>` 作業ディレクトリを作成し、`ExecName.exe` をコピーして起動。標準出力/標準エラー取得。
* 完了後 ZIP（成果物）を作成して `AckJob` で返送。クレジット加算は **CPU 使用率が閾値以下になってから**返す。送信は `_sendLock` で直列化。 

### `Client.cs` — 単一 Leader へ投げる最小クライアント

* 起動時に `HelloClient(cap)` を送り、サンプルとして 3 ジョブを `calcA` に投入して結果を受信。 

### `ClientMulti.cs` — 複数 Leader に横断投入 & フェイルオーバ

* 複数エンドポイントへ同時接続し、**ラウンドロビン or in-flight 最小**で送信先を選択。
* 送信直前に実ソケットへ書き込み（失敗＝**未送信扱い**でペンディングへ）→生存接続が復活したら再送。
* **in-flight テーブル**で送信済みジョブを管理し、**むやみに二重送信しない**。
* 接続断後 **Grace=10s** 経過で、その接続に属する in-flight を回収→他接続へ再送（**同じ JobId を維持**）。
* 自動再接続は指数バックオフ（±ジッタ）。 

### `Program.cs` — エントリポイント

* `leader <port>` / `worker <host> <port> [pattern]` / `client <host> <port> <clientId> [desired]`
* **`clientx <clientId> <host1:port1> <host2:port2> ... [desiredTotal]`**（複数 Leader へ投入）をサポート。 

---

## 動作概要（メッセージフロー）

1. **Client(または ClientMulti)** が接続し **HelloClient** で `ClientId` と希望並列 `cap` を宣言。Leader は `_clientCap` に記録。  
2. **Worker** は **HelloWorker**（購読パターン）→ 初期 **Credit = 並列上限** を通知。 
3. Client から **SubmitJob**（`job.submit.<exec>`）。Leader は exec ごとのキューに入れ、**ラウンドロビン**で割当候補を回す。 
4. Leader は **Client の cap と in-flight** を見つつ、**Credit>0** の Worker に **AssignJob**。**ACK タイムアウト**をセットし、in-flight に記録。 
5. Worker はプロセス起動・完了後に **AckJob**（結果ペイロード）→ Leader は **Result** を Client に中継し、in-flight を減算。Worker は CPU 閾値を満たしたら **Credit+1**。  

---

## 障害時の挙動 / フェイルオーバ

* **Leader 側**

  * ACK タイムアウトで **指数バックオフ再送**、最大試行超過で **DLQ**。所有 Worker が落ちた場合は in-flight を回収して再キュー。 
* **ClientMulti 側**

  * 送信直前エラーは **未送信扱い**で保留 → 生存接続へ自動振替。
  * 送信済みは **in-flight テーブル**で二重送信を抑止。
  * **接続断 + Grace 経過**で当該 in-flight を回収し、**同じ JobId のまま**他接続へ再送。
  * 接続は **指数バックオフ + ジッタ**で再確立。 

---

## スロットリング / 背圧

* **Worker → Leader の Credit 制御**

  * Worker は「いま空いている受け入れ枠」を Credit として申告。Leader は Credit を消費しながら Assign する。ペイロードは `int32LE`。過大/不正値は防御。 
* **Client の cap（希望並列）**

  * Leader は Client ごとに in-flight をカウントし、cap 超過は割当を保留。 
* **Worker 側 CPU スロットル**

  * 完了後すぐに Credit+1 せず、CPU 使用率がしきい値未満になるまで少し待ってから返す。バースト抑制。 

---

## 制限・セキュリティ

* **メッセージ最大 4MB**（フレームごと）— 大きな成果物は ZIP を返さず、外部ストレージ参照に切り替える設計にしやすい。 
* **危険パス防止**（`..`, `/`, `\` を拒否）— Worker の入力展開時に検査。 
* **DedupCache** は Worker **プロセス内**の重複防止。**グループ間**の重複は Client 側（ClientMulti の in-flight）で抑止。  
* **TLS/認可は未実装**：必要に応じて `SslStream` で暗号化、ClientId/exec ごとに ACL を導入してください。

---

## ビルドと実行

### 1) Leader / Worker / Client（単一グループ）

```bash
# Leader
dotnet run -- leader 5000

# Worker（パターンはワイルドカード可）
dotnet run -- worker 127.0.0.1 5000 job.assign.*

# Client（希望並列 cap を 8 として名乗る例）
dotnet run -- client 127.0.0.1 5000 clientA 8
```

* 起動オプションは `Program.cs` を参照。`client` は `HelloClient` で cap を宣言します。  

### 2) 複数グループ（複数 Leader）＋ ClientMulti

```bash
# Leaders
dotnet run -- leader 5000
dotnet run -- leader 5001
dotnet run -- leader 5002

# Workers（各グループへ接続）
dotnet run -- worker 127.0.0.1 5000 job.assign.*
dotnet run -- worker 127.0.0.1 5001 job.assign.*
dotnet run -- worker 127.0.0.1 5002 job.assign.*

# ClientMulti：総cap=12 を均等配分して 3 グループへ横断投入
dotnet run -- clientx clientA 127.0.0.1:5000 127.0.0.1:5001 127.0.0.1:5002 12
```

* `clientx` は複数 Leader へ同時接続。cap は総量を均等割で各 Leader に宣言。  

---

## マルチグループ分散投入（ClientMulti）

**主な機能**（デフォルト推奨値で実装済み）:

* **ポリシー**：ラウンドロビン（必要なら in-flight 最小へ変更可）
* **未送信振替**：送信直前エラーは pending に退避→生存接続に自動再送
* **二重送信防止**：in-flight テーブルで送信済みを管理
* **明示フェイルオーバ**：接続断＋**Grace=10s** で in-flight を回収→同じ JobId を他接続へ再送
* **再接続**：指数バックオフ（0.5→1→2→…最大 30s、±20% ジッタ） 

> 注意：Worker の `DedupCache` はプロセス内のみ。**グループ間の重複は Client 側の in-flight/フェイルオーバ制御で抑止**してください。  

---

## 設定（環境変数）

* `CLIENT_DESIRED_PAR` — `client` の cap 既定値（未指定時は 4）。起動引数が優先。 
* `WORKER_MAX_PAR` — Worker の並列上限（既定 4）。 
* `WORKER_CPU_CAP` — Worker の CPU 閾値（0.10〜0.95、既定 0.75）。 

---

## 今後の拡張TODO

* **TLS（`SslStream`）/ 認可**（ClientId/exec ごとの ACL）
* **Leader 状態の永続化**（in-flight / DLQ / キュー）
* **メトリクス**（in-flight, 再送回数, DLQ 件数, p95 実行時間など）
* **成果物外部化**（4MB 超の ZIP はオブジェクトストレージ参照にする）
* **Router（ゲートウェイ）**：Client を単一接続にし、前段でポリシー制御

---

## 付録：実行ファイル配置と実行の流れ（Worker）

* 実行対象は `ExeDir`（既定 `/opt/grid/exe`）に `<ExecName>.exe` を配置。
* 受け取ったジョブは `/tmp/jobs/<JobId>` に展開し、その中に EXE をコピー＆実行。
* 結果 ZIP と stdout/stderr を `AckJob` で返送。失敗時は `"FAILED"`。 

---

**ライセンス/注意**: 本コードは学習・試験目的のプロトタイプ設計です。実運用では TLS・認可・永続化・監視の導入、ならびに成果物サイズの考慮を推奨します。
