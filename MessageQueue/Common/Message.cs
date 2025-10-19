namespace MessageQueue.Common;

//[length:4B LE][type:1B][msgId:16B][corrId:16B][payloadLen:4B LE][payload:payloadLen]

public enum MsgType : byte
{
    // --- 既存メッセージ ---
    SubmitJob = 1,  // Client -> Leader
    AssignJob = 2,  // Leader -> Worker
    AckJob = 3,  // Worker -> Leader (完了通知/ACK)
    Credit = 4,  // Worker -> Leader (受け入れ枠増)
    Result = 5,  // Leader -> Client（Workerから受け取った結果を中継）
    HelloClient = 9,  // Client -> Leader（ClientId を名乗る）
    HelloWorker = 10, // Worker -> Leader（購読パターンを名乗る）

    // --- 追加: クラスタ / Raft-lite 用 ---
    NotLeader = 20, // Follower -> Client/Worker（リダイレクト案内）
    LeaderHello = 21, // ノード間握手（必要なら使用）
    AppendEntries = 22, // Leader -> Follower（ログ複製／ハートビート）
    AppendResp = 23, // Follower -> Leader（複製結果）
    RequestVote = 24, // Candidate -> Peer（投票依頼）
    VoteResp = 25, // Peer -> Candidate（投票応答）
}

public sealed class Message
{
    public MsgType Type { get; init; }
    public Guid MsgId { get; init; }                     // JobId / WorkerId / arbitrary
    public Guid CorrId { get; init; }                    // 相関（Ack など）
    public string Subject { get; init; } = string.Empty; // ルーティング（"group" など）
    public byte[] Payload { get; init; } = Array.Empty<byte>();
}
