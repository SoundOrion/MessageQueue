namespace MessageQueue.Common;

//[length:4B LE][type:1B][msgId:16B][corrId:16B][payloadLen:4B LE][payload:payloadLen]


public enum MsgType : byte
{
    SubmitJob = 1, // Client -> Leader
    AssignJob = 2, // Leader -> Worker
    AckJob = 3, // Worker -> Leader (完了通知/ACK)
    Credit = 4, // Worker -> Leader (受け入れ枠増)
    Result = 5, // Leader -> Client（Workerから受け取った結果を中継）
    HelloClient = 9, // Client -> Leader（ClientId を名乗る）
    HelloWorker = 10 // Worker -> Leader（購読パターンを名乗る）
}

public sealed class Message
{
    public MsgType Type { get; init; }
    public Guid MsgId { get; init; }                  // JobId / WorkerId / arbitrary
    public Guid CorrId { get; init; }                 // 相関（Ack など）
    public string Subject { get; init; } = string.Empty; // ルーティング（"group" を置く）
    public byte[] Payload { get; init; } = Array.Empty<byte>();
}

