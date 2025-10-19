namespace MessageQueue.Common;

//[length:4B LE][type:1B][msgId:16B][corrId:16B][payloadLen:4B LE][payload:payloadLen]


public enum MsgType : byte
{
    SubmitJob = 1,   // Client -> Leader
    AssignJob = 2,   // Leader -> Worker
    AckJob = 3,   // Worker -> Leader
    Credit = 4,   // Worker -> Leader (受け入れ枠増)
    HelloClient = 9,   // Client -> Leader (役割宣言)
    HelloWorker = 10,  // Worker -> Leader (役割宣言: MsgId=workerId, Subject=group)
}

public sealed class Message
{
    public MsgType Type { get; init; }
    public Guid MsgId { get; init; }                  // JobId / WorkerId / arbitrary
    public Guid CorrId { get; init; }                 // 相関（Ack など）
    public string Subject { get; init; } = string.Empty; // ルーティング（"group" を置く）
    public byte[] Payload { get; init; } = Array.Empty<byte>();
}

