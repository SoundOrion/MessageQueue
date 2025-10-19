namespace MessageQueue.Common;

//[length:4B LE][type:1B][msgId:16B][corrId:16B][payloadLen:4B LE][payload:payloadLen]

public enum MsgType : byte
{
    SubmitJob = 1,
    AssignJob = 2,
    AckJob = 3,
    Credit = 4,
    HelloClient = 9,
    HelloWorker = 10,
}

public sealed class Message
{
    public MsgType Type { get; init; }
    public Guid MsgId { get; init; }       // 送信側が発行（Assign=JobId / Submit=JobId）
    public Guid CorrId { get; init; }      // 応答相関（Ackは受け取ったMsgのMsgIdを入れる）
    public byte[] Payload { get; init; } = Array.Empty<byte>();
}

