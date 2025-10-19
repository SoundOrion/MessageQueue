namespace MessageQueue.Common;

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
    public byte[] Payload { get; init; } = Array.Empty<byte>();
}
