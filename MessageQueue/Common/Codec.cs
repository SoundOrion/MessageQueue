using System.Buffers.Binary;
using System.Net.Sockets;

namespace MessageQueue.Common;

public static class Codec
{
    public static async Task WriteAsync(NetworkStream ns, Message m, CancellationToken ct)
    {
        int bodyLen = 1 + 16 + 16 + 4 + m.Payload.Length;
        var len = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(len, bodyLen);
        await ns.WriteAsync(len, ct);

        await ns.WriteAsync(new[] { (byte)m.Type }, ct);
        await ns.WriteAsync(m.MsgId.ToByteArray(), ct);
        await ns.WriteAsync(m.CorrId.ToByteArray(), ct);

        var pl = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(pl, m.Payload.Length);
        await ns.WriteAsync(pl, ct);

        if (m.Payload.Length > 0)
            await ns.WriteAsync(m.Payload, ct);
    }

    public static async Task<Message?> ReadAsync(NetworkStream ns, CancellationToken ct)
    {
        var lenBuf = await ReadExactAsync(ns, 4, ct);
        if (lenBuf is null) return null;
        int bodyLen = BinaryPrimitives.ReadInt32LittleEndian(lenBuf);

        var body = await ReadExactAsync(ns, bodyLen, ct);
        if (body is null) return null;

        var span = body.AsSpan();
        var type = (MsgType)span[0];
        var msgId = new Guid(span.Slice(1, 16));
        var corr = new Guid(span.Slice(17, 16));
        int payloadLen = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(33, 4));
        var payload = payloadLen == 0 ? Array.Empty<byte>() : span.Slice(37, payloadLen).ToArray();

        return new Message { Type = type, MsgId = msgId, CorrId = corr, Payload = payload };
    }

    private static async Task<byte[]?> ReadExactAsync(NetworkStream ns, int n, CancellationToken ct)
    {
        byte[] buf = new byte[n];
        int read = 0;
        while (read < n)
        {
            int r = await ns.ReadAsync(buf.AsMemory(read, n - read), ct);
            if (r <= 0) return null;
            read += r;
        }
        return buf;
    }
}
