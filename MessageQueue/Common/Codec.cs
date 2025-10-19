using System.Buffers.Binary;
using System.Net.Sockets;

namespace MessageQueue.Common;

public static class Codec
{
    public static async Task WriteAsync(NetworkStream ns, Message m, CancellationToken ct)
    {
        int bodyLen = 1 + 4 + m.Payload.Length; // type(1) + payloadLen(4) + payload
        var len = new byte[4];

        BinaryPrimitives.WriteInt32LittleEndian(len, bodyLen);
        await ns.WriteAsync(len, ct);
        await ns.WriteAsync(new[] { (byte)m.Type }, ct);

        var pl = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(pl, m.Payload.Length);
        await ns.WriteAsync(pl, ct);

        if (m.Payload.Length > 0)
            await ns.WriteAsync(m.Payload, ct);
    }

    public static async Task<Message?> ReadAsync(NetworkStream ns, CancellationToken ct)
    {
        // [length:4]
        var lenBuf = await ReadExactAsync(ns, 4, ct);
        if (lenBuf is null) return null; // ソケット終了
        int bodyLen = BinaryPrimitives.ReadInt32LittleEndian(lenBuf);

        // 本文
        var body = await ReadExactAsync(ns, bodyLen, ct);
        if (body is null) return null;

        var span = body.AsSpan();
        var type = (MsgType)span[0];
        int payloadLen = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(1, 4));
        var payload = payloadLen == 0 ? Array.Empty<byte>() : span.Slice(5, payloadLen).ToArray();

        return new Message { Type = type, Payload = payload };
    }

    private static async Task<byte[]?> ReadExactAsync(NetworkStream ns, int n, CancellationToken ct)
    {
        byte[] buf = new byte[n];
        int read = 0;
        while (read < n)
        {
            int r = await ns.ReadAsync(buf.AsMemory(read, n - read), ct);
            if (r <= 0) return null; // 切断
            read += r;
        }
        return buf;
    }
}

