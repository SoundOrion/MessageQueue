using System;
using System.Buffers.Binary;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MessageQueue.Common;

// Frame: [len:4LE]
//        [type:1][msgId:16][corrId:16]
//        [subjectLen:2LE][subject:utf8]
//        [payloadLen:4LE][payload:..]
public static class Codec
{
    public static async Task WriteAsync(NetworkStream ns, Message m, CancellationToken ct)
    {
        var subBytes = Encoding.UTF8.GetBytes(m.Subject ?? "");
        int bodyLen = 1 + 16 + 16 + 2 + subBytes.Length + 4 + m.Payload.Length;

        var len = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(len, bodyLen);
        await ns.WriteAsync(len, ct);

        await ns.WriteAsync(new[] { (byte)m.Type }, ct);
        await ns.WriteAsync(m.MsgId.ToByteArray(), ct);
        await ns.WriteAsync(m.CorrId.ToByteArray(), ct);

        var sbl = new byte[2];
        BinaryPrimitives.WriteInt16LittleEndian(sbl, (short)subBytes.Length);
        await ns.WriteAsync(sbl, ct);
        if (subBytes.Length > 0) await ns.WriteAsync(subBytes, ct);

        var pbl = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(pbl, m.Payload.Length);
        await ns.WriteAsync(pbl, ct);
        if (m.Payload.Length > 0) await ns.WriteAsync(m.Payload, ct);
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

        int ofs = 33;
        int subLen = BinaryPrimitives.ReadInt16LittleEndian(span.Slice(ofs, 2)); ofs += 2;
        string subject = subLen == 0 ? "" : Encoding.UTF8.GetString(span.Slice(ofs, subLen)); ofs += subLen;

        int payloadLen = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(ofs, 4)); ofs += 4;
        byte[] payload = payloadLen == 0 ? Array.Empty<byte>() : span.Slice(ofs, payloadLen).ToArray();

        return new Message { Type = type, MsgId = msgId, CorrId = corr, Subject = subject, Payload = payload };
    }

    private static async Task<byte[]?> ReadExactAsync(NetworkStream ns, int n, CancellationToken ct)
    {
        var buf = new byte[n]; int read = 0;
        while (read < n)
        {
            int r = await ns.ReadAsync(buf.AsMemory(read, n - read), ct);
            if (r <= 0) return null;
            read += r;
        }
        return buf;
    }
}
