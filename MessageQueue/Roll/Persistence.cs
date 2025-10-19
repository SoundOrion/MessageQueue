// /Persistence.cs
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace MessageQueue.Roll;

public sealed class Persistence : IAsyncDisposable
{
    readonly string _root;
    readonly string _walPath;
    FileStream _wal;
    readonly JsonSerializerOptions _jso = new(JsonSerializerDefaults.Web);

    public Persistence(string rootDir = "state")
    {
        _root = rootDir;
        Directory.CreateDirectory(_root);
        _walPath = Path.Combine(_root, "events.log");
        _wal = new FileStream(_walPath, FileMode.Append, FileAccess.Write, FileShare.Read, 4096, FileOptions.Asynchronous);
    }

    public async Task AppendAsync(object record, bool durable = true, CancellationToken ct = default)
    {
        var json = JsonSerializer.Serialize(record, _jso);
        var bytes = Encoding.UTF8.GetBytes(json + "\n");
        await _wal.WriteAsync(bytes, 0, bytes.Length, ct);
        if (durable) await _wal.FlushAsync(ct); // Flush(true) 同等（必要ならfsyncを検討）
    }

    public async Task SnapshotAsync(LeaderStateSnapshot snap, CancellationToken ct = default)
    {
        var dir = Path.Combine(_root, "snapshots");
        Directory.CreateDirectory(dir);
        var path = Path.Combine(dir, $"state-{DateTime.UtcNow:yyyyMMdd-HHmmss}.json");
        await File.WriteAllTextAsync(path, JsonSerializer.Serialize(snap, _jso), ct);
    }

    public async Task<(LeaderStateSnapshot? snapshot, IEnumerable<string> walLines)> LoadAsync(CancellationToken ct = default)
    {
        LeaderStateSnapshot? snap = null;

        var dir = Path.Combine(_root, "snapshots");
        if (Directory.Exists(dir))
        {
            string? last = null;
            foreach (var f in Directory.GetFiles(dir, "state-*.json"))
                if (last is null || string.Compare(f, last, StringComparison.Ordinal) > 0) last = f;
            if (last != null)
            {
                var json = await File.ReadAllTextAsync(last, ct);
                snap = JsonSerializer.Deserialize<LeaderStateSnapshot>(json, _jso);
            }
        }

        var lines = File.Exists(_walPath) ? File.ReadLines(_walPath) : Array.Empty<string>();
        return (snap, lines);
    }

    public async ValueTask DisposeAsync()
    {
        try { await _wal.FlushAsync(); } catch { }
        _wal.Dispose();
    }
}

// ---- スナップショット用DTO ----
public sealed class LeaderStateSnapshot
{
    public Dictionary<string, List<JobWire>> Queues { get; init; } = new(); // exec -> jobs
    public Dictionary<Guid, InflightWire> Inflight { get; init; } = new();  // jobId -> inflight
    public List<JobWire> Dlq { get; init; } = new();
    public Dictionary<string, int> ClientCap { get; init; } = new();
    public Dictionary<string, int> ClientInflight { get; init; } = new();
}

public sealed record JobWire(Guid JobId, string ClientId, string ExecName, byte[] RawPayload);
public sealed record InflightWire(JobWire Job, Guid? OwnerWorker, DateTime DueAt, TimeSpan Timeout, int Attempt);

// ---- WALレコード ----
public sealed record WalEnqueue(string type, DateTime at, JobWire job);
public sealed record WalAssign(string type, DateTime at, Guid jobId, Guid? workerId, int attempt);
public sealed record WalAck(string type, DateTime at, Guid jobId, string clientId);
public sealed record WalTimeoutRequeue(string type, DateTime at, Guid jobId, int attempt);
public sealed record WalDlq(string type, DateTime at, Guid jobId);
public sealed record WalWorkerDownRequeue(string type, DateTime at, Guid jobId, Guid workerId);
