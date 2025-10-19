using System.Collections.Concurrent;

namespace MessageQueue.Common;

public sealed class DedupCache
{
    private readonly ConcurrentDictionary<Guid, long> _table = new();
    private readonly TimeSpan _ttl;
    public DedupCache(TimeSpan ttl) => _ttl = ttl;

    public bool TryAdd(Guid id)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return _table.TryAdd(id, now);
    }

    public bool Contains(Guid id) => _table.ContainsKey(id);

    // 時々呼ぶ
    public void Sweep()
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        foreach (var kv in _table)
        {
            if (now - kv.Value > _ttl.TotalMilliseconds)
                _table.TryRemove(kv.Key, out _);
        }
    }
}


