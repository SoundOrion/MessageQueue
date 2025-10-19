using System;
using System.Collections.Concurrent;

namespace MessageQueue.Common;

public sealed class DedupCache
{
    private readonly ConcurrentDictionary<Guid, long> _seen = new();
    private readonly TimeSpan _ttl;
    public DedupCache(TimeSpan ttl) => _ttl = ttl;

    public bool Contains(Guid id) => _seen.ContainsKey(id);
    public bool TryAdd(Guid id)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return _seen.TryAdd(id, now);
    }

    public void Sweep()
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        foreach (var kv in _seen)
            if (now - kv.Value > _ttl.TotalMilliseconds)
                _seen.TryRemove(kv.Key, out _);
    }
}
