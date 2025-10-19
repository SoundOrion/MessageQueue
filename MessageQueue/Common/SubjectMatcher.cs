using System;

namespace MessageQueue.Common;

public static class SubjectMatcher
{
    public static bool Match(string pattern, string subject)
    {
        if (pattern == ">") return true;

        var p = pattern.Split('.', StringSplitOptions.RemoveEmptyEntries);
        var s = subject.Split('.', StringSplitOptions.RemoveEmptyEntries);

        for (int i = 0; i < p.Length; i++)
        {
            if (p[i] == ">") return true;
            if (i >= s.Length) return false;
            if (p[i] == "*") continue;
            if (!p[i].Equals(s[i], StringComparison.OrdinalIgnoreCase)) return false;
        }
        return s.Length == p.Length;
    }
}
