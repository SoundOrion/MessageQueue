using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MessageQueue.Common;

public static class SubjectMatcher
{
    public static bool Match(string pattern, string subject)
    {
        if (pattern == ">") return true;

        var pSegs = pattern.Split('.', StringSplitOptions.RemoveEmptyEntries);
        var sSegs = subject.Split('.', StringSplitOptions.RemoveEmptyEntries);

        for (int i = 0; i < pSegs.Length; i++)
        {
            if (pSegs[i] == ">")
                return true; // 以降すべてOK

            if (i >= sSegs.Length)
                return false; // subjectが短すぎる

            if (pSegs[i] == "*")
                continue; // 任意セグメントOK

            if (!pSegs[i].Equals(sSegs[i], StringComparison.OrdinalIgnoreCase))
                return false;
        }

        return sSegs.Length == pSegs.Length;
    }
}
