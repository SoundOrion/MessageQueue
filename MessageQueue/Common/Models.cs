using System;
using System.Collections.Generic;

namespace MessageQueue.Common;

public record InputFile(string Name, string? CacheId, byte[]? Content);
public record JobRequest(Guid JobId, string ClientId, string ExecName, List<string> Args, List<InputFile> Files);
public record JobResult(Guid JobId, string ClientId, string ExecName, string Status, string Stdout, string Stderr, byte[]? OutputArchive);
