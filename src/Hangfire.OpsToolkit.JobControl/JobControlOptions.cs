using Microsoft.AspNetCore.Http;

namespace Hangfire.OpsToolkit.JobControl;

/// <summary>
/// Optional configuration for <see cref="JobControlEndpoints.MapJobControl"/> /
/// <see cref="JobControlEndpoints.MapJobControlApi"/>. Audit makes actor identity load-bearing (it is
/// recorded, not just used for authorization), so extraction becomes configurable here rather than
/// staying a hardcoded <c>Identity?.Name</c> read.
/// </summary>
public sealed record JobControlOptions
{
    /// <summary>
    /// Extracts the audit actor from the request. Default: <c>HttpContext.User.Identity?.Name ?? "unknown"</c>.
    /// Hosts whose principal carries identity elsewhere (e.g. an email claim) configure this instead.
    /// </summary>
    public Func<HttpContext, string>? ActorProvider { get; init; }

    /// <summary>Count cap on the audit list (retention). Default 10,000 — years of history at human-action volume.</summary>
    public int AuditMaxEntries { get; init; } = 10_000;

    /// <summary><c>GET /audit</c>'s default row limit when the caller doesn't specify one.</summary>
    public int AuditDefaultReadLimit { get; init; } = 200;
}
