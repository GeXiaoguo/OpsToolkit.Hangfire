using Microsoft.AspNetCore.Http;

namespace OpsToolkit.Hangfire.JobControl;

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

    /// <summary>The Runs dashboard's paged list endpoints' default page size when the caller doesn't specify <c>count</c>.</summary>
    public int RunsDefaultPageSize { get; init; } = 50;

    /// <summary>
    /// Path the built-in Hangfire dashboard is mounted at, used only to build "view in dashboard" links
    /// in the bundled UI. Default <c>/hangfire</c> — override when the host mounts it elsewhere (e.g.
    /// <c>app.UseHangfireDashboard("/admin/jobs")</c>).
    /// </summary>
    public string DashboardPath { get; init; } = "/hangfire";

    /// <summary>
    /// The registrar the host declares its recurring jobs through, when it has adopted schedule
    /// overrides. The override endpoints need it to rebuild each <c>Job</c> from code (the stored
    /// payload is never trusted) and to answer what the code-default cron is. Null (the default) means
    /// the host still registers jobs directly — every Feature-1 control keeps working, and the
    /// override endpoints respond 501 with an explanation instead of being silently absent.
    /// </summary>
    public RecurringJobRegistrar? Registrar { get; init; }

    /// <summary>
    /// Long-running-job liveness configuration (beat throttling, stall-detector scan cadence). Always
    /// present with defaults — see <see cref="LivenessOptions"/> for why there is no null-means-off gate.
    /// </summary>
    public LivenessOptions Liveness { get; init; } = new();
}
