using Hangfire;
using Hangfire.Common;
using Hangfire.Storage;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace Hangfire.OpsToolkit.JobControl;

public sealed record DisableRecurringJobRequest(string? Reason);

/// <summary>
/// Superset of the built-in dashboard's Recurring Jobs page columns (Id, Cron, TimeZone, Job,
/// NextExecution, LastExecution, Created — see Hangfire.Core's RecurringJobsPage.cshtml) plus
/// <see cref="DisableStatus"/>. A host adopting this page in place of the built-in one loses no
/// information from it.
/// </summary>
public sealed record RecurringJobSummary(
    string Id,
    string Cron,
    string? TimeZoneId,
    string? JobDisplayName,
    DateTime? NextExecution,
    DateTime? LastExecution,
    string? LastJobId,
    string? LastJobState,
    DateTime? CreatedAt,
    string? Error,
    RecurringJobDisableStatus? DisableStatus);

/// <summary>
/// The API's two route groups, returned so a host can chain additional endpoint metadata (rate
/// limiting, CORS, OpenAPI) beyond the authorization policies already applied at group level.
/// </summary>
public sealed record JobControlApiGroups(RouteGroupBuilder View, RouteGroupBuilder Manage);

/// <summary>
/// HTTP plane of job control: the list/disable/enable API and the bundled operator UI (a single
/// self-contained HTML file embedded in this assembly — vanilla JS, no build step, no external asset
/// requests, no static-file hosting required of the host). The server plane (the filter that skips
/// disabled jobs) is registered separately via
/// <see cref="GlobalConfigurationExtensions.UseJobControl"/>.
///
/// Authorization is taken as required parameters and applied at the route-group level — fail-secure by
/// construction (mapping the API without policies is a compile error, not an anonymous endpoint), and
/// endpoints added by future versions of this package inherit the correct gate without host changes.
/// </summary>
public static class JobControlEndpoints
{
    // Nested under /hangfire deliberately: hosts commonly convert a browser session into the
    // Authorization header only for paths under their dashboard's own mount point. A sibling path like
    // /api/hangfire/recurring looks equally sensible but can silently 401 from the browser despite a
    // valid session, if that conversion is scoped to /hangfire only.
    public const string DefaultApiBase = "/hangfire/api/recurring";
    public const string DefaultUiPath = "/hangfire/job-control";

    private const string UiResourceName = "Hangfire.OpsToolkit.JobControl.wwwroot.job-control.html";
    private const string ApiBasePlaceholder = "{{API_BASE}}";

    // GET /audit request cap — independent of JobControlOptions.AuditDefaultReadLimit (the default when
    // a caller doesn't specify one); this bounds what a caller CAN ask for even when specifying a limit.
    private const int AuditReadLimitHardCap = 1000;

    /// <summary>
    /// One-call integration: maps the API and the bundled UI. <paramref name="viewPolicy"/> gates reads
    /// (job list + audit history + the UI page); <paramref name="managePolicy"/> gates mutations
    /// (disable/enable/trigger/delete — each of which is audited, see <see cref="AuditStore"/>).
    /// </summary>
    /// <remarks>
    /// Behavior change: <c>/{jobId}/delete</c> now returns 404 for an unknown id (previously 200
    /// unconditionally) — a false-success shouldn't reach the audit record. Pre-1.0, so not versioned
    /// as a breaking change, but worth flagging to callers that inspected the old status code.
    /// </remarks>
    public static void MapJobControl(
        this IEndpointRouteBuilder endpoints,
        string viewPolicy,
        string managePolicy,
        string uiPath = DefaultUiPath,
        string apiBase = DefaultApiBase,
        JobControlOptions? options = null)
    {
        endpoints.MapJobControlApi(viewPolicy, managePolicy, apiBase, options);
        endpoints.MapJobControlUi(uiPath, apiBase).RequireAuthorization(viewPolicy);
    }

    /// <summary>API only — for hosts that bring their own frontend.</summary>
    public static JobControlApiGroups MapJobControlApi(
        this IEndpointRouteBuilder endpoints,
        string viewPolicy,
        string managePolicy,
        string apiBase,
        JobControlOptions? options = null)
    {
        var jobControlOptions = options ?? new JobControlOptions();
        var view = endpoints.MapGroup(apiBase).RequireAuthorization(viewPolicy);
        var manage = endpoints.MapGroup(apiBase).RequireAuthorization(managePolicy);

        view.MapGet("", () =>
        {
            using var connection = JobStorage.Current.GetConnection();
            var jobs = connection.GetRecurringJobs()
                .Select(job => new RecurringJobSummary(
                    job.Id,
                    job.Cron,
                    job.TimeZoneId,
                    jobDisplayName(job),
                    job.NextExecution,
                    job.LastExecution,
                    job.LastJobId,
                    job.LastJobState,
                    job.CreatedAt,
                    job.Error,
                    RecurringJobDisableStore.GetStatus(connection, job.Id)))
                .ToList();
            return Results.Ok(jobs);
        });

        // Skipped occurrences while disabled are never backfilled, so a required reason makes the
        // operator say why before that (irreversible) side effect starts happening.
        manage.MapPost("/{jobId}/disable", (string jobId, DisableRecurringJobRequest request, HttpContext http) =>
        {
            if (string.IsNullOrWhiteSpace(request.Reason))
                return Results.BadRequest("A reason is required to disable a job.");

            var who = actor(http, jobControlOptions);
            var auditEntry = new AuditEntry(
                AuditEntry.CurrentVersion, DateTime.UtcNow, who, "disable", jobId, request.Reason, "ok", Detail: null);

            var found = RecurringJobDisableStore.SetDisabled(
                JobStorage.Current, jobId, disabled: true, who, request.Reason, auditEntry.At,
                auditEntry, jobControlOptions.AuditMaxEntries);

            if (!found)
            {
                AuditStore.Append(JobStorage.Current, auditEntry with { Outcome = "not-found" }, jobControlOptions.AuditMaxEntries);
                return Results.NotFound(jobId);
            }
            return Results.Ok();
        });

        manage.MapPost("/{jobId}/enable", (string jobId, DisableRecurringJobRequest? request, HttpContext http) =>
        {
            var who = actor(http, jobControlOptions);
            var auditEntry = new AuditEntry(
                AuditEntry.CurrentVersion, DateTime.UtcNow, who, "enable", jobId, request?.Reason, "ok", Detail: null);

            var found = RecurringJobDisableStore.SetDisabled(
                JobStorage.Current, jobId, disabled: false, who, request?.Reason, auditEntry.At,
                auditEntry, jobControlOptions.AuditMaxEntries);

            if (!found)
            {
                AuditStore.Append(JobStorage.Current, auditEntry with { Outcome = "not-found" }, jobControlOptions.AuditMaxEntries);
                return Results.NotFound(jobId);
            }
            return Results.Ok();
        });

        // Parity with the built-in dashboard's Trigger-now / Delete — implemented natively (not by
        // proxying to Hangfire's own /recurring/trigger|remove routes) so they sit behind managePolicy
        // like disable/enable, rather than inheriting whatever policy a host happened to put on
        // MapHangfireDashboard.
        manage.MapPost("/{jobId}/trigger", (string jobId, HttpContext http) =>
        {
            var who = actor(http, jobControlOptions);
            var triggeredJobId = new RecurringJobManager(JobStorage.Current).TriggerJob(jobId);

            // The correlation seed that later lets a background-job audit trail tie this human action
            // to the execution it caused.
            var detail = triggeredJobId != null
                ? new Dictionary<string, string> { ["BackgroundJobId"] = triggeredJobId }
                : null;
            AuditStore.Append(JobStorage.Current, new AuditEntry(
                AuditEntry.CurrentVersion, DateTime.UtcNow, who, "trigger", jobId, Reason: null,
                triggeredJobId != null ? "ok" : "not-found", detail), jobControlOptions.AuditMaxEntries);

            return triggeredJobId != null ? Results.Ok() : Results.NotFound(jobId);
        });

        // 404-parity with disable/enable/trigger on an unknown id (previously 200 unconditionally) —
        // behavior change, acceptable pre-1.0: it removes a false-success from the audit record.
        // RemoveIfExists destroys the hash including the disable metadata, so this audit entry becomes
        // the only surviving record of what was deleted — the snapshot in Detail is load-bearing, not
        // decorative.
        manage.MapPost("/{jobId}/delete", (string jobId, HttpContext http) =>
        {
            var who = actor(http, jobControlOptions);
            using var connection = JobStorage.Current.GetConnection();
            var hash = connection.GetAllEntriesFromHash($"recurring-job:{jobId}");
            if (hash == null || !hash.ContainsKey("Job"))
            {
                AuditStore.Append(JobStorage.Current, new AuditEntry(
                    AuditEntry.CurrentVersion, DateTime.UtcNow, who, "delete", jobId, Reason: null, "not-found", Detail: null),
                    jobControlOptions.AuditMaxEntries);
                return Results.NotFound(jobId);
            }

            var dto = connection.GetRecurringJobs(new[] { jobId }).Single();
            var detail = new Dictionary<string, string> { ["Cron"] = dto.Cron ?? "" };
            var displayName = jobDisplayName(dto);
            if (displayName != null) detail["Job"] = displayName;

            new RecurringJobManager(JobStorage.Current).RemoveIfExists(jobId);

            AuditStore.Append(JobStorage.Current, new AuditEntry(
                AuditEntry.CurrentVersion, DateTime.UtcNow, who, "delete", jobId, Reason: null, "ok", detail),
                jobControlOptions.AuditMaxEntries);
            return Results.Ok();
        });

        view.MapGet("/audit", (int? limit, string? jobId) =>
        {
            var effectiveLimit = Math.Clamp(limit ?? jobControlOptions.AuditDefaultReadLimit, 1, AuditReadLimitHardCap);
            try
            {
                return Results.Ok(AuditStore.Read(JobStorage.Current, effectiveLimit, jobId));
            }
            catch (NotSupportedException ex)
            {
                // Read-back degrades to "no history view" on an unsupported storage; capture (a core
                // interface write) keeps working regardless.
                return Results.Json(new { error = ex.Message }, statusCode: StatusCodes.Status501NotImplemented);
            }
        });

        return new JobControlApiGroups(view, manage);
    }

    /// <summary>
    /// Bundled UI only. Returns the endpoint builder so a caller composing manually can apply its own
    /// auth requirement, the same way it would for <c>MapHangfireDashboard(...)</c>; prefer
    /// <see cref="MapJobControl"/>, which gates the page with the view policy automatically.
    /// </summary>
    public static RouteHandlerBuilder MapJobControlUi(
        this IEndpointRouteBuilder endpoints,
        string uiPath = DefaultUiPath,
        string apiBase = DefaultApiBase)
    {
        var html = loadUiTemplate().Replace(ApiBasePlaceholder, apiBase);
        return endpoints.MapGet(uiPath, () => Results.Content(html, "text/html"));
    }

    // Generic claims-identity extraction by default — works for any ASP.NET Core auth scheme the host
    // configures, not tied to any particular identity provider. Hosts whose principal carries identity
    // elsewhere (e.g. an email claim) override via JobControlOptions.ActorProvider.
    private static string actor(HttpContext http, JobControlOptions options) =>
        options.ActorProvider?.Invoke(http) ?? http.User.Identity?.Name ?? "unknown";

    // Mirrors Job.ToString(includeQueue: false) — the fallback the built-in dashboard's Html.JobName
    // helper itself resolves to when a host hasn't configured a [JobDisplayName]/DisplayNameFunc.
    // Surfaces a load failure (renamed/removed method) the same way the built-in page's Job column
    // does, rather than throwing.
    private static string? jobDisplayName(RecurringJobDto job) => job switch
    {
        { Job: not null } => $"{job.Job.Type.Name}.{job.Job.Method.Name}",
        { LoadException: { InnerException: { } inner } } => inner.Message,
        { LoadException: { } loadException } => loadException.Message,
        _ => null,
    };

    private static string loadUiTemplate()
    {
        var assembly = typeof(JobControlEndpoints).Assembly;
        using var stream = assembly.GetManifestResourceStream(UiResourceName)
            ?? throw new InvalidOperationException(
                $"Embedded resource '{UiResourceName}' not found — check the EmbeddedResource item in the csproj.");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}
