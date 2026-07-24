using System.Text.Json;
using Hangfire;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace OpsToolkit.Hangfire.JobControl;

public sealed record DisableRecurringJobRequest(string? Reason);

public sealed record CronOverrideRequest(string? Cron, string? Reason);

public sealed record CronResetRequest(string? Reason);

public sealed record ArgsOverrideRequest(Dictionary<string, JsonElement>? Args, string? Reason);

public sealed record ArgsResetRequest(string? Reason);

/// <summary>
/// <see cref="Args"/> values overlay the job's effective values (stored override ?? code default) for
/// this one run; null/empty means "run with the effective values as-is". <see cref="Persist"/> also
/// stores the merged values as the job's parameter override, so scheduled fires adopt them —
/// requiring <see cref="Reason"/>, like every other override write.
/// </summary>
public sealed record InvokeRequest(Dictionary<string, JsonElement>? Args, bool Persist = false, string? Reason = null);

/// <summary>
/// Superset of the built-in dashboard's Recurring Jobs page columns (Id, Cron, TimeZone, Job,
/// NextExecution, LastExecution, Created — see Hangfire.Core's RecurringJobsPage.cshtml) plus
/// <see cref="DisableStatus"/> and the override fields. A host adopting this page in place
/// of the built-in one loses no information from it.
///
/// <see cref="Cron"/> is what storage holds — the <i>effective</i> schedule. <see cref="Declared"/>
/// is null when the host has no <see cref="JobControlOptions.Registrar"/> (overrides unavailable,
/// controls hidden), false when a registrar exists but this job isn't declared through it, and true
/// when <see cref="CronDefault"/> carries the code default. <see cref="Override"/> is the stored
/// override row (schedule and/or parameter values), if any — when invalidated, it is dormant and the
/// code defaults are what runs.
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
    RecurringJobDisableStatus? DisableStatus,
    string? CronDefault,
    bool? Declared,
    RecurringJobRuntimeRow? Override);

/// <summary>
/// One parameter of the schema-driven form: the <see cref="JobParameterSchema"/> shape plus this
/// job's current values. <see cref="OverrideValue"/> is the stored override for this parameter (if
/// any, live or dormant); <see cref="Effective"/> is what the next scheduled fire will use —
/// the override when the row is live, the <see cref="CodeDefault"/> otherwise.
/// </summary>
public sealed record JobParameterView(
    string Name,
    string Type,
    bool Editable,
    IReadOnlyList<string>? EnumValues,
    object? CodeDefault,
    object? OverrideValue,
    object? Effective);

/// <summary>
/// <c>GET /{jobId}/parameters</c> — everything the Parameters/Invoke forms need. Empty
/// <see cref="Parameters"/> means the method takes no arguments (invoke still works; there is just
/// nothing to edit). <see cref="Override"/> is the same row the list endpoint carries, so either
/// source can drive the dormancy callout.
/// </summary>
public sealed record RecurringJobParametersView(
    string JobId,
    IReadOnlyList<JobParameterView> Parameters,
    RecurringJobRuntimeRow? Override);

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
    public const string DefaultApiBase = "/hangfire/api";
    public const string DefaultRecurringApiBase = DefaultApiBase + "/recurring";

    // Renamed from bare "/hangfire/job-control" now that a second page (RunEndpoints, "/runs") exists
    // alongside it. LegacyUiPath keeps old bookmarks working
    // via a redirect mapped by MapJobControl.
    public const string DefaultUiPath = "/hangfire/job-control/recurring";
    public const string LegacyUiPath = "/hangfire/job-control";

    private const string UiResourceName = "OpsToolkit.Hangfire.JobControl.wwwroot.recurring.html";
    private const string ApiBasePlaceholder = "{{API_BASE}}";
    private const string OwnUiPathPlaceholder = "{{OWN_UI_PATH}}";
    private const string RunsUiPathPlaceholder = "{{RUNS_UI_PATH}}";
    private const string DashboardPathPlaceholder = "{{DASHBOARD_PATH}}";

    // GET /audit request cap — independent of JobControlOptions.AuditDefaultReadLimit (the default when
    // a caller doesn't specify one); this bounds what a caller CAN ask for even when specifying a limit.
    // Internal (not private): RunEndpoints' own per-job audit passthrough shares this same cap.
    internal const int AuditReadLimitHardCap = 1000;

    /// <summary>
    /// Job parameter stamped on every background job the invoke endpoint creates, carrying the
    /// recurring job id it was invoked from. Deliberately <b>not</b> <c>RecurringJobId</c> (Hangfire's
    /// own trigger stamp): <see cref="DisabledRecurringJobFilter"/> skips on that one, and a manual
    /// invoke must run even when the job is disabled — the intended force-run. Value is JSON-encoded
    /// in storage (Hangfire serializes <c>CreateContext.Parameters</c> values), like
    /// <c>RecurringJobId</c> itself — see <see cref="AuditStore.TryGetRecurringJobId"/>'s remarks.
    /// </summary>
    public const string ManualInvokeOfParameterName = "JobControl.ManualInvokeOf";

    /// <summary>
    /// One-call integration: maps both dashboards — Recurring Jobs (this one) and Job Runs (<see
    /// cref="RunEndpoints"/>) — plus a redirect from the pre-rename <see cref="LegacyUiPath"/>.
    /// <paramref name="viewPolicy"/> gates reads (job lists + audit history + both UI pages);
    /// <paramref name="managePolicy"/> gates mutations (disable/enable/trigger/delete — each of which is
    /// audited, see <see cref="AuditStore"/>).
    /// <paramref name="apiBase"/> is the shared API root; recurring-job routes are mapped beneath
    /// <c>/recurring</c> and run routes beneath <c>/runs</c>.
    /// </summary>
    /// <remarks>
    /// Behavior changes, both pre-1.0, so they are versioned as minor releases but worth flagging
    /// to any caller that already depended on the old shape:
    /// <list type="bullet">
    /// <item><c>/{jobId}/delete</c> now returns 404 for an unknown id (previously 200 unconditionally) —
    /// a false-success shouldn't reach the audit record.</item>
    /// <item>The default UI path moved from <c>/hangfire/job-control</c> to <see cref="DefaultUiPath"/>
    /// now that a second page exists alongside it; the old
    /// path redirects rather than 404ing.</item>
    /// </list>
    /// </remarks>
    public static void MapJobControl(
        this IEndpointRouteBuilder endpoints,
        string viewPolicy,
        string managePolicy,
        string uiPath = DefaultUiPath,
        string apiBase = DefaultApiBase,
        string runsUiPath = RunEndpoints.DefaultUiPath,
        JobControlOptions? options = null)
    {
        var jobControlOptions = options ?? new JobControlOptions();
        var recurringApiBase = childApiBase(apiBase, "recurring");
        var runsApiBase = childApiBase(apiBase, "runs");
        endpoints.MapJobControlApi(viewPolicy, managePolicy, recurringApiBase, options);
        endpoints.MapJobControlUi(uiPath, recurringApiBase, runsUiPath, jobControlOptions.DashboardPath).RequireAuthorization(viewPolicy);
        endpoints.MapJobRunsApi(viewPolicy, managePolicy, runsApiBase, options);
        endpoints.MapJobRunsUi(runsUiPath, runsApiBase, uiPath).RequireAuthorization(viewPolicy);
        endpoints.MapGet(LegacyUiPath, () => Results.Redirect(uiPath)).RequireAuthorization(viewPolicy);
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
            var registrar = jobControlOptions.Registrar;
            using var connection = JobStorage.Current.GetConnection();
            var jobs = connection.GetRecurringJobs()
                .Select(job =>
                {
                    var definition = registrar?.Find(job.Id);
                    return new RecurringJobSummary(
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
                        RecurringJobDisableStore.GetStatus(connection, job.Id),
                        definition?.CronDefault,
                        registrar is null ? null : definition is not null,
                        RecurringJobRuntimeStore.Load(connection, job.Id));
                })
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

        // Schedule override plane — active only when the host declares its jobs through a
        // RecurringJobRegistrar (JobControlOptions.Registrar). Validation is project-first: Hangfire's
        // own AddOrUpdate parses the cron (the same parser the scheduler runs) and throws
        // ArgumentException on a bad expression, so "accepted here" can never drift from "runnable
        // there" — and the override row is only persisted after the projection succeeded.
        manage.MapPost("/{jobId}/cron", (string jobId, CronOverrideRequest request, HttpContext http) =>
        {
            var registrar = jobControlOptions.Registrar;
            if (registrar is null) return overridesNotConfigured();
            if (string.IsNullOrWhiteSpace(request.Cron))
                return Results.BadRequest("A cron expression is required.");
            if (string.IsNullOrWhiteSpace(request.Reason))
                return Results.BadRequest("A reason is required to override a schedule.");

            var who = actor(http, jobControlOptions);
            var definition = registrar.Find(jobId);
            if (definition is null)
            {
                AuditStore.Append(JobStorage.Current, new AuditEntry(
                    AuditEntry.CurrentVersion, DateTime.UtcNow, who, "cron-override", jobId, request.Reason, "not-found", Detail: null),
                    jobControlOptions.AuditMaxEntries);
                return Results.NotFound($"'{jobId}' is not declared through the registrar — only code-declared jobs can carry a schedule override.");
            }

            string? oldCron;
            RecurringJobRuntimeRow? row;
            using (var connection = JobStorage.Current.GetConnection())
            {
                oldCron = connection.GetAllEntriesFromHash($"recurring-job:{jobId}")?.GetValueOrDefault("Cron");
                row = RecurringJobRuntimeStore.Load(connection, jobId);
            }

            try
            {
                // The validation projection carries the job's effective args (a live parameter
                // override must not be clobbered back to code defaults by a schedule change).
                new RecurringJobManager(JobStorage.Current).AddOrUpdate(
                    definition.Id, effectiveJob(definition, row), request.Cron,
                    new RecurringJobOptions { TimeZone = definition.TimeZone });
            }
            catch (ArgumentException ex)
            {
                return Results.BadRequest(ex.InnerException?.Message ?? ex.Message);
            }

            var auditEntry = new AuditEntry(
                AuditEntry.CurrentVersion, DateTime.UtcNow, who, "cron-override", jobId, request.Reason, "ok",
                new Dictionary<string, string> { ["OldCron"] = oldCron ?? "", ["NewCron"] = request.Cron });
            RecurringJobRuntimeStore.SetCronOverride(
                JobStorage.Current, jobId, request.Cron, who, request.Reason, auditEntry.At,
                auditEntry, jobControlOptions.AuditMaxEntries);
            // The write above cleared any dormancy mark; re-projecting from the stored row re-marks
            // a still-broken part (e.g. stored args no longer binding) instead of leaving it looking live.
            reprojectEffective(JobStorage.Current, definition);
            return Results.Ok();
        });

        manage.MapPost("/{jobId}/cron/reset", (string jobId, CronResetRequest? request, HttpContext http) =>
        {
            var registrar = jobControlOptions.Registrar;
            if (registrar is null) return overridesNotConfigured();

            var who = actor(http, jobControlOptions);
            var definition = registrar.Find(jobId);
            var auditEntry = new AuditEntry(
                AuditEntry.CurrentVersion, DateTime.UtcNow, who, "cron-reset", jobId, request?.Reason, "ok",
                definition is null ? null : new Dictionary<string, string> { ["RestoredCron"] = definition.CronDefault });

            if (definition is null)
            {
                AuditStore.Append(JobStorage.Current, auditEntry with { Outcome = "not-found", Detail = null }, jobControlOptions.AuditMaxEntries);
                return Results.NotFound($"'{jobId}' is not declared through the registrar.");
            }

            // The field clear and its audit record commit together; only then is the schedule
            // re-projected. ClearCronOverride returning false means there was nothing to reset — no
            // false-success in the audit trail (it writes nothing in that case). A parameter override
            // on the same row survives the reset, which is why re-projection goes through the shared
            // effective-values path rather than asserting the full code defaults.
            var removed = RecurringJobRuntimeStore.ClearCronOverride(
                JobStorage.Current, jobId, who, request?.Reason, auditEntry.At, auditEntry, jobControlOptions.AuditMaxEntries);
            if (!removed)
                return Results.NotFound($"'{jobId}' has no schedule override to reset.");

            reprojectEffective(JobStorage.Current, definition);
            return Results.Ok();
        });

        // The schema-driven form's data source: parameter names/types with code-default, stored
        // override, and effective values. Registrar-gated like every override route — the schema IS
        // the code definition.
        view.MapGet("/{jobId}/parameters", (string jobId) =>
        {
            var registrar = jobControlOptions.Registrar;
            if (registrar is null) return overridesNotConfigured();
            var definition = registrar.Find(jobId);
            if (definition is null)
                return Results.NotFound($"'{jobId}' is not declared through the registrar — only code-declared jobs have a parameter schema.");

            RecurringJobRuntimeRow? row;
            using (var connection = JobStorage.Current.GetConnection())
                row = RecurringJobRuntimeStore.Load(connection, jobId);

            var overrideValues = parseArgsObject(row?.ArgsJson);
            var overrideLive = row is { IsInvalidated: false };
            var parameters = JobArgs.Schema(definition)
                .Select(parameter =>
                {
                    object? overrideValue = overrideValues.TryGetValue(parameter.Name, out var element) ? element : null;
                    return new JobParameterView(
                        parameter.Name, parameter.Type, parameter.Editable, parameter.EnumValues,
                        parameter.CodeDefault,
                        overrideValue,
                        Effective: overrideLive && overrideValue is not null ? overrideValue : parameter.CodeDefault);
                })
                .ToList();
            return Results.Ok(new RecurringJobParametersView(jobId, parameters, row));
        });

        // The parameter twin of POST /{jobId}/cron. Validation is bind-first (the pure counterpart of
        // cron's project-first: JobArgs.Bind judges the values against the same method signature the
        // performer will invoke), and the override row is only persisted after the values bound.
        manage.MapPost("/{jobId}/args", (string jobId, ArgsOverrideRequest request, HttpContext http) =>
        {
            var registrar = jobControlOptions.Registrar;
            if (registrar is null) return overridesNotConfigured();
            if (request.Args is null || request.Args.Count == 0)
                return Results.BadRequest("At least one parameter value is required.");
            if (string.IsNullOrWhiteSpace(request.Reason))
                return Results.BadRequest("A reason is required to override parameter values.");

            var who = actor(http, jobControlOptions);
            var definition = registrar.Find(jobId);
            if (definition is null)
            {
                AuditStore.Append(JobStorage.Current, new AuditEntry(
                    AuditEntry.CurrentVersion, DateTime.UtcNow, who, "args-override", jobId, request.Reason, "not-found", Detail: null),
                    jobControlOptions.AuditMaxEntries);
                return Results.NotFound($"'{jobId}' is not declared through the registrar — only code-declared jobs can carry a parameter override.");
            }

            var argsJson = JsonSerializer.Serialize(request.Args);
            var binding = JobArgs.Bind(definition, argsJson);
            if (!binding.Succeeded)
                return Results.BadRequest(string.Join("; ", binding.Errors));

            string? oldArgs;
            using (var connection = JobStorage.Current.GetConnection())
                oldArgs = RecurringJobRuntimeStore.Load(connection, jobId)?.ArgsJson;

            var auditEntry = new AuditEntry(
                AuditEntry.CurrentVersion, DateTime.UtcNow, who, "args-override", jobId, request.Reason, "ok",
                new Dictionary<string, string> { ["OldArgs"] = oldArgs ?? "", ["NewArgs"] = argsJson });
            RecurringJobRuntimeStore.SetArgsOverride(
                JobStorage.Current, jobId, argsJson, who, request.Reason, auditEntry.At,
                auditEntry, jobControlOptions.AuditMaxEntries);
            reprojectEffective(JobStorage.Current, definition);
            return Results.Ok();
        });

        manage.MapPost("/{jobId}/args/reset", (string jobId, ArgsResetRequest? request, HttpContext http) =>
        {
            var registrar = jobControlOptions.Registrar;
            if (registrar is null) return overridesNotConfigured();

            var who = actor(http, jobControlOptions);
            var definition = registrar.Find(jobId);
            var auditEntry = new AuditEntry(
                AuditEntry.CurrentVersion, DateTime.UtcNow, who, "args-reset", jobId, request?.Reason, "ok",
                definition is null ? null : new Dictionary<string, string> { ["RestoredArgs"] = codeDefaultArgsJson(definition) });

            if (definition is null)
            {
                AuditStore.Append(JobStorage.Current, auditEntry with { Outcome = "not-found", Detail = null }, jobControlOptions.AuditMaxEntries);
                return Results.NotFound($"'{jobId}' is not declared through the registrar.");
            }

            var removed = RecurringJobRuntimeStore.ClearArgsOverride(
                JobStorage.Current, jobId, who, request?.Reason, auditEntry.At, auditEntry, jobControlOptions.AuditMaxEntries);
            if (!removed)
                return Results.NotFound($"'{jobId}' has no parameter override to reset.");

            reprojectEffective(JobStorage.Current, definition);
            return Results.Ok();
        });

        // Force-run with (optionally edited) parameter values. Deliberately an ad-hoc Create, not
        // TriggerJob: the created job carries ManualInvokeOfParameterName instead of RecurringJobId,
        // so DisabledRecurringJobFilter does NOT skip it — invoking a disabled job is the intended
        // escape hatch (the UI says so before the click), and the audit entry records who forced it.
        manage.MapPost("/{jobId}/invoke", (string jobId, InvokeRequest? request, HttpContext http) =>
        {
            var registrar = jobControlOptions.Registrar;
            if (registrar is null) return overridesNotConfigured();
            var persist = request?.Persist == true;
            if (persist && string.IsNullOrWhiteSpace(request!.Reason))
                return Results.BadRequest("A reason is required to persist parameter values.");

            var who = actor(http, jobControlOptions);
            var definition = registrar.Find(jobId);
            if (definition is null)
            {
                AuditStore.Append(JobStorage.Current, new AuditEntry(
                    AuditEntry.CurrentVersion, DateTime.UtcNow, who, "invoke", jobId, request?.Reason, "not-found", Detail: null),
                    jobControlOptions.AuditMaxEntries);
                return Results.NotFound($"'{jobId}' is not declared through the registrar — only code-declared jobs can be invoked with parameters.");
            }

            RecurringJobRuntimeRow? row;
            using (var connection = JobStorage.Current.GetConnection())
                row = RecurringJobRuntimeStore.Load(connection, jobId);

            // This run's values: the job's effective values (live stored override ?? code defaults)
            // overlaid with whatever the operator edited for this invocation.
            var merged = parseArgsObject(row is { IsInvalidated: false } ? row.ArgsJson : null);
            foreach (var pair in request?.Args ?? new Dictionary<string, JsonElement>())
                merged[pair.Key] = pair.Value;
            var argsJson = merged.Count > 0 ? JsonSerializer.Serialize(merged) : null;
            if (persist && argsJson is null)
                return Results.BadRequest("There are no parameter values to persist — the job takes no arguments or none were supplied.");

            var binding = JobArgs.Bind(definition, argsJson);
            if (!binding.Succeeded)
                return Results.BadRequest(string.Join("; ", binding.Errors));

            var backgroundJobId = new BackgroundJobClient(JobStorage.Current).Create(
                new Job(definition.Job.Type, definition.Job.Method, binding.Args!.ToArray()!),
                new EnqueuedState(),
                new Dictionary<string, object> { [ManualInvokeOfParameterName] = jobId });

            // The same correlation seed the trigger endpoint writes, so a run-level audit trail ties
            // this human action to the execution it caused.
            AuditStore.Append(JobStorage.Current, new AuditEntry(
                AuditEntry.CurrentVersion, DateTime.UtcNow, who, "invoke", jobId, request?.Reason, "ok",
                new Dictionary<string, string>
                {
                    ["BackgroundJobId"] = backgroundJobId,
                    ["Args"] = argsJson ?? "",
                    ["Persisted"] = persist ? "true" : "false",
                }), jobControlOptions.AuditMaxEntries);

            if (persist)
            {
                // Persist what this run used (the merged values), through the same transactional
                // audit path as POST /args — the trail shows an args-override alongside the invoke.
                var overrideAudit = new AuditEntry(
                    AuditEntry.CurrentVersion, DateTime.UtcNow, who, "args-override", jobId, request!.Reason, "ok",
                    new Dictionary<string, string> { ["OldArgs"] = row?.ArgsJson ?? "", ["NewArgs"] = argsJson! });
                RecurringJobRuntimeStore.SetArgsOverride(
                    JobStorage.Current, jobId, argsJson!, who, request.Reason, overrideAudit.At,
                    overrideAudit, jobControlOptions.AuditMaxEntries);
                reprojectEffective(JobStorage.Current, definition);
            }

            return Results.Ok(new { BackgroundJobId = backgroundJobId });
        });

        // Drift repair without a redeploy: re-runs the same reconcile-and-project pass startup runs
        // (e.g. after a mixed-version rollout re-asserted code crons over operator overrides). Never
        // removes undeclared jobs — that stays a startup policy the host opts into explicitly.
        manage.MapPost("/reconcile", (HttpContext http) =>
        {
            var registrar = jobControlOptions.Registrar;
            if (registrar is null) return overridesNotConfigured();

            var summary = registrar.Apply(
                JobStorage.Current, jobControlOptions, actor(http, jobControlOptions),
                removeUndeclared: false, auditEvenIfUnchanged: true);
            return Results.Ok(summary);
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
    /// Bundled UI only. <paramref name="runsUiPath"/> feeds the shared cross-nav header's link forward to
    /// the Job Runs page — see <see cref="RunEndpoints.MapJobRunsUi"/> for the mirror. <paramref
    /// name="dashboardPath"/> builds the page's "view in dashboard" links (see <see
    /// cref="JobControlOptions.DashboardPath"/>); its trailing slash, if any, is trimmed before
    /// substitution so the page's own <c>+ "/jobs/details/..."</c> concatenation never double-slashes.
    /// Returns the endpoint builder so a caller composing manually can apply its own auth requirement, the
    /// same way it would for <c>MapHangfireDashboard(...)</c>; prefer <see cref="MapJobControl"/>, which
    /// gates the page with the view policy automatically.
    /// </summary>
    public static RouteHandlerBuilder MapJobControlUi(
        this IEndpointRouteBuilder endpoints,
        string uiPath = DefaultUiPath,
        string apiBase = DefaultRecurringApiBase,
        string runsUiPath = RunEndpoints.DefaultUiPath,
        string dashboardPath = "/hangfire")
    {
        var html = loadUiTemplate()
            .Replace(ApiBasePlaceholder, apiBase)
            .Replace(OwnUiPathPlaceholder, uiPath)
            .Replace(RunsUiPathPlaceholder, runsUiPath)
            .Replace(DashboardPathPlaceholder, dashboardPath.TrimEnd('/'));
        return endpoints.MapGet(uiPath, () => Results.Content(html, "text/html"));
    }

    // Round-trips a stored/submitted args JSON object into a name → element map; empty for null. Case-
    // insensitive keys, matching JobArgs.Bind's own lookup.
    private static Dictionary<string, JsonElement> parseArgsObject(string? argsJson)
    {
        if (string.IsNullOrEmpty(argsJson)) return new Dictionary<string, JsonElement>(StringComparer.OrdinalIgnoreCase);
        try
        {
            var parsed = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(argsJson);
            return parsed is null
                ? new Dictionary<string, JsonElement>(StringComparer.OrdinalIgnoreCase)
                : new Dictionary<string, JsonElement>(parsed, StringComparer.OrdinalIgnoreCase);
        }
        catch (JsonException)
        {
            // A malformed stored value must not 500 a read or an invoke; Bind/reprojection is where
            // it gets judged (and marked) properly.
            return new Dictionary<string, JsonElement>(StringComparer.OrdinalIgnoreCase);
        }
    }

    // The job to project for a definition given its stored row: override args when they bind, code
    // defaults otherwise. Bind failure isn't handled here — reprojectEffective (via the registrar's
    // shared path) is where a bad stored value gets marked; this is only for projections that must
    // carry the effective args incidentally (the cron endpoint's validation projection).
    private static Job effectiveJob(RecurringJobTypeDefinition definition, RecurringJobRuntimeRow? row)
    {
        if (row?.ArgsJson is not { } argsJson) return definition.Job;
        var binding = JobArgs.Bind(definition, argsJson);
        return binding.Succeeded
            ? new Job(definition.Job.Type, definition.Job.Method, binding.Args!.ToArray()!)
            : definition.Job;
    }

    // Re-asserts effective = override ?? default from the row as currently stored, through the same
    // unit-semantics path Apply uses — including re-marking a still-broken part after a Set*/Clear*
    // write just cleared the row's dormancy mark.
    private static void reprojectEffective(JobStorage storage, RecurringJobTypeDefinition definition)
    {
        RecurringJobRuntimeRow? row;
        using (var connection = storage.GetConnection())
            row = RecurringJobRuntimeStore.Load(connection, definition.Id);
        RecurringJobRegistrar.ProjectEffective(storage, new RecurringJobManager(storage), definition, row);
    }

    // The code-default values as an args JSON object — the args-reset audit detail, mirroring
    // cron-reset's RestoredCron.
    private static string codeDefaultArgsJson(RecurringJobTypeDefinition definition)
        => JsonSerializer.Serialize(JobArgs.Schema(definition)
            .Where(parameter => parameter.Editable)
            .ToDictionary(parameter => parameter.Name, parameter => parameter.CodeDefault));

    // 501 mirrors the audit read-back degrade: the capability is absent by host configuration, not
    // by caller error — the body says exactly what the host must wire up to turn it on.
    private static IResult overridesNotConfigured() => Results.Json(
        new
        {
            error = "Schedule overrides require the host to declare its recurring jobs through a " +
                    "RecurringJobRegistrar and share it via JobControlOptions.Registrar.",
        },
        statusCode: StatusCodes.Status501NotImplemented);

    // Generic claims-identity extraction by default — works for any ASP.NET Core auth scheme the host
    // configures, not tied to any particular identity provider. Hosts whose principal carries identity
    // elsewhere (e.g. an email claim) override via JobControlOptions.ActorProvider. Internal (not
    // private): RunEndpoints' mutations share this exact extraction, since audit actor identity must be
    // computed identically everywhere it's recorded.
    internal static string actor(HttpContext http, JobControlOptions options) =>
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

    private static string childApiBase(string apiBase, string child)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(apiBase);
        return $"{apiBase.TrimEnd('/')}/{child}";
    }
}
