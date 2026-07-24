using Hangfire;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace OpsToolkit.Hangfire.JobControl;

/// <summary>
/// Body shared by the three run mutation endpoints — <c>cancel</c>/<c>requeue</c>/<c>delete</c> (§2.1,
/// §3.2). <see cref="Reason"/> is required for cancel, optional for requeue/delete;
/// <see cref="ExpectedState"/> is required for all three — the state the UI saw, surfaced from
/// mechanic #10's <c>ChangeState</c> assertion so a job that moved in between comes back as a 409
/// refusal rather than a blind mutation of the wrong thing.
/// </summary>
public sealed record RunActionRequest(string? Reason, string? ExpectedState);

/// <summary>Per-state counts driving the Runs dashboard's tab badges (<see cref="StatisticsDto"/>, trimmed to the tabs this page has).</summary>
public sealed record RunStats(long Enqueued, long Scheduled, long Processing, long Succeeded, long Failed, long Deleted, long Servers);

public sealed record RunQueueSummary(string Name, long Length, long? Fetched);

public sealed record RunEnqueuedSummary(string Id, string? JobDisplayName, DateTime? EnqueuedAt);

/// <summary>
/// The current execution's liveness beat, when the job carries a <see cref="HeartbeatAttribute"/>
/// contract (null for every other job). <see cref="Overdue"/> is a wall-clock <b>display hint</b> —
/// "a beat is late" the moment it happens; <see cref="Stalled"/> is the authoritative verdict — the
/// <see cref="StallDetector"/> confirmed the silence on its own observation clock and flagged the
/// execution. <see cref="Acknowledged"/> mirrors the flag's operator acknowledgment (review F8: the
/// flag itself stays until recovery or terminal state).
/// </summary>
public sealed record RunBeatSummary(
    DateTime LastBeatAt, double? Percent, string? Message, int TimeoutSeconds, bool Overdue, bool Stalled, bool Acknowledged);

/// <summary>
/// One flagged execution on <c>GET /stalled</c> — the beat snapshot plus the flag and its
/// acknowledgment state. <see cref="RetryPhase"/>/<see cref="RetryAttempt"/>/<see cref="MaxRetries"/>
/// surface the §5 retry workflow when the contract opted into it (null otherwise): a
/// <c>cancel-requested</c>/<c>blocked</c>/<c>exhausted</c> item is a <b>Deleted</b> job the workflow
/// still governs, kept on this surface deliberately (alerting and the break-glass decision live here).
/// </summary>
public sealed record RunStalledSummary(
    string Id,
    string? JobDisplayName,
    string ExecutionId,
    string? ServerId,
    DateTime? StartedAt,
    DateTime StalledAt,
    DateTime LastBeatAt,
    long Seq,
    double? Percent,
    string? Message,
    int TimeoutSeconds,
    bool Acknowledged,
    string? AcknowledgedBy,
    DateTime? AcknowledgedAt,
    string? AcknowledgeReason,
    string? RetryPhase,
    int? RetryAttempt,
    int? MaxRetries);

public sealed record RunDetectorServerStatus(string ServerId, DateTime LastScanAt, int ScanIntervalSeconds, bool Fresh);

/// <summary>
/// Detector health on <c>GET /stalled</c> (review F5): <see cref="Status"/> is <c>healthy</c> when at
/// least one server's scan lease is fresh, else <c>degraded</c> — so "no detector running" is visible
/// as degraded status, never mistaken for "no stalls".
/// </summary>
public sealed record RunDetectorStatus(string Status, DateTime? LastScanAt, IReadOnlyList<RunDetectorServerStatus> Servers);

/// <summary>
/// <c>GET /stalled</c>'s payload. <see cref="AcknowledgedCount"/>/<see cref="UnacknowledgedCount"/> are
/// split so alerting pages on the latter only (review F8); <see cref="ActiveContractCount"/> lets a
/// zero-item response distinguish "no stalls among N contracted executions" from "nothing is contracted".
/// </summary>
public sealed record RunStalledResponse(
    RunDetectorStatus Detector,
    long ActiveContractCount,
    int AcknowledgedCount,
    int UnacknowledgedCount,
    IReadOnlyList<RunStalledSummary> Items);

/// <summary>Body of <c>POST /{id}/acknowledge-stall</c> — the reason is required (it is recorded on the flag and audited).</summary>
public sealed record StallAcknowledgeRequest(string? Reason);

public sealed record RunProcessingSummary(string Id, string? JobDisplayName, string? ServerId, DateTime? StartedAt, RunBeatSummary? Beat);

public sealed record RunScheduledSummary(string Id, string? JobDisplayName, DateTime EnqueueAt);

public sealed record RunSucceededSummary(string Id, string? JobDisplayName, DateTime? SucceededAt, long? DurationMs);

public sealed record RunFailedSummary(string Id, string? JobDisplayName, DateTime? FailedAt, string? ExceptionType, string? ExceptionMessage);

/// <summary>
/// <see cref="Cancelled"/> and the fields after it come from the <c>JobControl.CancelRequested</c> job
/// parameter (see <see cref="CancellationRequestStore"/>), not from Hangfire's own <c>DeletedJobDto</c> —
/// that's what lets the Deleted tab distinguish a governed cancel from a plain delete (§2.4/§3.4).
/// <see cref="StallPhase"/> is the current execution's §5 retry-workflow phase when one exists (a
/// <c>blocked</c> row is requeue-guarded and offers force-requeue instead).
/// </summary>
public sealed record RunDeletedSummary(
    string Id, string? JobDisplayName, DateTime? DeletedAt,
    bool Cancelled, string? CancelledBy, DateTime? CancelledAt, string? CancelReason, string? StallPhase);

public sealed record RunServerSummary(string Name, int WorkersCount, DateTime StartedAt, IList<string> Queues, DateTime? Heartbeat);

public sealed record RunStateHistoryEntry(string StateName, string? Reason, DateTime CreatedAt, IDictionary<string, string>? Data);

/// <summary>
/// Drives the drawer: <see cref="JobDetailsDto"/> trimmed to what an operator needs (invocation display
/// name, parameters, and the full per-run state history) — <see cref="JobDetailsDto.InvocationData"/>
/// itself is Hangfire's serialized-args wire format, not something worth exposing raw.
/// </summary>
public sealed record RunJobDetails(
    string Id,
    string? JobDisplayName,
    IDictionary<string, string>? Properties,
    DateTime? CreatedAt,
    DateTime? ExpireAt,
    IReadOnlyList<RunStateHistoryEntry> History);

/// <summary>
/// HTTP plane of the Job Runs dashboard: a JSON facade over <see cref="IMonitoringApi"/> (the same read
/// plane the built-in Hangfire dashboard itself renders from) plus its bundled operator UI, the
/// cancel-request → abort → acknowledge protocol, and the
/// governed requeue/delete mutations (§3.2).
/// </summary>
public static class RunEndpoints
{
    public const string DefaultApiBase = "/hangfire/api/runs";
    public const string DefaultUiPath = "/hangfire/job-control/runs";

    private const string UiResourceName = "OpsToolkit.Hangfire.JobControl.wwwroot.runs.html";
    private const string ApiBasePlaceholder = "{{API_BASE}}";
    private const string OwnUiPathPlaceholder = "{{OWN_UI_PATH}}";
    private const string RecurringUiPathPlaceholder = "{{RECURRING_UI_PATH}}";

    // Independent of JobControlOptions.RunsDefaultPageSize (the default when a caller doesn't specify
    // one); this bounds what a caller CAN ask for even when specifying a count — same split as
    // JobControlEndpoints.AuditReadLimitHardCap.
    private const int RunsReadLimitHardCap = 500;

    /// <summary>API only — for hosts that bring their own frontend. Mirrors <see cref="JobControlEndpoints.MapJobControlApi"/>.</summary>
    public static JobControlApiGroups MapJobRunsApi(
        this IEndpointRouteBuilder endpoints,
        string viewPolicy,
        string managePolicy,
        string apiBase,
        JobControlOptions? options = null)
    {
        var jobControlOptions = options ?? new JobControlOptions();
        var view = endpoints.MapGroup(apiBase).RequireAuthorization(viewPolicy);
        var manage = endpoints.MapGroup(apiBase).RequireAuthorization(managePolicy);

        view.MapGet("/stats", () =>
        {
            var monitor = JobStorage.Current.GetMonitoringApi();
            var stats = monitor.GetStatistics();
            return Results.Ok(new RunStats(
                stats.Enqueued, stats.Scheduled, stats.Processing, stats.Succeeded, stats.Failed, stats.Deleted, stats.Servers));
        });

        // Drives the Queued tab's queue picker; per-queue job lists come from /enqueued?queue=.
        view.MapGet("/queues", () =>
        {
            var monitor = JobStorage.Current.GetMonitoringApi();
            var queues = monitor.Queues();
            return Results.Ok(queues.Select(q => new RunQueueSummary(q.Name, q.Length, q.Fetched)).ToList());
        });

        view.MapGet("/enqueued", (string? queue, int? from, int? count) =>
        {
            var monitor = JobStorage.Current.GetMonitoringApi();
            // No queue named yet (first load of the tab) — default to whatever /queues would list first,
            // the same "pick one so the tab isn't empty" behavior as the built-in dashboard.
            var effectiveQueue = queue ?? monitor.Queues().FirstOrDefault()?.Name;
            if (effectiveQueue is null) return Results.Ok(Array.Empty<RunEnqueuedSummary>());

            var (pageFrom, pageCount) = clampPage(from, count, jobControlOptions.RunsDefaultPageSize);
            var jobs = monitor.EnqueuedJobs(effectiveQueue, pageFrom, pageCount);
            return Results.Ok(jobs
                .Select(pair => new RunEnqueuedSummary(pair.Key, jobDisplayName(pair.Value.Job, pair.Value.LoadException), pair.Value.EnqueuedAt))
                .ToList());
        });

        view.MapGet("/processing", (int? from, int? count) =>
        {
            var monitor = JobStorage.Current.GetMonitoringApi();
            var (pageFrom, pageCount) = clampPage(from, count, jobControlOptions.RunsDefaultPageSize);
            var jobs = monitor.ProcessingJobs(pageFrom, pageCount);

            // Two GetJobParameter reads per row (pointer + record, both PK lookups) — the same per-row
            // cost the Deleted tab already pays for its cancel marker — plus one stalled-index read per
            // page and a marker read for the (rare) flagged rows only.
            using var connection = JobStorage.Current.GetConnection();
            var stalledMembers = new HashSet<string>(LivenessStore.ReadStalledMembers(connection), StringComparer.Ordinal);
            var now = DateTime.UtcNow;
            return Results.Ok(jobs
                .Select(pair =>
                {
                    var beat = LivenessStore.ReadCurrentBeat(connection, pair.Key);
                    if (beat is null)
                    {
                        return new RunProcessingSummary(
                            pair.Key, jobDisplayName(pair.Value.Job, pair.Value.LoadException), pair.Value.ServerId, pair.Value.StartedAt, null);
                    }

                    var stalled = stalledMembers.Contains(LivenessStore.ActiveMember(pair.Key, beat.ExecutionId));
                    var acknowledged = stalled
                        && LivenessStore.ReadStall(connection, pair.Key, beat.ExecutionId)?.AcknowledgedBy is not null;
                    return new RunProcessingSummary(
                        pair.Key, jobDisplayName(pair.Value.Job, pair.Value.LoadException), pair.Value.ServerId, pair.Value.StartedAt,
                        new RunBeatSummary(beat.BeatAt, beat.Percent, beat.Message, beat.TimeoutSeconds,
                            Overdue: now - beat.BeatAt > TimeSpan.FromSeconds(beat.TimeoutSeconds),
                            Stalled: stalled, Acknowledged: acknowledged));
                })
                .ToList());
        });

        // §7: flagged executions plus detector status — never an unqualified zero (review F5). Reads the
        // stalled index, each flag's marker + beat snapshot, and the detector health leases.
        view.MapGet("/stalled", () =>
        {
            using var connection = JobStorage.Current.GetConnection();

            var items = new List<RunStalledSummary>();
            foreach (var member in LivenessStore.ReadStalledMembers(connection))
            {
                var tuple = LivenessStore.TryParseActiveMember(member);
                if (tuple is null) continue;
                var (jobId, executionId) = tuple.Value;

                // Index entry whose marker is gone: ClearStall's documented crash window — skipped here,
                // physically removed by the detector's next self-heal pass.
                var marker = LivenessStore.ReadStall(connection, jobId, executionId);
                if (marker is null) continue;

                var beat = LivenessStore.ReadBeat(connection, jobId, executionId);
                var jobData = tryGetJobData(connection, jobId);
                var attempt = LivenessStore.ReadStallAttempt(connection, jobId, executionId);
                items.Add(new RunStalledSummary(
                    jobId,
                    jobData is null ? null : jobDisplayName(jobData.Job, jobData.LoadException),
                    executionId,
                    beat?.ServerId,
                    beat?.StartedAt,
                    marker.StalledAt,
                    beat?.BeatAt ?? marker.StalledAt,
                    beat?.Seq ?? marker.Seq,
                    beat?.Percent,
                    beat?.Message,
                    beat?.TimeoutSeconds ?? 0,
                    Acknowledged: marker.AcknowledgedBy is not null,
                    marker.AcknowledgedBy,
                    marker.AcknowledgedAt,
                    marker.AcknowledgeReason,
                    RetryPhase: attempt?.Phase,
                    RetryAttempt: attempt?.AttemptNumber,
                    MaxRetries: attempt?.MaxRetries));
            }

            var utcNow = DateTime.UtcNow;
            var servers = LivenessStore.ReadDetectorLeases(connection)
                .Select(lease => new RunDetectorServerStatus(
                    lease.ServerId, lease.LastScanAt, lease.ScanIntervalSeconds, LivenessStore.IsDetectorLeaseFresh(lease, utcNow)))
                .OrderByDescending(server => server.LastScanAt)
                .ToList();
            var detector = new RunDetectorStatus(
                servers.Any(server => server.Fresh) ? "healthy" : "degraded",
                servers.Count == 0 ? null : servers.Max(server => server.LastScanAt),
                servers);

            var acknowledged = items.Count(item => item.Acknowledged);
            return Results.Ok(new RunStalledResponse(
                detector,
                LivenessStore.CountActive(connection),
                acknowledged,
                items.Count - acknowledged,
                items.OrderByDescending(item => item.StalledAt).ToList()));
        });

        view.MapGet("/scheduled", (int? from, int? count) =>
        {
            var monitor = JobStorage.Current.GetMonitoringApi();
            var (pageFrom, pageCount) = clampPage(from, count, jobControlOptions.RunsDefaultPageSize);
            var jobs = monitor.ScheduledJobs(pageFrom, pageCount);
            return Results.Ok(jobs
                .Select(pair => new RunScheduledSummary(pair.Key, jobDisplayName(pair.Value.Job, pair.Value.LoadException), pair.Value.EnqueueAt))
                .ToList());
        });

        view.MapGet("/succeeded", (int? from, int? count) =>
        {
            var monitor = JobStorage.Current.GetMonitoringApi();
            var (pageFrom, pageCount) = clampPage(from, count, jobControlOptions.RunsDefaultPageSize);
            var jobs = monitor.SucceededJobs(pageFrom, pageCount);
            return Results.Ok(jobs
                .Select(pair => new RunSucceededSummary(pair.Key, jobDisplayName(pair.Value.Job, pair.Value.LoadException), pair.Value.SucceededAt, pair.Value.TotalDuration))
                .ToList());
        });

        view.MapGet("/failed", (int? from, int? count) =>
        {
            var monitor = JobStorage.Current.GetMonitoringApi();
            var (pageFrom, pageCount) = clampPage(from, count, jobControlOptions.RunsDefaultPageSize);
            var jobs = monitor.FailedJobs(pageFrom, pageCount);
            return Results.Ok(jobs
                .Select(pair => new RunFailedSummary(pair.Key, jobDisplayName(pair.Value.Job, pair.Value.LoadException), pair.Value.FailedAt, pair.Value.ExceptionType, pair.Value.ExceptionMessage))
                .ToList());
        });

        view.MapGet("/deleted", (int? from, int? count) =>
        {
            var monitor = JobStorage.Current.GetMonitoringApi();
            var (pageFrom, pageCount) = clampPage(from, count, jobControlOptions.RunsDefaultPageSize);
            var jobs = monitor.DeletedJobs(pageFrom, pageCount);

            // A few GetJobParameter reads per row (PK lookups) — trivial at page-size volume — are what
            // distinguish a governed cancel from a plain delete, and a requeue-guarded blocked stall
            // from an ordinary Deleted row, without the drawer (§3.1, §3.4; liveness plan §5 Rule 4).
            using var connection = JobStorage.Current.GetConnection();
            return Results.Ok(jobs
                .Select(pair =>
                {
                    var marker = CancellationRequestStore.Read(connection, pair.Key);
                    var executionId = marker is null ? null : LivenessStore.ReadCurrentExecutionId(connection, pair.Key);
                    var attempt = executionId is null ? null : LivenessStore.ReadStallAttempt(connection, pair.Key, executionId);
                    return new RunDeletedSummary(
                        pair.Key, jobDisplayName(pair.Value.Job, pair.Value.LoadException), pair.Value.DeletedAt,
                        Cancelled: marker is not null, marker?.By, marker?.At, marker?.Reason,
                        StallPhase: attempt?.Phase);
                })
                .ToList());
        });

        view.MapGet("/servers", () =>
        {
            var monitor = JobStorage.Current.GetMonitoringApi();
            var servers = monitor.Servers();
            return Results.Ok(servers
                .Select(s => new RunServerSummary(s.Name, s.WorkersCount, s.StartedAt, s.Queues, s.Heartbeat))
                .ToList());
        });

        view.MapGet("/{id}", (string id) =>
        {
            var monitor = JobStorage.Current.GetMonitoringApi();
            JobDetailsDto? details;
            try
            {
                details = monitor.JobDetails(id);
            }
            catch (FormatException)
            {
                // Verified empirically against Hangfire.PostgreSql 1.20.13 (this repo's own test/demo
                // dependency): its JobDetails does an unguarded Convert.ToInt64(jobId) internally (job
                // ids there are auto-increment integers, stringified) and throws FormatException for an
                // id that never had that shape, rather than returning null like a "well-formed but
                // missing" id does. Same 404 outcome either way from the operator's side.
                details = null;
            }
            if (details is null) return Results.NotFound(id);

            var history = (details.History ?? new List<StateHistoryDto>())
                .Select(h => new RunStateHistoryEntry(h.StateName, h.Reason, h.CreatedAt, h.Data))
                .ToList();
            return Results.Ok(new RunJobDetails(
                id, jobDisplayName(details.Job, details.LoadException), details.Properties, details.CreatedAt, details.ExpireAt, history));
        });

        // Thin passthrough over the same AuditStore the Recurring page's own /audit endpoint reads —
        // same store and schema. Scoped under this page's
        // own API base so the Runs drawer's cancel panel (§3.4: "resolved by polling that job's audit
        // entries") doesn't need to know the Recurring page's API base just to poll one job's history.
        view.MapGet("/{id}/audit", (string id, int? limit) =>
        {
            var effectiveLimit = Math.Clamp(limit ?? jobControlOptions.AuditDefaultReadLimit, 1, JobControlEndpoints.AuditReadLimitHardCap);
            try
            {
                return Results.Ok(AuditStore.Read(JobStorage.Current, effectiveLimit, id));
            }
            catch (NotSupportedException ex)
            {
                return Results.Json(new { error = ex.Message }, statusCode: StatusCodes.Status501NotImplemented);
            }
        });

        // Shared by cancel/requeue/delete below — one audit-append shape per action name.
        void appendAudit(string action, string actor, string jobId, string? reason, string outcome, IReadOnlyDictionary<string, string>? detail)
            => AuditStore.Append(JobStorage.Current, new AuditEntry(
                AuditEntry.CurrentVersion, DateTime.UtcNow, actor, action, jobId, reason, outcome, detail),
                jobControlOptions.AuditMaxEntries);

        // §2.1: request → abort → acknowledge. Reason and expectedState are both required — expectedState
        // is the state the UI saw (Enqueued/Scheduled/Processing), surfaced from mechanic #10's
        // ChangeState assertion so a job that moved in between comes back as a 409 refusal, not a blind
        // kill of the wrong thing.
        manage.MapPost("/{id}/cancel", (string id, RunActionRequest request, HttpContext http) =>
        {
            if (string.IsNullOrWhiteSpace(request.Reason))
                return Results.BadRequest("A reason is required to cancel a job.");
            if (string.IsNullOrWhiteSpace(request.ExpectedState))
                return Results.BadRequest("expectedState is required.");

            var who = JobControlEndpoints.actor(http, jobControlOptions);

            JobData? jobData;
            using (var connection = JobStorage.Current.GetConnection())
            {
                jobData = tryGetJobData(connection, id);
            }

            if (jobData is null)
            {
                appendAudit("cancel", who, id, request.Reason, "not-found", detail: null);
                return Results.NotFound(id);
            }

            if (!string.Equals(jobData.State, request.ExpectedState, StringComparison.OrdinalIgnoreCase))
            {
                appendAudit("cancel", who, id, request.Reason, "wrong-state",
                    new Dictionary<string, string> { ["CurrentState"] = jobData.State ?? "" });
                return Results.Conflict(new { currentState = jobData.State });
            }

            // Reason rides on the state itself (not just the audit trail) — even the built-in
            // dashboard's job detail page then shows who/why in the state history (§2.1 point 1).
            var stateReason = $"Cancelled by {who}: {request.Reason}";
            var client = new BackgroundJobClient(JobStorage.Current);

            // Only a Processing cancel has a running body to acknowledge — queued/scheduled cancels are
            // complete the instant the state change lands (mechanics #7, #8), so preparing a request for
            // those would leave a permanent phantom (nothing will ever acknowledge it). A Processing
            // cancel runs the linearized protocol (liveness plan §5 Rule 2, shared with the stall
            // detector): the request marker is PREPARED before the transition, and the transition itself
            // — a CancelledState whose state data carries the request identity — is the commit record.
            // A fast abort can therefore never outrun the marker into OnPerformed (the retired
            // abort-observed near-miss), and a cancel whose transition loses is retired unacknowledged.
            var isProcessingCancel = string.Equals(request.ExpectedState, ProcessingState.StateName, StringComparison.OrdinalIgnoreCase);
            IState targetState;
            string? requestId = null;
            if (isProcessingCancel)
            {
                requestId = Guid.NewGuid().ToString("N");
                string? executionId;
                using (var connection = JobStorage.Current.GetConnection())
                {
                    // Scope the cancel to the current liveness execution only when that execution is
                    // actually enrolled right now (active tuple present) — a stale pointer (say, a prior
                    // run's, after this run's enrollment failed) must not fence out the ack.
                    executionId = LivenessStore.ReadCurrentExecutionId(connection, id);
                    if (executionId is not null && !LivenessStore.ReadActiveMembers(connection)
                            .Contains(LivenessStore.ActiveMember(id, executionId)))
                    {
                        executionId = null;
                    }

                    CancellationRequestStore.Write(connection, id, who, DateTime.UtcNow, request.Reason, requestId, executionId);
                }
                targetState = new CancelledState(requestId, executionId) { Reason = stateReason };
            }
            else
            {
                targetState = new DeletedState { Reason = stateReason };
            }

            var changed = client.ChangeState(id, targetState, request.ExpectedState);

            if (!changed)
            {
                // Lost a race between the state read above and the change itself (job moved again in
                // between) — retire the prepared request (no ack may ever match it, §5 Rule 2 step 4)
                // and refetch for an honest current-state 409 rather than a bare failure.
                string? current;
                using (var connection = JobStorage.Current.GetConnection())
                {
                    if (requestId is not null) CancellationRequestStore.ClearIfRequest(connection, id, requestId);
                    current = tryGetJobData(connection, id)?.State;
                }
                appendAudit("cancel", who, id, request.Reason, "wrong-state",
                    new Dictionary<string, string> { ["CurrentState"] = current ?? "unknown" });
                return Results.Conflict(new { currentState = current });
            }

            var okDetail = new Dictionary<string, string> { ["FromState"] = request.ExpectedState };
            var displayName = jobDisplayName(jobData.Job, jobData.LoadException);
            if (displayName is not null) okDetail["JobDisplayName"] = displayName;

            using (var connection = JobStorage.Current.GetConnection())
            {
                var recurringJobId = AuditStore.TryGetRecurringJobId(connection, id);
                if (recurringJobId is not null) okDetail["RecurringJobId"] = recurringJobId;
            }

            appendAudit("cancel", who, id, request.Reason, "ok", okDetail);
            return Results.Ok();
        });

        // Review F8: acknowledge, not clear — the stalled condition remains true (the beat *is*
        // overdue; clearing would silently re-flag). Records who/why/when on the flag, scoped to the
        // current execution so it can never hide a later attempt's stall; retires automatically with the
        // flag on recovery or terminal state. Alerting keys off the unacknowledged count only.
        manage.MapPost("/{id}/acknowledge-stall", (string id, StallAcknowledgeRequest request, HttpContext http) =>
        {
            if (string.IsNullOrWhiteSpace(request.Reason))
                return Results.BadRequest("A reason is required to acknowledge a stall.");

            var who = JobControlEndpoints.actor(http, jobControlOptions);
            using var connection = JobStorage.Current.GetConnection();

            if (tryGetJobData(connection, id) is null)
            {
                appendAudit("acknowledge-stall", who, id, request.Reason, "not-found", detail: null);
                return Results.NotFound(id);
            }

            var executionId = LivenessStore.ReadCurrentExecutionId(connection, id);
            if (executionId is null || LivenessStore.ReadStall(connection, id, executionId) is null)
            {
                appendAudit("acknowledge-stall", who, id, request.Reason, "not-stalled", detail: null);
                return Results.Conflict(new { error = "This job's current execution is not flagged as stalled." });
            }

            // A stall phase transition (review F4): re-read the marker under the per-job lock so the
            // acknowledgment can't resurrect a flag the detector retired in between, and two operators
            // can't both win.
            try
            {
                using var _ = connection.AcquireDistributedLock(LivenessStore.JobLockResource(id), TimeSpan.FromSeconds(2));
                var marker = LivenessStore.ReadStall(connection, id, executionId);
                if (marker is null)
                {
                    appendAudit("acknowledge-stall", who, id, request.Reason, "not-stalled", detail: null);
                    return Results.Conflict(new { error = "The stall flag was retired (recovered or terminal) before the acknowledgment landed." });
                }

                if (marker.AcknowledgedBy is not null)
                {
                    appendAudit("acknowledge-stall", who, id, request.Reason, "already-acknowledged",
                        new Dictionary<string, string> { ["AcknowledgedBy"] = marker.AcknowledgedBy });
                    return Results.Conflict(new { acknowledgedBy = marker.AcknowledgedBy, acknowledgedAt = marker.AcknowledgedAt });
                }

                LivenessStore.WriteStall(connection, id, marker with
                {
                    AcknowledgedBy = who,
                    AcknowledgedAt = DateTime.UtcNow,
                    AcknowledgeReason = request.Reason,
                });
            }
            catch (DistributedLockTimeoutException)
            {
                return Results.Conflict(new { error = "The stall detector holds this job's lock — retry in a moment." });
            }

            var detail = new Dictionary<string, string> { ["ExecutionId"] = executionId };
            var recurringJobId = AuditStore.TryGetRecurringJobId(connection, id);
            if (recurringJobId is not null) detail["RecurringJobId"] = recurringJobId;
            appendAudit("acknowledge-stall", who, id, request.Reason, "ok", detail);
            return Results.Ok();
        });

        // §3.2: requeue is a rescue lever on Enqueued (a lost/stuck queue entry), "Run now" on Scheduled
        // (same wire action, different UI label), a deliberate re-execution on Succeeded/Failed, and the
        // second half of the governed "stop and rerun" sequence on Deleted. For a stall-workflow cancel
        // the API hard-blocks requeue until the cancelled execution acknowledges (§5 Rule 4; OPS-003) —
        // force-requeue is the audited exit. For a plain operator cancel (no workflow record, and for a
        // non-enrolled job no execution identity an ack could even fence on) the ack gate stays a UI
        // affordance only, since a dead-worker no-ack would otherwise strand the job forever.
        manage.MapPost("/{id}/requeue", (string id, RunActionRequest request, HttpContext http) =>
        {
            if (string.IsNullOrWhiteSpace(request.ExpectedState))
                return Results.BadRequest("expectedState is required.");

            var who = JobControlEndpoints.actor(http, jobControlOptions);

            // §3.3: requeue of a job the caller claims is Processing is refused outright — unlike the
            // expectedState mismatch below (a race the caller couldn't have known about), this rejects
            // exactly what the caller asked for, because honoring it risks a concurrent double-execution
            // against whatever body might still be running.
            if (string.Equals(request.ExpectedState, ProcessingState.StateName, StringComparison.OrdinalIgnoreCase))
            {
                appendAudit("requeue", who, id, request.Reason, "processing-rejected", detail: null);
                return Results.Conflict(new { currentState = ProcessingState.StateName });
            }

            JobData? jobData;
            using (var connection = JobStorage.Current.GetConnection())
            {
                jobData = tryGetJobData(connection, id);
            }

            if (jobData is null)
            {
                appendAudit("requeue", who, id, request.Reason, "not-found", detail: null);
                return Results.NotFound(id);
            }

            if (!string.Equals(jobData.State, request.ExpectedState, StringComparison.OrdinalIgnoreCase))
            {
                appendAudit("requeue", who, id, request.Reason, "wrong-state",
                    new Dictionary<string, string> { ["CurrentState"] = jobData.State ?? "" });
                return Results.Conflict(new { currentState = jobData.State });
            }

            bool changed;
            using (var connection = JobStorage.Current.GetConnection())
            {
                // Liveness plan §5 Rule 4 (review B4; OPS-003): a Deleted job whose committed stall-cancel
                // has no matching acknowledgment is refused in BOTH pre-ack phases — cancel-requested (the
                // grace window is a wait for evidence, not evidence that waiting is over) and blocked —
                // because its body's fate is unknowable, and adding a queue row can race storage-native
                // recovery of the orphaned fetched lease into a double-run. Keyed off the workflow record,
                // so non-liveness jobs are unaffected; a late acknowledgment lifts the guard by itself.
                // The break-glass exit is POST /{id}/force-requeue. The whole read/decision/transition/
                // cleanup sequence runs under the detector's per-job lock (review F4; OPS-003 §4):
                // without it the detector could advance the workflow between this check and the requeue.
                IDisposable requeueLock;
                try
                {
                    requeueLock = connection.AcquireDistributedLock(LivenessStore.JobLockResource(id), TimeSpan.FromSeconds(2));
                }
                catch (DistributedLockTimeoutException)
                {
                    return Results.Conflict(new { error = "Another liveness decision holds this job's lock — retry in a moment." });
                }

                using (requeueLock)
                {
                    var governed = readGovernedStallCancel(connection, id);
                    var ack = governed is null
                        ? null
                        : CancellationRequestStore.ReadAck(connection, id, governed.ExecutionId, governed.RequestId);
                    if (governed is not null && ack is null)
                    {
                        appendAudit("requeue", who, id, request.Reason, "stall-blocked",
                            new Dictionary<string, string>
                            {
                                ["ExecutionId"] = governed.ExecutionId,
                                ["RequestId"] = governed.RequestId,
                                ["Phase"] = governed.Phase,
                            });
                        return Results.Conflict(new
                        {
                            error = "This job's stall-cancel was never acknowledged (the body's fate is unknowable) — " +
                                    "requeue is blocked. Recycle the owning worker process first, or use force-requeue " +
                                    "to break glass (audited; duplicate-execution hazard).",
                            stallBlocked = true,
                        });
                    }

                    changed = JobRequeue.TryRequeue(JobStorage.Current, connection, id, request.ExpectedState);

                    // The late-ack path: the governed workflow whose acknowledgment lifted the guard is
                    // settled here, under the same lock as the transition (OPS-003 §4 step 7), instead of
                    // leaving a stale record for the detector's sweep to skirt around. completed-anyway is
                    // recorded honestly — this requeue was a deliberate rerun of a finished body, not a retry.
                    if (changed && governed is not null && ack is not null)
                    {
                        var deliberateRerun = ack.Result == CancelAckRecord.ResultCompletedAnyway;
                        LivenessStore.WriteStallAttempt(connection, id, governed with
                        {
                            Phase = deliberateRerun
                                ? StallAttemptRecord.PhaseCompletedAnyway
                                : StallAttemptRecord.PhaseRetried,
                            UpdatedAt = DateTime.UtcNow,
                            Detail = deliberateRerun
                                ? $"Completed anyway; deliberately re-run by {who}."
                                : $"Requeued by {who} after acknowledgment ({ack.Result}).",
                        });
                        LivenessStore.RemoveStalledMember(connection, id, governed.ExecutionId);
                        LivenessStore.RemoveRetryPendingMember(connection, id, governed.ExecutionId);
                    }
                }
            }

            if (!changed)
            {
                string? current;
                using (var connection = JobStorage.Current.GetConnection())
                {
                    current = tryGetJobData(connection, id)?.State;
                }
                appendAudit("requeue", who, id, request.Reason, "wrong-state",
                    new Dictionary<string, string> { ["CurrentState"] = current ?? "unknown" });
                return Results.Conflict(new { currentState = current });
            }

            var detail = new Dictionary<string, string> { ["FromState"] = request.ExpectedState };
            var displayName = jobDisplayName(jobData.Job, jobData.LoadException);
            if (displayName is not null) detail["JobDisplayName"] = displayName;

            using (var connection = JobStorage.Current.GetConnection())
            {
                var recurringJobId = AuditStore.TryGetRecurringJobId(connection, id);
                if (recurringJobId is not null) detail["RecurringJobId"] = recurringJobId;
            }

            appendAudit("requeue", who, id, request.Reason, "ok", detail);
            return Results.Ok();
        });

        // The break-glass exit from the Rule-4 pre-ack states (§5, review B4; OPS-003): a deliberate,
        // separately confirmed, reason-required override for an operator who has weighed the documented
        // hazards — the old body may still be running (duplicate side effects), and its eventual terminal
        // transition can clobber the new attempt's state. Recommended first move is recycling the owning
        // worker process instead. Accepts BOTH unacknowledged phases — cancel-requested and blocked —
        // because AckGracePeriod expiring adds no safety an operator could be made to wait for; the risk
        // acceptance is identical in either phase. Refused for anything else — the ordinary requeue
        // endpoint owns every other case.
        manage.MapPost("/{id}/force-requeue", (string id, RunActionRequest request, HttpContext http) =>
        {
            if (string.IsNullOrWhiteSpace(request.Reason))
                return Results.BadRequest("A reason is required to force-requeue past an unacknowledged stall-cancel.");
            if (!string.Equals(request.ExpectedState, DeletedState.StateName, StringComparison.OrdinalIgnoreCase))
                return Results.BadRequest("expectedState must be Deleted — only an unacknowledged stall-cancel can be force-requeued.");

            var who = JobControlEndpoints.actor(http, jobControlOptions);

            using var connection = JobStorage.Current.GetConnection();
            var jobData = tryGetJobData(connection, id);
            if (jobData is null)
            {
                appendAudit("force-requeue-unacknowledged-stall", who, id, request.Reason, "not-found", detail: null);
                return Results.NotFound(id);
            }

            // A stall phase transition (review F4; OPS-003 §4): everything is re-read inside the per-job
            // lock — the workflow may have advanced, and the acknowledgment may have landed, while this
            // request waited behind the detector.
            IDisposable forceLock;
            try
            {
                forceLock = connection.AcquireDistributedLock(LivenessStore.JobLockResource(id), TimeSpan.FromSeconds(2));
            }
            catch (DistributedLockTimeoutException)
            {
                return Results.Conflict(new { error = "Another liveness decision holds this job's lock — retry in a moment." });
            }

            using (forceLock)
            {
                var pending = readGovernedStallCancel(connection, id);
                if (pending is null)
                {
                    appendAudit("force-requeue-unacknowledged-stall", who, id, request.Reason, "not-eligible", detail: null);
                    return Results.Conflict(new
                    {
                        error = "This job is not governed by an unacknowledged stall-cancel — use the ordinary requeue action.",
                    });
                }

                // The ack re-read is the last word before the transition (OPS-003 §"Acknowledgment
                // arriving during force-requeue"): completed-anyway makes the force decision stale — the
                // body finished its work, so re-running it is a separate deliberate action, never the
                // stale override. The workflow settles here, under the lock, so the refusal is durable.
                var ack = CancellationRequestStore.ReadAck(connection, id, pending.ExecutionId, pending.RequestId);
                if (ack?.Result == CancelAckRecord.ResultCompletedAnyway)
                {
                    LivenessStore.WriteStallAttempt(connection, id, pending with
                    {
                        Phase = StallAttemptRecord.PhaseCompletedAnyway,
                        UpdatedAt = DateTime.UtcNow,
                        Detail = "Completed-anyway acknowledgment arrived before a force-requeue committed.",
                    });
                    LivenessStore.RemoveStalledMember(connection, id, pending.ExecutionId);
                    LivenessStore.RemoveRetryPendingMember(connection, id, pending.ExecutionId);
                    appendAudit("force-requeue-unacknowledged-stall", who, id, request.Reason, "completed-anyway",
                        new Dictionary<string, string> { ["ExecutionId"] = pending.ExecutionId, ["RequestId"] = pending.RequestId });
                    return Results.Conflict(new
                    {
                        error = "The cancelled execution completed its work before the override committed — " +
                                "re-running it would duplicate a completed run. Rerun the job as its own " +
                                "deliberate action if that is intended.",
                        completedAnyway = true,
                    });
                }

                if (!JobRequeue.TryRequeue(JobStorage.Current, connection, id, DeletedState.StateName,
                        stateReason: ack is null
                            ? $"Force-requeued by {who} past an unacknowledged stall-cancel"
                            : $"Requeued by {who}; acknowledged ({ack.Result}) while the force-requeue was in flight",
                        clearMarkerOnlyForRequestId: pending.RequestId))
                {
                    var current = tryGetJobData(connection, id)?.State;
                    appendAudit("force-requeue-unacknowledged-stall", who, id, request.Reason, "wrong-state",
                        new Dictionary<string, string> { ["CurrentState"] = current ?? "unknown" });
                    return Results.Conflict(new { currentState = current });
                }

                // Identity-scoped retirement of the superseded records (§5 Rule 4): the workflow record
                // advances to its terminal phase (kept — it is the audit trail's durable companion), the
                // surfaced tuple retires, and any straggler pending tuple goes with it. An aborted/faulted
                // ack that landed while this request waited means the risk condition disappeared before
                // commit — recorded as an acknowledged retry, not a forced one.
                LivenessStore.WriteStallAttempt(connection, id, pending with
                {
                    Phase = ack is null ? StallAttemptRecord.PhaseForceRequeued : StallAttemptRecord.PhaseRetried,
                    UpdatedAt = DateTime.UtcNow,
                    Detail = ack is null
                        ? $"Force-requeued by {who}."
                        : $"Acknowledged ({ack.Result}) before the force-requeue committed; requeued by {who}.",
                });
                LivenessStore.RemoveStalledMember(connection, id, pending.ExecutionId);
                LivenessStore.RemoveRetryPendingMember(connection, id, pending.ExecutionId);

                var detail = new Dictionary<string, string>
                {
                    ["ExecutionId"] = pending.ExecutionId,
                    ["RequestId"] = pending.RequestId,
                    ["SourcePhase"] = pending.Phase,
                };
                if (ack is not null) detail["AckResult"] = ack.Result;
                var displayName = jobDisplayName(jobData.Job, jobData.LoadException);
                if (displayName is not null) detail["JobDisplayName"] = displayName;
                var recurringJobId = AuditStore.TryGetRecurringJobId(connection, id);
                if (recurringJobId is not null) detail["RecurringJobId"] = recurringJobId;

                appendAudit("force-requeue-unacknowledged-stall", who, id, request.Reason, "ok", detail);
                return Results.Ok();
            }
        });

        // §3.2: delete is for a terminal run with no body left to stop — Succeeded or Failed only (a
        // queued/scheduled/processing job is stopped via cancel, which carries the reasoned intent a
        // running body needs; delete here is pure history removal). Distinct audit action `delete-run`
        // from the recurring page's own `delete` — different target kind, and old entries must stay
        // unambiguous (§5).
        manage.MapPost("/{id}/delete", (string id, RunActionRequest request, HttpContext http) =>
        {
            if (string.IsNullOrWhiteSpace(request.ExpectedState))
                return Results.BadRequest("expectedState is required.");
            if (!string.Equals(request.ExpectedState, SucceededState.StateName, StringComparison.OrdinalIgnoreCase) &&
                !string.Equals(request.ExpectedState, FailedState.StateName, StringComparison.OrdinalIgnoreCase))
                return Results.BadRequest("expectedState must be Succeeded or Failed — a queued or running job is stopped via cancel, not delete.");

            var who = JobControlEndpoints.actor(http, jobControlOptions);

            JobData? jobData;
            IDictionary<string, string>? stateData;
            using (var connection = JobStorage.Current.GetConnection())
            {
                jobData = tryGetJobData(connection, id);
                // Captured before the state change below, while the job is still in the state we're
                // about to delete it from — reading it afterwards would see Deleted's own state data
                // instead of the Failed exception details this snapshot exists to preserve.
                stateData = jobData is null ? null : connection.GetStateData(id)?.Data;
            }

            if (jobData is null)
            {
                appendAudit("delete-run", who, id, request.Reason, "not-found", detail: null);
                return Results.NotFound(id);
            }

            if (!string.Equals(jobData.State, request.ExpectedState, StringComparison.OrdinalIgnoreCase))
            {
                appendAudit("delete-run", who, id, request.Reason, "wrong-state",
                    new Dictionary<string, string> { ["CurrentState"] = jobData.State ?? "" });
                return Results.Conflict(new { currentState = jobData.State });
            }

            var client = new BackgroundJobClient(JobStorage.Current);
            var changed = client.Delete(id, request.ExpectedState);

            if (!changed)
            {
                string? current;
                using (var connection = JobStorage.Current.GetConnection())
                {
                    current = tryGetJobData(connection, id)?.State;
                }
                appendAudit("delete-run", who, id, request.Reason, "wrong-state",
                    new Dictionary<string, string> { ["CurrentState"] = current ?? "unknown" });
                return Results.Conflict(new { currentState = current });
            }

            // The entry may end up the only surviving record once the job row expires — same rationale
            // as the recurring page's own delete snapshot (+exception summary for Failed).
            var detail = new Dictionary<string, string> { ["FromState"] = request.ExpectedState };
            var displayName = jobDisplayName(jobData.Job, jobData.LoadException);
            if (displayName is not null) detail["JobDisplayName"] = displayName;
            if (string.Equals(request.ExpectedState, FailedState.StateName, StringComparison.OrdinalIgnoreCase) && stateData is not null)
            {
                if (stateData.TryGetValue("ExceptionType", out var excType)) detail["ExceptionType"] = excType;
                if (stateData.TryGetValue("ExceptionMessage", out var excMessage)) detail["ExceptionMessage"] = excMessage;
            }

            using (var connection = JobStorage.Current.GetConnection())
            {
                var recurringJobId = AuditStore.TryGetRecurringJobId(connection, id);
                if (recurringJobId is not null) detail["RecurringJobId"] = recurringJobId;
            }

            appendAudit("delete-run", who, id, request.Reason, "ok", detail);
            return Results.Ok();
        });

        return new JobControlApiGroups(view, manage);
    }

    /// <summary>
    /// Bundled UI only. <paramref name="recurringUiPath"/> feeds the shared cross-nav header's link back
    /// to the Recurring Jobs page — see <see cref="JobControlEndpoints.MapJobControlUi"/> for the mirror.
    /// Prefer <see cref="JobControlEndpoints.MapJobControl"/>, which wires both pages together and gates
    /// this one with the view policy automatically.
    /// </summary>
    public static RouteHandlerBuilder MapJobRunsUi(
        this IEndpointRouteBuilder endpoints,
        string uiPath = DefaultUiPath,
        string apiBase = DefaultApiBase,
        string recurringUiPath = JobControlEndpoints.DefaultUiPath)
    {
        var html = loadUiTemplate()
            .Replace(ApiBasePlaceholder, apiBase)
            .Replace(OwnUiPathPlaceholder, uiPath)
            .Replace(RecurringUiPathPlaceholder, recurringUiPath);
        return endpoints.MapGet(uiPath, () => Results.Content(html, "text/html"));
    }

    private static (int From, int Count) clampPage(int? from, int? count, int defaultCount)
        => (Math.Max(0, from ?? 0), Math.Clamp(count ?? defaultCount, 1, RunsReadLimitHardCap));

    // The Rule-4 guard's single lookup (§5, review B4; OPS-003): the current execution's workflow record
    // when — and only when — its phase is one of the two pre-acknowledgment phases (cancel-requested or
    // blocked) AND the job's current state is that attempt's own committed cancel, so a workflow another
    // transition superseded never guards a job it no longer governs, and every non-liveness job costs
    // exactly one parameter read. Ack presence is deliberately NOT folded in here — it is a separate
    // decision input the two callers read under the same per-job lock and settle differently.
    private static StallAttemptRecord? readGovernedStallCancel(IStorageConnection connection, string jobId)
    {
        var executionId = LivenessStore.ReadCurrentExecutionId(connection, jobId);
        if (executionId is null) return null;

        var attempt = LivenessStore.ReadStallAttempt(connection, jobId, executionId);
        if (attempt is null) return null;
        if (attempt.Phase is not (StallAttemptRecord.PhaseCancelRequested or StallAttemptRecord.PhaseBlocked)) return null;

        return CancelledState.IsCommittedFor(connection, jobId, attempt.RequestId) ? attempt : null;
    }

    // Same Hangfire.PostgreSql 1.20.13 id-shape quirk noted on the GET /{id} handler above
    // (Convert.ToInt64(jobId) unguarded internally) — GetJobData shares the same storage-layer id
    // parsing, so a non-numeric id must be treated as "doesn't exist" here too, not an unhandled 500.
    private static JobData? tryGetJobData(IStorageConnection connection, string jobId)
    {
        try
        {
            return connection.GetJobData(jobId);
        }
        catch (FormatException)
        {
            return null;
        }
    }

    // Generalizes JobControlEndpoints.jobDisplayName (which is keyed off RecurringJobDto specifically)
    // across the several Hangfire monitoring DTOs that each carry their own Job/LoadException pair with
    // no shared interface between them.
    private static string? jobDisplayName(Job? job, JobLoadException? loadException) => (job, loadException) switch
    {
        ({ } j, _) => $"{j.Type.Name}.{j.Method.Name}",
        (_, { InnerException: { } inner }) => inner.Message,
        (_, { } le) => le.Message,
        _ => null,
    };

    private static string loadUiTemplate()
    {
        var assembly = typeof(RunEndpoints).Assembly;
        using var stream = assembly.GetManifestResourceStream(UiResourceName)
            ?? throw new InvalidOperationException(
                $"Embedded resource '{UiResourceName}' not found — check the EmbeddedResource item in the csproj.");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}
