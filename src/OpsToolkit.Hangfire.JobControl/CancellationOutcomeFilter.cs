using Hangfire;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;

namespace OpsToolkit.Hangfire.JobControl;

/// <summary>
/// Acknowledgment half of the governed cancellation protocol: the
/// moment a job's execution observably stops because of a governed cancel request — or the honest record
/// that it didn't — recorded as an audit entry and, for an execution-scoped cancel, as a durable
/// <see cref="CancelAckRecord"/> the stall detector's requeue gate reads. Registered by
/// <see cref="GlobalConfigurationExtensions.UseJobControl"/>, same idempotent-guard pattern as
/// <see cref="DisabledRecurringJobFilter"/>.
///
/// <b>An acknowledgment is matched against the committed state, not the marker alone (liveness plan §5
/// Rule 2, review B2):</b> the cancel endpoint and the stall detector both prepare the request marker
/// <i>before</i> the transition and commit the request identity inside <see cref="CancelledState"/>'s
/// own state data. This filter acks only when the job's current state carries a request id the prepared
/// marker matches — so a fast abort can no longer outrun the marker write (the retired near-miss), and a
/// cancel whose transition lost can never be acknowledged. When the committed cancel is
/// execution-scoped, the finishing execution's own identity (from <c>PerformContext.Items</c>, set at
/// liveness enrollment) must match too — attempt A finishing can never acknowledge a request aimed at
/// attempt B (acceptance test 14).
///
/// <c>OnPerforming</c> is a no-op: there is nothing to decide before the body runs. <c>OnPerformed</c>
/// fires with the exception in context even when the body threw — including <see cref="JobAbortedException"/>
/// (mechanic #9) — which is the seam that makes a real acknowledgment observable in-process, at the
/// moment the body stopped.
/// </summary>
public sealed class CancellationOutcomeFilter : IServerFilter
{
    private readonly int _auditMaxEntries;

    public CancellationOutcomeFilter(int auditMaxEntries)
    {
        _auditMaxEntries = auditMaxEntries;
    }

    public void OnPerforming(PerformingContext context)
    {
    }

    public void OnPerformed(PerformedContext context)
    {
        // Never throws: any failure recording an ack must not affect the worker's own exception
        // handling/rethrow, which runs immediately after this filter returns (same rule §2.3 states
        // explicitly). Broad catch is deliberate — every failure mode here is equally "log and move on".
        try
        {
            record(context);
        }
        catch (Exception ex)
        {
            LogProvider.GetLogger(typeof(CancellationOutcomeFilter))
                .ErrorException("Failed to record a cancellation acknowledgment.", ex);
        }
    }

    private void record(PerformedContext context)
    {
        var jobId = context.BackgroundJob.Id;
        var marker = CancellationRequestStore.Read(context.Connection, jobId);
        var committed = readCommittedCancel(context.Connection, jobId);

        if (committed is null)
        {
            // No governed cancel committed this job's current state. A pre-0.5 binary's cancel (marker
            // written after a plain DeletedState — no request id on either side) is still honored during
            // a rolling deploy: the marker alone was that protocol's whole criterion.
            if (marker is { RequestId: null })
            {
                acknowledge(context, jobId, marker, executionScope: null);
                return;
            }

            // Someone else changed the job's state away from Processing (built-in dashboard Delete, a
            // raw BackgroundJob.Delete call, a state clobber from application code). The actor is
            // unknowable at this seam (job filters carry no HTTP context), so the event still enters the
            // activity feed as `abort-observed` instead of vanishing silently.
            if (context.Exception is JobAbortedException)
            {
                append(context, jobId, "abort-observed", actor: "unknown", reason: null,
                    recurringJobIdDetail(AuditStore.TryGetRecurringJobId(context.Connection, jobId)));
            }
            return;
        }

        // A committed cancel with no matching prepared marker is one whose preparation this side can't
        // vouch for (marker overwritten by a later cancel, or foreign state data) — an abort is still
        // honestly observed, but no acknowledgment may be forged for it.
        if (marker is null || !string.Equals(marker.RequestId, committed.Value.RequestId, StringComparison.Ordinal))
        {
            if (context.Exception is JobAbortedException)
            {
                append(context, jobId, "abort-observed", actor: "unknown", reason: null,
                    recurringJobIdDetail(AuditStore.TryGetRecurringJobId(context.Connection, jobId)));
            }
            return;
        }

        // Execution fencing (acceptance test 14): an execution-scoped cancel is acknowledged only by
        // the finishing execution carrying that exact identity — a zombie attempt finishing after
        // native overlap must not settle a request aimed at its replacement.
        if (committed.Value.ExecutionId is not null)
        {
            context.Items.TryGetValue(PerformContextLivenessExtensions.ExecutionIdItemKey, out var raw);
            if (raw is not string finishingExecutionId
                || !string.Equals(finishingExecutionId, committed.Value.ExecutionId, StringComparison.Ordinal))
            {
                return;
            }
        }

        acknowledge(context, jobId, marker, committed.Value.ExecutionId is null
            ? null
            : (committed.Value.ExecutionId, committed.Value.RequestId));
    }

    private void acknowledge(PerformedContext context, string jobId, CancelRequestMarker marker,
        (string ExecutionId, string RequestId)? executionScope)
    {
        // Outcome is always "ok" here — the ack write itself either succeeds or the catch above logs it;
        // the job body's actual fate is the classification in Detail["Result"], per §2.3.
        string result;
        Dictionary<string, string> detail;
        switch (context.Exception)
        {
            case JobAbortedException:
                var elapsedMs = Math.Max(0, (long)(DateTime.UtcNow - marker.At).TotalMilliseconds);
                result = CancelAckRecord.ResultAborted;
                detail = new Dictionary<string, string> { ["Result"] = result, ["ElapsedMs"] = elapsedMs.ToString() };
                break;

            case null:
                // The body ran to completion despite the request — mechanic #6 guarantees the job stays
                // Deleted regardless, so this is evidence the job body isn't cancellation-safe, not a
                // late completion clobbering the cancel.
                result = CancelAckRecord.ResultCompletedAnyway;
                detail = new Dictionary<string, string> { ["Result"] = result };
                break;

            case OperationCanceledException:
                // Deliberate exclusion (§2.3): a shutdown-triggered OperationCanceledException (not an
                // abort) is rethrown as-is by Hangfire and the job is requeued — it will run again, so an
                // ack here would be wrong. The rerun's own OnPerformed settles it instead.
                return;

            default:
                result = CancelAckRecord.ResultFaulted;
                detail = new Dictionary<string, string> { ["Result"] = result, ["Exception"] = context.Exception.GetType().Name };
                break;
        }

        // The durable half first (the detector's requeue gate reads it) — if the audit append then
        // fails, the workflow still settles and only the human feed is short one line.
        if (executionScope is { } scope)
        {
            CancellationRequestStore.WriteAck(context.Connection, jobId, new CancelAckRecord(
                CancelAckRecord.CurrentVersion, scope.RequestId, scope.ExecutionId, result, DateTime.UtcNow));
            detail["ExecutionId"] = scope.ExecutionId;
            detail["RequestId"] = scope.RequestId;
        }

        var recurringJobId = AuditStore.TryGetRecurringJobId(context.Connection, jobId);
        if (recurringJobId is not null) detail["RecurringJobId"] = recurringJobId;
        append(context, jobId, "cancel-ack", marker.By, marker.Reason, detail);
    }

    /// <summary>
    /// The request identity the job's <b>current</b> state data names, when that state is a committed
    /// governed cancel — null otherwise (including when the state moved on again: a request whose
    /// transition no longer governs the job must not be acknowledged).
    /// </summary>
    private static (string RequestId, string? ExecutionId)? readCommittedCancel(IStorageConnection connection, string jobId)
    {
        StateData? stateData;
        try
        {
            stateData = connection.GetStateData(jobId);
        }
        catch (FormatException)
        {
            // The Hangfire.PostgreSql id-shape quirk RunEndpoints documents — "doesn't exist" here.
            return null;
        }

        if (stateData?.Data is null
            || !DeletedState.StateName.Equals(stateData.Name, StringComparison.OrdinalIgnoreCase)
            || !stateData.Data.TryGetValue(CancelledState.RequestIdDataKey, out var requestId)
            || string.IsNullOrEmpty(requestId))
        {
            return null;
        }

        stateData.Data.TryGetValue(CancelledState.ExecutionIdDataKey, out var executionId);
        return (requestId, string.IsNullOrEmpty(executionId) ? null : executionId);
    }

    private static Dictionary<string, string>? recurringJobIdDetail(string? recurringJobId)
        => recurringJobId is null ? null : new Dictionary<string, string> { ["RecurringJobId"] = recurringJobId };

    // The executing context's own storage first (the LivenessFilter convention) — JobStorage.Current is
    // only a fallback, and is unset entirely in filter-level tests and multi-storage hosts.
    private void append(PerformedContext context, string jobId, string action, string actor, string? reason,
        IReadOnlyDictionary<string, string>? detail)
        => AuditStore.Append(context.Storage ?? JobStorage.Current, new AuditEntry(
            AuditEntry.CurrentVersion, DateTime.UtcNow, actor, action, jobId, reason, "ok", detail), _auditMaxEntries);
}
