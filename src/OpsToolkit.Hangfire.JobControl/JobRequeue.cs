using Hangfire;
using Hangfire.States;
using Hangfire.Storage;

namespace OpsToolkit.Hangfire.JobControl;

/// <summary>
/// The one requeue implementation, shared by the ordinary requeue endpoint, the break-glass
/// force-requeue, and the stall detector's retry (liveness plan §5 Rule 4: "the shared requeue helper —
/// extracted from the endpoint so the two callers cannot drift"). A requeue is always an expected-state
/// <c>ChangeState</c> — the caller's precondition travels into the transition so a race comes back as
/// <c>false</c>, never as a blind mutation — and always retires the cancel-request marker, so a
/// cancelled-then-requeued job's next run can't inherit a stale request and record a phantom
/// <c>completed-anyway</c> ack (§2.3 marker lifecycle).
/// </summary>
internal static class JobRequeue
{
    /// <summary>
    /// Moves the job to <c>Enqueued</c> — or <c>Scheduled</c> when <paramref name="delay"/> is positive,
    /// so Hangfire's own scheduler owns the wait (a detector or server restart can neither double- nor
    /// re-time it; acceptance test 9). Returns false when the job was no longer in
    /// <paramref name="expectedState"/>. <paramref name="clearMarkerOnlyForRequestId"/> scopes the
    /// marker retirement to one request (OPS-003: a force-requeue overriding an older workflow must not
    /// wipe a newer request's marker — the cell is last-writer-wins); null keeps the blanket clear the
    /// §2.3 marker lifecycle documents for ordinary requeues.
    /// </summary>
    public static bool TryRequeue(
        JobStorage storage, IStorageConnection connection, string jobId, string expectedState,
        TimeSpan? delay = null, string? stateReason = null, string? clearMarkerOnlyForRequestId = null)
    {
        var client = new BackgroundJobClient(storage);
        IState next = delay is { } wait && wait > TimeSpan.Zero
            ? new ScheduledState(wait) { Reason = stateReason }
            : new EnqueuedState { Reason = stateReason };

        if (!client.ChangeState(jobId, next, expectedState)) return false;

        if (clearMarkerOnlyForRequestId is null) CancellationRequestStore.Clear(connection, jobId);
        else CancellationRequestStore.ClearIfRequest(connection, jobId, clearMarkerOnlyForRequestId);
        return true;
    }
}
