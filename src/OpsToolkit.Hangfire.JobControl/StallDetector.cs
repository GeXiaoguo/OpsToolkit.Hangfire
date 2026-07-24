using System.Diagnostics;
using System.Reflection;
using Hangfire;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

namespace OpsToolkit.Hangfire.JobControl;

/// <summary>
/// Detection plane of liveness: a Hangfire <see cref="IBackgroundProcess"/> that scans the contracted
/// executions the <see cref="LivenessFilter"/> indexed and flags the ones whose beats stopped. On
/// whenever the server knows about it — there is no separate enable flag; the host's single line is
/// <c>services.AddJobControlStallDetector(jobControl)</c> (or <c>additionalProcesses</c> on a
/// self-hosted <c>BackgroundJobServer</c>). That line exists because of a Hangfire seam, not a product
/// choice: <c>UseJobControl()</c> registers global <i>job filters</i>, and Hangfire offers no way for an
/// <c>IGlobalConfiguration</c> extension to attach a background process to the server.
///
/// A confirmed stall writes a marker, indexes the tuple, and audits <c>stall-detected</c>; a flagged
/// execution whose beats resume is un-flagged and audited <c>stall-recovered</c> — a slow-but-fine job
/// (one long silent DB query) that got flagged is recoverable, which is why flag is the default and
/// auto-anything stays opt-in. For contracts that opted into <see cref="StallAction.Retry"/>, this
/// process also drives the governed retry state machine (liveness plan §5): cancel the hung-but-alive
/// body through the shared cancellation protocol — but only when the owner's server heartbeat is fresh
/// AND the stall has outlived the host-declared <see cref="LivenessOptions.StorageLeaseWindow"/> (Rule
/// 1: never cancel a possibly-dead owner; its crash recovery belongs to the storage) — then requeue
/// <b>only on the cancelled execution's own acknowledgment</b> (Rule 4: no ack ⇒ the job stays Deleted,
/// surfaced as <c>stall-retry-blocked</c>; no queue row is ever added without an ack). The post-cancel
/// workflow is durable: driven by the <c>retry-pending</c> index plus a per-execution
/// <see cref="StallAttemptRecord"/>, it survives detector and server restarts (Rule 3).
///
/// <b>Stall confirmation runs on this detector's own monotonic clock (review C3):</b> it records when
/// <i>it</i> first observed the current beat <c>Seq</c> (Stopwatch-based) and flags only when that
/// <c>Seq</c> has stayed unchanged for the contract's timeout of its own observation. Cross-server
/// clock skew therefore affects display only; a wall-clock jump cannot mass-flag; a storage read
/// failure resets the baseline (a blip can never mass-flag); a detector restart delays — never
/// suppresses — confirmation by at most one timeout plus one scan interval.
/// </summary>
public sealed class StallDetector : IBackgroundProcess
{
    private static readonly Stopwatch MonotonicClock = Stopwatch.StartNew();
    private static readonly ILog Log = LogProvider.GetLogger(typeof(StallDetector));

    /// <summary>
    /// How recent the owner's last server heartbeat must be for cancellation gate (a) — liveness plan
    /// §5 Rule 1: "within 2 × HeartbeatInterval" at Hangfire's default
    /// (<c>BackgroundProcessingServerOptions.HeartbeatInterval</c> = 30s). Deliberately a constant, not
    /// an option: a host that raises its heartbeat interval past this fails the gate <i>conservatively</i>
    /// — no cancel, flag-only — never the unsafe direction.
    /// </summary>
    internal static readonly TimeSpan OwnerHeartbeatFreshnessWindow = TimeSpan.FromSeconds(60);

    private readonly JobControlOptions _options;
    private readonly LivenessOptions _liveness;

    // Per-tuple confirmation baseline: the Seq this instance last saw and when (on its own clock) it
    // first saw it. Instance state, touched only from the process's single dispatcher thread.
    private readonly Dictionary<string, Observation> _observations = new();
    private bool _invisibilityConfigurationChecked;
    private bool _nonSlidingDetected;
    private bool _leaseWindowMissingLogged;
    private bool _nonSlidingDowngradeLoggedThisPass;

    // Server list snapshot for the pass, fetched lazily on the first cancellation-gate check — the
    // common all-healthy pass never pays for it.
    private IList<ServerDto>? _serversThisPass;

    private readonly record struct Observation(long Seq, TimeSpan FirstObservedAt);

    public StallDetector(JobControlOptions? options = null)
    {
        _options = options ?? new JobControlOptions();
        _liveness = _options.Liveness;
        if (_liveness.ScanInterval <= TimeSpan.Zero)
            throw new ArgumentException("LivenessOptions.ScanInterval must be positive.", nameof(options));
    }

    public void Execute(BackgroundProcessContext context)
    {
        try
        {
            Scan(context.Storage, context.ServerId);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // A failed pass advances nothing — no baseline moved, no flag written, and the health lease
            // was not renewed, so a persistently failing detector reads as degraded, not as "no stalls".
            Log.ErrorException("Stall detector scan failed; no liveness state was advanced this pass.", ex);
        }

        context.Wait(_liveness.ScanInterval);
    }

    /// <summary>
    /// One full scan pass — normally driven by <see cref="Execute"/> every
    /// <see cref="LivenessOptions.ScanInterval"/>; public so tests and hosts embedding the detector
    /// outside a Hangfire server can drive passes deterministically. <paramref name="serverId"/> names
    /// the health lease this pass renews on success.
    /// </summary>
    public void Scan(JobStorage storage, string serverId)
    {
        using var connection = storage.GetConnection();
        WarnOnceOnNonSlidingInvisibility(storage);
        _serversThisPass = null;
        _nonSlidingDowngradeLoggedThisPass = false;

        // Both indexes, every pass (review F1): the active index drives detection, the retry-pending
        // index drives every post-cancel workflow phase — a Deleted job leaves the former on the next
        // self-heal, so pending work must never depend on it.
        var activeMembers = LivenessStore.ReadActiveMembers(connection);
        var stalledMembers = new HashSet<string>(LivenessStore.ReadStalledMembers(connection), StringComparer.Ordinal);
        var retryPendingMembers = LivenessStore.ReadRetryPendingMembers(connection);

        // Baselines for tuples that left the active index are dead weight — drop them so the map can't
        // grow past the live contract count.
        var activeSet = new HashSet<string>(activeMembers, StringComparer.Ordinal);
        foreach (var gone in _observations.Keys.Where(member => !activeSet.Contains(member)).ToList())
            _observations.Remove(gone);

        foreach (var member in activeMembers)
        {
            var tuple = LivenessStore.TryParseActiveMember(member);
            if (tuple is null) continue; // foreign/corrupt member — not ours to interpret, never flagged

            try
            {
                ProcessActiveMember(connection, storage, member, tuple.Value.JobId, tuple.Value.ExecutionId,
                    stalledMembers.Contains(member));
            }
            catch (Exception ex)
            {
                // Review C3/AT18: a storage failure mid-tuple resets that tuple's baseline — confirmation
                // restarts rather than counting silence it couldn't verify. A blip can never mass-flag.
                _observations.Remove(member);
                Log.WarnException($"Stall scan of {member} failed; its confirmation baseline was reset.", ex);
            }
        }

        // The post-cancel workflow (liveness plan §5 Rules 3-4): ack-gated requeue, blocking, exhaustion,
        // supersession — each tuple independently, so one bad row can't stall the others.
        foreach (var member in retryPendingMembers)
        {
            var tuple = LivenessStore.TryParseActiveMember(member);
            if (tuple is null) continue;

            try
            {
                ProcessRetryPendingMember(connection, storage, tuple.Value.JobId, tuple.Value.ExecutionId);
            }
            catch (Exception ex)
            {
                Log.WarnException($"Stall-retry workflow pass for {member} failed; retried next pass.", ex);
            }
        }

        // Stalled-index entries whose tuple is no longer active: terminal-state leftovers (a crash
        // between EndContract's two set removals), foreign writes — or, deliberately retained, the §5
        // state machine's surfaced Deleted phases (cancel-requested/blocked/exhausted stay visible on
        // the stalled surface for as long as our committed cancel still governs the job).
        foreach (var member in stalledMembers.Where(m => !activeSet.Contains(m)))
        {
            var tuple = LivenessStore.TryParseActiveMember(member);
            if (tuple is null) continue;

            try
            {
                if (IsProcessingUnderExecution(connection, tuple.Value.JobId, tuple.Value.ExecutionId)) continue;
                if (IsSurfacedTerminalStall(connection, tuple.Value.JobId, tuple.Value.ExecutionId)) continue;
                LivenessStore.RemoveStalledMember(connection, tuple.Value.JobId, tuple.Value.ExecutionId);
            }
            catch (Exception ex)
            {
                Log.WarnException($"Self-heal of stalled index entry {member} failed; retried next pass.", ex);
            }
        }

        // Renewed only after the pass succeeded — the lease carries the last *successful* scan, so a
        // detector that runs but always fails still reads as degraded (review F5).
        LivenessStore.RenewDetectorLease(connection, new DetectorLease(
            DetectorLease.CurrentVersion, serverId, DateTime.UtcNow, (int)Math.Max(1, _liveness.ScanInterval.TotalSeconds)));
        LivenessStore.PruneDetectorIndex(connection);
    }

    private void ProcessActiveMember(
        IStorageConnection connection, JobStorage storage, string member, string jobId, string executionId, bool isFlagged)
    {
        // Self-heal, scoped by execution identity: covers a worker crash where OnPerformed never ran, a
        // cancelled zombie whose entry lingers, and an execution superseded by storage-native re-fetch
        // (the pointer is written only at contract start, so pointer ≠ tuple means a newer attempt owns
        // this job id now).
        if (!IsProcessingUnderExecution(connection, jobId, executionId))
        {
            // The stalled entry follows the §5 retention rule rather than being retired unconditionally
            // with the contract: a stall-cancelled execution stays surfaced while its post-cancel
            // workflow (cancel-requested/blocked/exhausted) still governs the job.
            if (IsSurfacedTerminalStall(connection, jobId, executionId))
                LivenessStore.RemoveActiveMember(connection, jobId, executionId);
            else
                LivenessStore.EndContract(connection, jobId, executionId);
            _observations.Remove(member);
            return;
        }

        var beat = LivenessStore.ReadBeat(connection, jobId, executionId);
        if (beat is null)
        {
            // Unreadable beat — same baseline-reset rule as a thrown storage failure.
            _observations.Remove(member);
            return;
        }

        var now = MonotonicClock.Elapsed;
        if (_observations.TryGetValue(member, out var observation) && observation.Seq == beat.Seq)
        {
            if (!isFlagged && now - observation.FirstObservedAt >= TimeSpan.FromSeconds(beat.TimeoutSeconds))
                FlagStalled(connection, storage, jobId, executionId, beat.Seq);
        }
        else
        {
            // First sight of this Seq — even one the wall clock says is ancient. Confirmation counts
            // from this instant on this detector's own clock (review C3).
            _observations[member] = new Observation(beat.Seq, now);
        }

        if (!isFlagged) return;

        var marker = LivenessStore.ReadStall(connection, jobId, executionId);
        if (marker is null)
        {
            // Index entry without a marker — the crash window ClearStall documents. Self-heal.
            LivenessStore.RemoveStalledMember(connection, jobId, executionId);
        }
        else if (beat.Seq != marker.Seq)
        {
            Recover(connection, storage, jobId, executionId, marker);
        }
        else
        {
            // Still flagged, still silent: apply the contract snapshot's OnStall policy (§4.5). For
            // Flag (the default) this is a no-op; for Retry it evaluates the Rule-1 cancellation gates.
            TryApplyRetryPolicy(connection, storage, jobId, executionId, beat, marker);
        }
    }

    /// <summary>
    /// A stall phase transition (review F4): the flag is written under the per-job lock with every input
    /// re-read inside it, and audited only after the guarded writes succeeded. Lock acquisition uses a
    /// short timeout; failure skips the job for this scan — another detector owns it.
    /// </summary>
    private void FlagStalled(IStorageConnection connection, JobStorage storage, string jobId, string executionId, long observedSeq)
    {
        IDisposable lockHandle;
        try
        {
            lockHandle = connection.AcquireDistributedLock(LivenessStore.JobLockResource(jobId), TimeSpan.FromSeconds(2));
        }
        catch (DistributedLockTimeoutException)
        {
            return;
        }

        using (lockHandle)
        {
            if (!IsProcessingUnderExecution(connection, jobId, executionId)) return;

            var beat = LivenessStore.ReadBeat(connection, jobId, executionId);
            if (beat is null || beat.Seq != observedSeq) return; // a beat landed in between — not stalled

            var existing = LivenessStore.ReadStall(connection, jobId, executionId);
            if (existing is not null)
            {
                // Another detector won the flag race (or a prior flag crashed before indexing) — repair
                // the index idempotently, but the transition and its audit line are theirs.
                LivenessStore.AddStalledMember(connection, jobId, executionId);
                return;
            }

            LivenessStore.WriteStall(connection, jobId, new StallMarker(
                StallMarker.CurrentVersion, executionId, DateTime.UtcNow, observedSeq,
                AcknowledgedBy: null, AcknowledgedAt: null, AcknowledgeReason: null));
            LivenessStore.AddStalledMember(connection, jobId, executionId);

            TryAudit(storage, connection, "stall-detected", jobId,
                $"No heartbeat for at least {beat.TimeoutSeconds}s of detector observation (seq {observedSeq} unchanged).",
                executionId);
        }
    }

    /// <summary>A beat advanced past the flagged <see cref="StallMarker.Seq"/> — retire the flag (same F4 locking rule).</summary>
    private void Recover(IStorageConnection connection, JobStorage storage, string jobId, string executionId, StallMarker flagged)
    {
        IDisposable lockHandle;
        try
        {
            lockHandle = connection.AcquireDistributedLock(LivenessStore.JobLockResource(jobId), TimeSpan.FromSeconds(2));
        }
        catch (DistributedLockTimeoutException)
        {
            return;
        }

        using (lockHandle)
        {
            var marker = LivenessStore.ReadStall(connection, jobId, executionId);
            if (marker is null) return; // someone else already retired it — theirs to audit

            var beat = LivenessStore.ReadBeat(connection, jobId, executionId);
            if (beat is null || beat.Seq == marker.Seq) return;

            LivenessStore.ClearStall(connection, jobId, executionId);
            TryAudit(storage, connection, "stall-recovered", jobId,
                $"Heartbeat resumed (seq advanced past {marker.Seq}) after the execution was flagged stalled at {flagged.StalledAt:O}.",
                executionId);
        }
    }

    /// <summary>
    /// The <see cref="StallAction.Retry"/> entry point — evaluates, in order: the policy itself, the
    /// once-per-execution rule (a blocked workflow is a human-only exit, never re-cancelled), the two
    /// configuration downgrades (positively non-sliding storage; undeclared
    /// <see cref="LivenessOptions.StorageLeaseWindow"/>), then the two Rule-1 cancellation gates. All
    /// refusals leave the execution flagged — the safe direction is always "don't cancel".
    /// </summary>
    private void TryApplyRetryPolicy(
        IStorageConnection connection, JobStorage storage, string jobId, string executionId, BeatRecord beat, StallMarker marker)
    {
        if (beat.OnStall != StallAction.Retry) return;
        if (LivenessStore.ReadStallAttempt(connection, jobId, executionId) is not null) return;

        if (_nonSlidingDetected)
        {
            // Resolved question 1: positive detection ⇒ retry ignored, flag-only, error-logged every
            // scan cycle (once per pass — per-job repetition adds nothing).
            if (!_nonSlidingDowngradeLoggedThisPass)
            {
                _nonSlidingDowngradeLoggedThisPass = true;
                Log.Error("StallAction.Retry is downgraded to flag-only: storage invisibility is NOT sliding, so a " +
                          "cancel + requeue would race the storage's own re-fetch of the running job. " +
                          "Configure sliding invisibility; see HangfireLiveness.md.");
            }
            return;
        }

        if (_liveness.StorageLeaseWindow is not TimeSpan leaseWindow)
        {
            if (!_leaseWindowMissingLogged)
            {
                _leaseWindowMissingLogged = true;
                Log.Error("StallAction.Retry is downgraded to flag-only: LivenessOptions.StorageLeaseWindow is not set. " +
                          "Declare it (at least the storage invisibility timeout) to enable governed stall retry; " +
                          "see HangfireLiveness.md.");
            }
            return;
        }

        // Rule 1 gate (b): the stall must outlive the storage's own recovery window — a dead owner
        // would already have been replaced by native re-fetch within it, so surviving it under fresh
        // heartbeats implies hung-but-alive: the only case cancellation is for.
        if (DateTime.UtcNow - marker.StalledAt < leaseWindow) return;

        // Rule 1 gate (a): owner absent from the server list, or its heartbeat stale → flag only,
        // defer to native recovery. Presence alone proves nothing (server records are leases the
        // watchdog removes only after ServerTimeout) — freshness is what's required.
        if (!IsOwnerHeartbeatFresh(storage, beat.ServerId)) return;

        BeginStallCancel(connection, storage, jobId, executionId, marker);
    }

    /// <summary>
    /// Issues the governed stall-cancel (liveness plan §5 Rule 2) — a stall phase transition, so it
    /// runs under the per-job lock with every input re-read inside. Ordering is the protocol: workflow
    /// record and request marker <b>prepared</b> first, the retry-pending tuple indexed, and only then
    /// the expected-state transition whose own state data commits the request identity. A lost
    /// transition retires everything prepared (Rule 2 step 4) — no ack is ever valid for a cancel that
    /// never won.
    /// </summary>
    private void BeginStallCancel(
        IStorageConnection connection, JobStorage storage, string jobId, string executionId, StallMarker marker)
    {
        IDisposable lockHandle;
        try
        {
            lockHandle = connection.AcquireDistributedLock(LivenessStore.JobLockResource(jobId), TimeSpan.FromSeconds(2));
        }
        catch (DistributedLockTimeoutException)
        {
            return;
        }

        using (lockHandle)
        {
            if (!IsProcessingUnderExecution(connection, jobId, executionId)) return;

            var beat = LivenessStore.ReadBeat(connection, jobId, executionId);
            var flagged = LivenessStore.ReadStall(connection, jobId, executionId);
            if (beat is null || flagged is null || beat.Seq != flagged.Seq) return; // recovered/moved in between
            if (LivenessStore.ReadStallAttempt(connection, jobId, executionId) is not null) return;

            var now = DateTime.UtcNow;
            var requestId = Guid.NewGuid().ToString("N");
            var retriesSpent = LivenessStore.ReadStallRetryCount(connection, jobId);
            var reason = $"No heartbeat for at least {beat.TimeoutSeconds}s of detector observation; " +
                         $"governed stall-cancel (would be retry {retriesSpent + 1} of {beat.MaxRetries}).";

            LivenessStore.WriteStallAttempt(connection, jobId, new StallAttemptRecord(
                StallAttemptRecord.CurrentVersion, executionId, requestId, StallAttemptRecord.PhaseCancelRequested,
                AttemptNumber: retriesSpent + 1, beat.MaxRetries, beat.RetryDelaySeconds,
                CancelRequestedAt: now, UpdatedAt: now, Detail: null));
            CancellationRequestStore.Write(connection, jobId, _liveness.ActorName, now, reason, requestId, executionId);
            LivenessStore.AddRetryPendingMember(connection, jobId, executionId);

            var committed = new BackgroundJobClient(storage).ChangeState(
                jobId,
                new CancelledState(requestId, executionId) { Reason = $"Cancelled by {_liveness.ActorName}: stalled (no heartbeat)" },
                ProcessingState.StateName);

            if (!committed)
            {
                // Rule 2 step 4: the transition lost (a body failure, recovery, or another actor won the
                // same instant) — retire the prepared request entirely; the next pass re-reads the world.
                LivenessStore.ClearStallAttempt(connection, jobId, executionId);
                LivenessStore.RemoveRetryPendingMember(connection, jobId, executionId);
                CancellationRequestStore.ClearIfRequest(connection, jobId, requestId);
                return;
            }

            TryAudit(storage, connection, "cancel", jobId, reason, executionId,
                new Dictionary<string, string> { ["RequestId"] = requestId });
        }
    }

    /// <summary>
    /// One pass of the durable post-cancel workflow for one tuple (liveness plan §5 Rules 3-5). The
    /// cheap reads run lock-free; every actual phase transition re-reads its inputs under the per-job
    /// lock (review F4). The decision tree, in order: orphaned/terminal records self-heal; a cancel
    /// whose transition never committed is retired (Rule 2 step 4) or, if another state won, superseded
    /// (Rule 5 — identity-scoped cleanup only, never a stall retry on top of an exception retry); a
    /// committed cancel waits for its ack within <see cref="LivenessOptions.AckGracePeriod"/> under a
    /// fresh owner, blocks otherwise (Rule 4 — no queue row without an ack), and on a matching ack
    /// either requeues through the shared helper or exhausts against the snapshot's budget.
    /// </summary>
    private void ProcessRetryPendingMember(IStorageConnection connection, JobStorage storage, string jobId, string executionId)
    {
        var attempt = LivenessStore.ReadStallAttempt(connection, jobId, executionId);
        if (attempt is null || attempt.Phase != StallAttemptRecord.PhaseCancelRequested)
        {
            // Orphaned tuple (a crash between a terminal phase write and this set removal) — self-heal.
            LivenessStore.RemoveRetryPendingMember(connection, jobId, executionId);
            return;
        }

        if (TryGetJobData(connection, jobId) is null)
        {
            // The job row expired or was removed — nothing left to govern.
            LivenessStore.RemoveRetryPendingMember(connection, jobId, executionId);
            LivenessStore.RemoveStalledMember(connection, jobId, executionId);
            return;
        }

        if (!IsCommittedCancel(connection, jobId, attempt))
        {
            ResolveUncommittedCancel(connection, jobId, executionId, attempt);
            return;
        }

        var ack = CancellationRequestStore.ReadAck(connection, jobId, executionId, attempt.RequestId);
        if (ack is null)
        {
            var beat = LivenessStore.ReadBeat(connection, jobId, executionId);
            var ownerFresh = IsOwnerHeartbeatFresh(storage, beat?.ServerId);
            var graceExpired = DateTime.UtcNow - attempt.CancelRequestedAt > _liveness.AckGracePeriod;
            if (ownerFresh && !graceExpired) return; // keep waiting — the watcher may not have fired yet

            Block(connection, storage, jobId, executionId, attempt, ownerFresh
                ? $"No acknowledgment within the {(int)_liveness.AckGracePeriod.TotalSeconds}s grace period — the body is likely hung beyond the reach of cancellation."
                : "The owning server went absent or its heartbeat stale after the cancel committed — the execution's fate is unknowable.");
            return;
        }

        if (ack.Result == CancelAckRecord.ResultCompletedAnyway)
        {
            // The body finished its work despite the cancel — re-running it would duplicate a completed
            // run, so the workflow ends here; the cancel-ack audit entry is the record.
            RunUnderJobLock(connection, jobId, () =>
            {
                var current = LivenessStore.ReadStallAttempt(connection, jobId, executionId);
                if (current is null || current.Phase != StallAttemptRecord.PhaseCancelRequested) return;
                LivenessStore.WriteStallAttempt(connection, jobId, current with
                {
                    Phase = StallAttemptRecord.PhaseCompletedAnyway,
                    UpdatedAt = DateTime.UtcNow,
                });
                LivenessStore.RemoveRetryPendingMember(connection, jobId, executionId);
                LivenessStore.RemoveStalledMember(connection, jobId, executionId);
            });
            return;
        }

        // aborted | faulted — the execution observably stopped; the requeue decision (Rule 4).
        RunUnderJobLock(connection, jobId, () =>
        {
            var current = LivenessStore.ReadStallAttempt(connection, jobId, executionId);
            if (current is null || current.Phase != StallAttemptRecord.PhaseCancelRequested) return;
            if (!IsCommittedCancel(connection, jobId, current)) return; // moved again — next pass resolves

            if (current.AttemptNumber > current.MaxRetries)
            {
                LivenessStore.WriteStallAttempt(connection, jobId, current with
                {
                    Phase = StallAttemptRecord.PhaseExhausted,
                    UpdatedAt = DateTime.UtcNow,
                    Detail = $"Retry budget spent ({current.MaxRetries}).",
                });
                LivenessStore.RemoveRetryPendingMember(connection, jobId, executionId);
                LivenessStore.AddStalledMember(connection, jobId, executionId); // surfaced until it exits by hand or expiry
                TryAudit(storage, connection, "stall-retry-exhausted", jobId,
                    $"Stall-cancel acknowledged ({ack.Result}), but the retry budget ({current.MaxRetries}) is spent — the job stays Deleted.",
                    executionId, new Dictionary<string, string> { ["RequestId"] = current.RequestId });
                return;
            }

            // Budget write first and idempotently (the count IS this attempt's number): a crash between
            // it and the requeue re-runs this decision next pass with the same outcome, never a double
            // increment.
            LivenessStore.WriteStallRetryCount(connection, jobId, current.AttemptNumber);

            var delay = TimeSpan.FromSeconds(current.RetryDelaySeconds);
            var requeued = JobRequeue.TryRequeue(storage, connection, jobId, DeletedState.StateName, delay,
                $"Stall retry {current.AttemptNumber} of {current.MaxRetries} by {_liveness.ActorName}");
            if (!requeued) return; // another actor moved the job first — superseded on the next pass

            LivenessStore.WriteStallAttempt(connection, jobId, current with
            {
                Phase = StallAttemptRecord.PhaseRetried,
                UpdatedAt = DateTime.UtcNow,
            });
            LivenessStore.RemoveRetryPendingMember(connection, jobId, executionId);
            LivenessStore.RemoveStalledMember(connection, jobId, executionId);
            TryAudit(storage, connection, "stall-retry", jobId,
                $"Stall-cancel acknowledged ({ack.Result}); retry {current.AttemptNumber} of {current.MaxRetries}" +
                (current.RetryDelaySeconds > 0 ? $" scheduled in {current.RetryDelaySeconds}s." : " enqueued."),
                executionId, new Dictionary<string, string> { ["RequestId"] = current.RequestId });
        });
    }

    /// <summary>
    /// A pending tuple whose cancel isn't the job's current state: either the transition never happened
    /// (crash inside the prepare window — retire the request, Rule 2 step 4, and let the still-standing
    /// flag re-evaluate next pass) or another Hangfire transition won (Rule 5/review F6 — Superseded:
    /// identity-scoped cleanup of our own pending data only, the winner is never touched).
    /// </summary>
    private void ResolveUncommittedCancel(IStorageConnection connection, string jobId, string executionId, StallAttemptRecord attempt)
    {
        RunUnderJobLock(connection, jobId, () =>
        {
            var current = LivenessStore.ReadStallAttempt(connection, jobId, executionId);
            if (current is null || current.Phase != StallAttemptRecord.PhaseCancelRequested) return;
            if (IsCommittedCancel(connection, jobId, current)) return; // landed after all — next pass proceeds

            if (IsProcessingUnderExecution(connection, jobId, executionId))
            {
                LivenessStore.ClearStallAttempt(connection, jobId, executionId);
                LivenessStore.RemoveRetryPendingMember(connection, jobId, executionId);
                CancellationRequestStore.ClearIfRequest(connection, jobId, current.RequestId);
                return;
            }

            LivenessStore.WriteStallAttempt(connection, jobId, current with
            {
                Phase = StallAttemptRecord.PhaseSuperseded,
                UpdatedAt = DateTime.UtcNow,
                Detail = "Another state transition won against the stall-cancel.",
            });
            LivenessStore.RemoveRetryPendingMember(connection, jobId, executionId);
            LivenessStore.RemoveStalledMember(connection, jobId, executionId);
            CancellationRequestStore.ClearIfRequest(connection, jobId, current.RequestId);
        });
    }

    /// <summary>The Rule-4 blocked terminal: surfaced, audited, human-only exit — and no queue row, ever.</summary>
    private void Block(
        IStorageConnection connection, JobStorage storage, string jobId, string executionId,
        StallAttemptRecord attempt, string why)
    {
        RunUnderJobLock(connection, jobId, () =>
        {
            var current = LivenessStore.ReadStallAttempt(connection, jobId, executionId);
            if (current is null || current.Phase != StallAttemptRecord.PhaseCancelRequested) return;
            if (CancellationRequestStore.ReadAck(connection, jobId, executionId, current.RequestId) is not null)
                return; // the ack landed while we deliberated — next pass requeues instead

            LivenessStore.WriteStallAttempt(connection, jobId, current with
            {
                Phase = StallAttemptRecord.PhaseBlocked,
                UpdatedAt = DateTime.UtcNow,
                Detail = why,
            });
            LivenessStore.RemoveRetryPendingMember(connection, jobId, executionId);
            LivenessStore.AddStalledMember(connection, jobId, executionId);
            TryAudit(storage, connection, "stall-retry-blocked", jobId, why, executionId,
                new Dictionary<string, string> { ["RequestId"] = current.RequestId });
        });
    }

    private void RunUnderJobLock(IStorageConnection connection, string jobId, Action guarded)
    {
        IDisposable lockHandle;
        try
        {
            lockHandle = connection.AcquireDistributedLock(LivenessStore.JobLockResource(jobId), TimeSpan.FromSeconds(2));
        }
        catch (DistributedLockTimeoutException)
        {
            return; // another detector owns this job for now
        }

        using (lockHandle)
        {
            guarded();
        }
    }

    /// <summary>Whether the job's current state is <b>this</b> attempt's committed cancel (state data is the commit record, §5 Rule 2).</summary>
    private static bool IsCommittedCancel(IStorageConnection connection, string jobId, StallAttemptRecord attempt)
        => CancelledState.IsCommittedFor(connection, jobId, attempt.RequestId);

    /// <summary>
    /// The stalled-index retention rule for non-Processing entries: a tuple stays surfaced exactly while
    /// its execution's workflow record is in a surfaced phase <b>and</b> our committed cancel still
    /// governs the job's state — an expiry, a requeue, or any foreign transition retires it on the next
    /// sweep.
    /// </summary>
    private static bool IsSurfacedTerminalStall(IStorageConnection connection, string jobId, string executionId)
    {
        var attempt = LivenessStore.ReadStallAttempt(connection, jobId, executionId);
        if (attempt is null) return false;
        if (attempt.Phase is not (StallAttemptRecord.PhaseCancelRequested
            or StallAttemptRecord.PhaseBlocked
            or StallAttemptRecord.PhaseExhausted))
        {
            return false;
        }

        return IsCommittedCancel(connection, jobId, attempt);
    }

    private bool IsOwnerHeartbeatFresh(JobStorage storage, string? serverId)
    {
        if (string.IsNullOrEmpty(serverId)) return false;

        _serversThisPass ??= storage.GetMonitoringApi().Servers();
        var owner = _serversThisPass.FirstOrDefault(server => string.Equals(server.Name, serverId, StringComparison.Ordinal));
        return owner?.Heartbeat is DateTime heartbeat && DateTime.UtcNow - heartbeat <= OwnerHeartbeatFreshnessWindow;
    }

    private static bool IsProcessingUnderExecution(IStorageConnection connection, string jobId, string executionId)
    {
        var jobData = TryGetJobData(connection, jobId);
        if (jobData is null || !ProcessingState.StateName.Equals(jobData.State, StringComparison.OrdinalIgnoreCase))
            return false;

        return string.Equals(LivenessStore.ReadCurrentExecutionId(connection, jobId), executionId, StringComparison.Ordinal);
    }

    // Same Hangfire.PostgreSql id-shape quirk RunEndpoints guards (Convert.ToInt64 unguarded inside
    // GetJobData): a member carrying an id that storage can't parse means "doesn't exist" here, not a
    // scan-killing exception.
    private static JobData? TryGetJobData(IStorageConnection connection, string jobId)
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

    private void TryAudit(JobStorage storage, IStorageConnection connection, string action, string jobId, string reason, string executionId,
        Dictionary<string, string>? extraDetail = null)
    {
        try
        {
            var detail = extraDetail ?? new Dictionary<string, string>();
            detail["ExecutionId"] = executionId;
            var job = TryGetJobData(connection, jobId)?.Job;
            if (job is not null) detail["Method"] = $"{job.Type.Name}.{job.Method.Name}";
            var recurringJobId = AuditStore.TryGetRecurringJobId(connection, jobId);
            if (recurringJobId is not null) detail["RecurringJobId"] = recurringJobId;

            AuditStore.Append(storage, new AuditEntry(
                AuditEntry.CurrentVersion, DateTime.UtcNow, _liveness.ActorName, action, jobId, reason, "ok", detail),
                _options.AuditMaxEntries);
        }
        catch (Exception ex)
        {
            Log.ErrorException($"Failed to record a {action} audit entry for job {jobId}.", ex);
        }
    }

    /// <summary>
    /// Best-effort reflection check of the host prerequisite (§1 of the liveness plan): without sliding
    /// invisibility, the storage re-fetches any job running past its fixed invisibility timeout —
    /// duplicating it regardless of beats. A <b>positive</b> non-sliding detection additionally
    /// downgrades <c>StallAction.Retry</c> to flag-only (resolved question 1: a downgrade with a
    /// repeated error log, never a crash); an inconclusive read warns once and assumes nothing — too
    /// weak a signal to withhold a host-declared policy on. Reflection because neither provider exposes
    /// its options through a core interface.
    /// </summary>
    private void WarnOnceOnNonSlidingInvisibility(JobStorage storage)
    {
        if (_invisibilityConfigurationChecked) return;
        _invisibilityConfigurationChecked = true;

        try
        {
            var options = storage.GetType()
                .GetProperty("Options", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)?
                .GetValue(storage);
            if (options is null)
            {
                Log.Info("Could not determine the storage invisibility configuration (no Options property) — " +
                         "verify sliding invisibility manually before relying on liveness for long-running jobs.");
                return;
            }

            // Hangfire.PostgreSql: bool UseSlidingInvisibilityTimeout (default false).
            if (options.GetType().GetProperty("UseSlidingInvisibilityTimeout")?.GetValue(options) is bool sliding)
            {
                if (!sliding)
                {
                    _nonSlidingDetected = true;
                    WarnNonSliding("UseSlidingInvisibilityTimeout is false");
                }
                return;
            }

            // Hangfire.SqlServer: TimeSpan? SlidingInvisibilityTimeout (null = transaction-based fetch).
            var slidingTimeoutProperty = options.GetType().GetProperty("SlidingInvisibilityTimeout");
            if (slidingTimeoutProperty is not null)
            {
                if (slidingTimeoutProperty.GetValue(options) is null)
                {
                    _nonSlidingDetected = true;
                    WarnNonSliding("SlidingInvisibilityTimeout is not set");
                }
                return;
            }

            Log.Info($"Could not determine the invisibility configuration of {storage.GetType().Name} — " +
                     "verify sliding invisibility manually before relying on liveness for long-running jobs.");
        }
        catch (Exception ex)
        {
            Log.InfoException("Storage invisibility configuration check failed; treating it as inconclusive.", ex);
        }

        static void WarnNonSliding(string finding)
            => Log.Warn($"Storage invisibility is NOT sliding ({finding}). Any job running longer than the fixed " +
                        "invisibility timeout is handed to a second worker — duplicated regardless of heartbeats. " +
                        "Long-running jobs require sliding invisibility; see HangfireLiveness.md.");
    }
}
