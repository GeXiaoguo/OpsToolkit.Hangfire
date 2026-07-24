using Hangfire;
using Hangfire.Common;
using Hangfire.PostgreSql;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using Shouldly;
using Xunit;

namespace OpsToolkit.Hangfire.JobControl.Tests;

// Drives the §5 retry state machine (liveness plan) end-to-end against real Postgres-backed storage:
// flag → Rule-1 gates → linearized cancel (CancelledState commit record) → the execution's own
// acknowledgment through the real CancellationOutcomeFilter → ack-gated requeue / blocked / exhausted /
// superseded. Same seeding conventions as StallDetectorTests (short-timeout contracts via
// LivenessStore.StartContract, Processing-named IState with no queue row); additionally the owner
// "server" is announced through the core connection API so the Rule-1 owner-freshness gate has a real
// monitoring-API heartbeat to read. Storage is built with UseSlidingInvisibilityTimeout = true — the §1
// host prerequisite — except where a test exercises the non-sliding downgrade itself.
public class StallRetryTests
{
    private const int TimeoutSeconds = 2;
    private static readonly TimeSpan PastTimeout = TimeSpan.FromSeconds(TimeoutSeconds + 0.4);
    private static readonly TimeSpan LeaseWindow = TimeSpan.FromMilliseconds(500);

    private static JobStorage buildStorage(bool sliding = true)
        => new PostgreSqlStorage(buildConnectionString(), new PostgreSqlStorageOptions
        {
            PrepareSchemaIfNecessary = true,
            UseSlidingInvisibilityTimeout = sliding,
        });

    private static string buildConnectionString()
    {
        string env(string name, string fallback) => Environment.GetEnvironmentVariable(name) ?? fallback;
        var host = env("OPSTOOLKIT_HANGFIRE_TEST_PG_HOST", "localhost");
        var port = env("OPSTOOLKIT_HANGFIRE_TEST_PG_PORT", "5434");
        var database = env("OPSTOOLKIT_HANGFIRE_TEST_PG_DATABASE", "opstoolkit_hangfire");
        var username = env("OPSTOOLKIT_HANGFIRE_TEST_PG_USERNAME", "postgres");
        var password = env("OPSTOOLKIT_HANGFIRE_TEST_PG_PASSWORD", "postgres");
        return $"host={host};port={port};database={database};username={username};password={password}";
    }

    private static StallDetector buildDetector(TimeSpan? leaseWindow = null, TimeSpan? ackGrace = null) => new(new JobControlOptions
    {
        Liveness = new LivenessOptions
        {
            ScanInterval = TimeSpan.FromMilliseconds(200),
            StorageLeaseWindow = leaseWindow,
            AckGracePeriod = ackGrace ?? TimeSpan.FromSeconds(60),
        },
    });

    private static string uniqueServerId() => $"unit-test-detector-{Guid.NewGuid():N}";

    /// <summary>A real server record with a fresh heartbeat — what the Rule-1 owner-freshness gate reads via the monitoring API.</summary>
    private static string announceFreshOwner(IStorageConnection connection)
    {
        var serverId = $"unit-test-owner-{Guid.NewGuid():N}";
        connection.AnnounceServer(serverId, new ServerContext { WorkerCount = 1, Queues = new[] { "default" } });
        connection.Heartbeat(serverId);
        return serverId;
    }

    private static string createProcessingJob(IStorageConnection connection)
    {
        var jobId = connection.CreateExpiredJob(
            Job.FromExpression<RetryFixtures>(f => f.LongRunning()),
            new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromHours(1));
        setState(connection, jobId, new ProcessingLikeState());
        return jobId;
    }

    private static void setState(IStorageConnection connection, string jobId, IState state)
    {
        using var transaction = connection.CreateWriteTransaction();
        transaction.SetJobState(jobId, state);
        transaction.Commit();
    }

    private static BeatRecord seedRetryContract(
        IStorageConnection connection, string jobId, string? serverId, int maxRetries = 3, int retryDelaySeconds = 0)
    {
        var now = DateTime.UtcNow;
        var record = new BeatRecord(
            BeatRecord.CurrentVersion, Guid.NewGuid().ToString("N"), StartedAt: now,
            TimeoutSeconds, ServerId: serverId, Seq: 1, BeatAt: now, Percent: null, Message: null,
            OnStall: StallAction.Retry, MaxRetries: maxRetries, RetryDelaySeconds: retryDelaySeconds);
        LivenessStore.StartContract(connection, jobId, record);
        return record;
    }

    /// <summary>Scan until flagged, then wait out the StorageLeaseWindow gate and scan again — the cancel pass.</summary>
    private static void flagThenCancel(StallDetector detector, JobStorage storage, string serverId)
    {
        detector.Scan(storage, serverId);          // baseline observation
        Thread.Sleep(PastTimeout);
        detector.Scan(storage, serverId);          // flag (policy gates refuse: stall too young)
        Thread.Sleep(LeaseWindow + TimeSpan.FromMilliseconds(200));
        detector.Scan(storage, serverId);          // gates pass — cancel commits
    }

    /// <summary>
    /// The real acknowledgment path: a PerformedContext carrying the finishing execution's identity in
    /// Items (as LivenessFilter enrollment does) through the real filters, in the worker's OnPerformed
    /// order (LivenessFilter retires the contract, CancellationOutcomeFilter records the ack).
    /// </summary>
    private static void driveOnPerformed(
        JobStorage storage, IStorageConnection connection, string jobId, string? itemsExecutionId, Exception? exception)
    {
        var job = Job.FromExpression<RetryFixtures>(f => f.LongRunning());
        var performContext = new PerformContext(
            storage, connection, new BackgroundJob(jobId, job, DateTime.UtcNow), new JobCancellationToken(false));
        if (itemsExecutionId is not null)
            performContext.Items[PerformContextLivenessExtensions.ExecutionIdItemKey] = itemsExecutionId;
        var performed = new PerformedContext(performContext, result: null, canceled: false, exception: exception);

        new LivenessFilter(auditMaxEntries: 1000).OnPerformed(performed);
        new CancellationOutcomeFilter(auditMaxEntries: 1000).OnPerformed(performed);
    }

    private static int auditCount(JobStorage storage, string jobId, string action)
        => AuditStore.Read(storage, limit: 100, jobId).Count(entry => entry.Action == action);

    private static string? jobState(IStorageConnection connection, string jobId)
        => connection.GetJobData(jobId)?.State;

    // The full §5 loop (acceptance tests 7 + 22): flag → gated cancel whose CancelledState data is the
    // commit record → the execution's own JobAbortedException ack, recorded under the exact
    // request/execution identity even though it fires the instant the transition lands (review B2's
    // fast-abort race, retired by prepared-request-first ordering) → ack-gated requeue with the budget
    // counted on the job.
    [Fact]
    public void RetryPolicy_FullLoop_Cancel_Ack_Requeue_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var owner = announceFreshOwner(connection);
        var jobId = createProcessingJob(connection);
        var record = seedRetryContract(connection, jobId, owner, maxRetries: 3);
        var detector = buildDetector(LeaseWindow, ackGrace: TimeSpan.FromSeconds(60));
        var serverId = uniqueServerId();

        flagThenCancel(detector, storage, serverId);

        jobState(connection, jobId).ShouldBe("Deleted");
        var stateData = connection.GetStateData(jobId);
        stateData!.Data[CancelledState.RequestIdDataKey].ShouldNotBeNullOrEmpty();
        stateData.Data[CancelledState.ExecutionIdDataKey].ShouldBe(record.ExecutionId);
        var marker = CancellationRequestStore.Read(connection, jobId);
        marker.ShouldNotBeNull();
        marker!.By.ShouldBe("system:liveness");
        marker.RequestId.ShouldBe(stateData.Data[CancelledState.RequestIdDataKey]);
        auditCount(storage, jobId, "cancel").ShouldBe(1);
        var attempt = LivenessStore.ReadStallAttempt(connection, jobId, record.ExecutionId);
        attempt.ShouldNotBeNull();
        attempt!.Phase.ShouldBe(StallAttemptRecord.PhaseCancelRequested);
        attempt.AttemptNumber.ShouldBe(1);

        // The cancelled body aborts immediately — the ack must land even before any later detector pass.
        driveOnPerformed(storage, connection, jobId, record.ExecutionId, new JobAbortedException());
        var ack = CancellationRequestStore.ReadAck(connection, jobId, record.ExecutionId, attempt.RequestId);
        ack.ShouldNotBeNull();
        ack!.Result.ShouldBe(CancelAckRecord.ResultAborted);
        auditCount(storage, jobId, "cancel-ack").ShouldBe(1);
        auditCount(storage, jobId, "abort-observed").ShouldBe(0);

        detector.Scan(storage, serverId); // the ack-gated requeue pass

        jobState(connection, jobId).ShouldBe("Enqueued"); // RetryDelaySeconds 0 ⇒ straight to the queue
        LivenessStore.ReadStallRetryCount(connection, jobId).ShouldBe(1);
        LivenessStore.ReadStallAttempt(connection, jobId, record.ExecutionId)!.Phase.ShouldBe(StallAttemptRecord.PhaseRetried);
        LivenessStore.ReadRetryPendingMembers(connection).ShouldNotContain(LivenessStore.ActiveMember(jobId, record.ExecutionId));
        LivenessStore.ReadStalledMembers(connection).ShouldNotContain(LivenessStore.ActiveMember(jobId, record.ExecutionId));
        CancellationRequestStore.Read(connection, jobId).ShouldBeNull(); // cleared by the shared requeue helper
        auditCount(storage, jobId, "stall-retry").ShouldBe(1);
    }

    // Acceptance tests 9 + 10: RetryDelaySeconds rides on Hangfire's own ScheduledState, so a second
    // detector (or the same one after a restart) observing the settled workflow neither re-times the
    // delay nor produces a second retry transition.
    [Fact]
    public void RetryDelay_ScheduledOnce_SecondDetectorAddsNothing_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var owner = announceFreshOwner(connection);
        var jobId = createProcessingJob(connection);
        var record = seedRetryContract(connection, jobId, owner, maxRetries: 3, retryDelaySeconds: 3600);
        var detector = buildDetector(LeaseWindow);
        var serverId = uniqueServerId();

        flagThenCancel(detector, storage, serverId);
        var attempt = LivenessStore.ReadStallAttempt(connection, jobId, record.ExecutionId)!;
        driveOnPerformed(storage, connection, jobId, record.ExecutionId, new JobAbortedException());
        detector.Scan(storage, serverId);

        jobState(connection, jobId).ShouldBe("Scheduled"); // Hangfire's scheduler owns the 3600s wait
        auditCount(storage, jobId, "stall-retry").ShouldBe(1);

        // Restart + a racing peer: both see a workflow already settled by identity.
        buildDetector(LeaseWindow).Scan(storage, uniqueServerId());
        detector.Scan(storage, serverId);
        jobState(connection, jobId).ShouldBe("Scheduled");
        auditCount(storage, jobId, "stall-retry").ShouldBe(1);
        LivenessStore.ReadStallRetryCount(connection, jobId).ShouldBe(1);
        LivenessStore.ReadStallAttempt(connection, jobId, record.ExecutionId)!.Phase.ShouldBe(StallAttemptRecord.PhaseRetried);
    }

    // Acceptance test 15 (§5 Rule 1): a server-list absence — or staleness — is never read as proof of
    // termination. The execution stays flagged, Processing, and untouched; storage-native recovery owns
    // the crashed-owner case.
    [Fact]
    public void OwnerAbsentFromServerList_FlagOnly_NeverCancelled_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var jobId = createProcessingJob(connection);
        var record = seedRetryContract(connection, jobId, serverId: $"dead-owner-{Guid.NewGuid():N}");
        var detector = buildDetector(LeaseWindow);
        var serverId = uniqueServerId();

        flagThenCancel(detector, storage, serverId);
        detector.Scan(storage, serverId); // extra passes change nothing
        detector.Scan(storage, serverId);

        jobState(connection, jobId).ShouldBe("Processing");
        LivenessStore.ReadStall(connection, jobId, record.ExecutionId).ShouldNotBeNull(); // still flagged
        LivenessStore.ReadStallAttempt(connection, jobId, record.ExecutionId).ShouldBeNull();
        auditCount(storage, jobId, "cancel").ShouldBe(0);

        LivenessStore.ClearStall(connection, jobId, record.ExecutionId);
        LivenessStore.EndContract(connection, jobId, record.ExecutionId);
    }

    // §5 Rule 1's second gate is host-declared: no StorageLeaseWindow ⇒ Retry downgrades to flag-only.
    [Fact]
    public void LeaseWindowUnset_RetryDowngradesToFlagOnly_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var owner = announceFreshOwner(connection);
        var jobId = createProcessingJob(connection);
        var record = seedRetryContract(connection, jobId, owner);
        var detector = buildDetector(leaseWindow: null);
        var serverId = uniqueServerId();

        flagThenCancel(detector, storage, serverId);

        jobState(connection, jobId).ShouldBe("Processing");
        LivenessStore.ReadStall(connection, jobId, record.ExecutionId).ShouldNotBeNull();
        auditCount(storage, jobId, "cancel").ShouldBe(0);

        LivenessStore.ClearStall(connection, jobId, record.ExecutionId);
        LivenessStore.EndContract(connection, jobId, record.ExecutionId);
    }

    // Resolved question 1 / §11.3: a positively detected non-sliding storage downgrades Retry to
    // flag-only — a cancel+requeue would race the storage's own re-fetch of the running job.
    [Fact]
    public void NonSlidingStorage_RetryDowngradesToFlagOnly_Test()
    {
        var storage = buildStorage(sliding: false);
        using var connection = storage.GetConnection();
        var owner = announceFreshOwner(connection);
        var jobId = createProcessingJob(connection);
        var record = seedRetryContract(connection, jobId, owner);
        var detector = buildDetector(LeaseWindow);
        var serverId = uniqueServerId();

        flagThenCancel(detector, storage, serverId);

        jobState(connection, jobId).ShouldBe("Processing");
        auditCount(storage, jobId, "cancel").ShouldBe(0);

        LivenessStore.ClearStall(connection, jobId, record.ExecutionId);
        LivenessStore.EndContract(connection, jobId, record.ExecutionId);
    }

    // Acceptance tests 16 + 21 (§5 Rule 4): no matching ack within the grace period ⇒ the job STAYS
    // Deleted — surfaced as stall-retry-blocked, no queue row ever added, human-only exit.
    [Fact]
    public void NoAckWithinGrace_Blocked_StaysDeleted_Surfaced_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var owner = announceFreshOwner(connection);
        var jobId = createProcessingJob(connection);
        var record = seedRetryContract(connection, jobId, owner);
        var detector = buildDetector(LeaseWindow, ackGrace: TimeSpan.FromMilliseconds(400));
        var serverId = uniqueServerId();

        flagThenCancel(detector, storage, serverId);
        jobState(connection, jobId).ShouldBe("Deleted");

        detector.Scan(storage, serverId); // within grace — keeps waiting
        auditCount(storage, jobId, "stall-retry-blocked").ShouldBe(0);

        Thread.Sleep(TimeSpan.FromMilliseconds(600));
        connection.Heartbeat(owner); // owner stays fresh — this block is grace-expiry, not owner loss
        detector.Scan(storage, serverId);

        jobState(connection, jobId).ShouldBe("Deleted"); // never requeued
        auditCount(storage, jobId, "stall-retry-blocked").ShouldBe(1);
        auditCount(storage, jobId, "stall-retry").ShouldBe(0);
        var attempt = LivenessStore.ReadStallAttempt(connection, jobId, record.ExecutionId);
        attempt!.Phase.ShouldBe(StallAttemptRecord.PhaseBlocked);
        LivenessStore.ReadRetryPendingMembers(connection).ShouldNotContain(LivenessStore.ActiveMember(jobId, record.ExecutionId));
        // Surfaced (§5 state machine: Blocked keeps the stalled entry) — and it survives further passes.
        LivenessStore.ReadStalledMembers(connection).ShouldContain(LivenessStore.ActiveMember(jobId, record.ExecutionId));
        detector.Scan(storage, serverId);
        LivenessStore.ReadStalledMembers(connection).ShouldContain(LivenessStore.ActiveMember(jobId, record.ExecutionId));
    }

    // Acceptance test 12: exhaustion is durable and idempotent across detector restarts. MaxRetries 0 is
    // the "kill on stall" policy — cancel the confirmed-hung body, never requeue.
    [Fact]
    public void Exhaustion_Durable_AcrossDetectorRestarts_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var owner = announceFreshOwner(connection);
        var jobId = createProcessingJob(connection);
        var record = seedRetryContract(connection, jobId, owner, maxRetries: 0);
        var detector = buildDetector(LeaseWindow);
        var serverId = uniqueServerId();

        flagThenCancel(detector, storage, serverId);
        driveOnPerformed(storage, connection, jobId, record.ExecutionId, new JobAbortedException());

        // The decisive pass runs on a REPLACEMENT instance — the workflow's inputs are all durable.
        buildDetector(LeaseWindow).Scan(storage, uniqueServerId());

        jobState(connection, jobId).ShouldBe("Deleted");
        auditCount(storage, jobId, "stall-retry-exhausted").ShouldBe(1);
        auditCount(storage, jobId, "stall-retry").ShouldBe(0);
        LivenessStore.ReadStallAttempt(connection, jobId, record.ExecutionId)!.Phase.ShouldBe(StallAttemptRecord.PhaseExhausted);
        LivenessStore.ReadStalledMembers(connection).ShouldContain(LivenessStore.ActiveMember(jobId, record.ExecutionId));

        // Idempotent under yet another restart.
        buildDetector(LeaseWindow).Scan(storage, uniqueServerId());
        auditCount(storage, jobId, "stall-retry-exhausted").ShouldBe(1);
        jobState(connection, jobId).ShouldBe("Deleted");
    }

    // Acceptance test 23 (§5 Rule 2 step 4): the body completes after the request was prepared but
    // before any transition — no ack may be recorded for a cancel that never won, and the prepared
    // request is retired so the still-standing flag can re-evaluate cleanly.
    [Fact]
    public void BodyCompletesBeforeTransition_NoAck_PreparedRequestRetired_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var owner = announceFreshOwner(connection);
        var jobId = createProcessingJob(connection);
        var record = seedRetryContract(connection, jobId, owner);

        // The crash window, seeded by hand: everything PREPARED, no state transition.
        var requestId = Guid.NewGuid().ToString("N");
        var now = DateTime.UtcNow;
        LivenessStore.WriteStallAttempt(connection, jobId, new StallAttemptRecord(
            StallAttemptRecord.CurrentVersion, record.ExecutionId, requestId, StallAttemptRecord.PhaseCancelRequested,
            AttemptNumber: 1, MaxRetries: 3, RetryDelaySeconds: 0, CancelRequestedAt: now, UpdatedAt: now, Detail: null));
        CancellationRequestStore.Write(connection, jobId, "system:liveness", now, "seeded", requestId, record.ExecutionId);
        LivenessStore.AddRetryPendingMember(connection, jobId, record.ExecutionId);

        // The body finishes normally (exception: null) while the job is still Processing — no committed
        // cancel in the state data, so nothing may be acknowledged.
        driveOnPerformed(storage, connection, jobId, record.ExecutionId, exception: null);
        auditCount(storage, jobId, "cancel-ack").ShouldBe(0);
        CancellationRequestStore.ReadAck(connection, jobId, record.ExecutionId, requestId).ShouldBeNull();

        // Re-seed Processing-under-execution (OnPerformed retired the active tuple) so the workflow pass
        // sees the retire-not-supersede branch: still Processing, cancel never committed.
        LivenessStore.StartContract(connection, jobId, record);
        buildDetector(LeaseWindow).Scan(storage, uniqueServerId());

        LivenessStore.ReadStallAttempt(connection, jobId, record.ExecutionId).ShouldBeNull();
        CancellationRequestStore.Read(connection, jobId).ShouldBeNull();
        LivenessStore.ReadRetryPendingMembers(connection).ShouldNotContain(LivenessStore.ActiveMember(jobId, record.ExecutionId));

        LivenessStore.EndContract(connection, jobId, record.ExecutionId);
    }

    // Acceptance test 11's determinism half (§5 Rule 5 / review F6): when another transition won against
    // the prepared cancel, the workflow's exit is Superseded — identity-scoped cleanup of its own data
    // only, never a stall retry stacked on the winner.
    [Fact]
    public void OtherTransitionWins_Superseded_CleanupOnly_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var owner = announceFreshOwner(connection);
        var jobId = createProcessingJob(connection);
        var record = seedRetryContract(connection, jobId, owner);

        var requestId = Guid.NewGuid().ToString("N");
        var now = DateTime.UtcNow;
        LivenessStore.WriteStallAttempt(connection, jobId, new StallAttemptRecord(
            StallAttemptRecord.CurrentVersion, record.ExecutionId, requestId, StallAttemptRecord.PhaseCancelRequested,
            AttemptNumber: 1, MaxRetries: 3, RetryDelaySeconds: 0, CancelRequestedAt: now, UpdatedAt: now, Detail: null));
        CancellationRequestStore.Write(connection, jobId, "system:liveness", now, "seeded", requestId, record.ExecutionId);
        LivenessStore.AddRetryPendingMember(connection, jobId, record.ExecutionId);
        LivenessStore.AddStalledMember(connection, jobId, record.ExecutionId);

        // The body's own failure path won (what AutomaticRetry acts on) — a plain terminal state with
        // no cancel identity in its data.
        setState(connection, jobId, new FailedLikeState());

        buildDetector(LeaseWindow).Scan(storage, uniqueServerId());

        jobState(connection, jobId).ShouldBe("Failed"); // the winner is never touched
        LivenessStore.ReadStallAttempt(connection, jobId, record.ExecutionId)!.Phase.ShouldBe(StallAttemptRecord.PhaseSuperseded);
        LivenessStore.ReadRetryPendingMembers(connection).ShouldNotContain(LivenessStore.ActiveMember(jobId, record.ExecutionId));
        LivenessStore.ReadStalledMembers(connection).ShouldNotContain(LivenessStore.ActiveMember(jobId, record.ExecutionId));
        auditCount(storage, jobId, "stall-retry").ShouldBe(0);
        auditCount(storage, jobId, "stall-retry-blocked").ShouldBe(0);
    }

    // Acceptance test 14: attempt A finishing can never acknowledge a request committed for attempt B —
    // the finishing execution's Items identity must match the committed state data exactly.
    [Fact]
    public void WrongExecutionIdentity_CannotAcknowledge_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var owner = announceFreshOwner(connection);
        var jobId = createProcessingJob(connection);
        var record = seedRetryContract(connection, jobId, owner);
        var detector = buildDetector(LeaseWindow);
        var serverId = uniqueServerId();

        flagThenCancel(detector, storage, serverId);
        jobState(connection, jobId).ShouldBe("Deleted");
        var attempt = LivenessStore.ReadStallAttempt(connection, jobId, record.ExecutionId)!;

        // A zombie from a different execution aborts — its identity doesn't match the committed cancel.
        driveOnPerformed(storage, connection, jobId, $"zombie-{Guid.NewGuid():N}", new JobAbortedException());

        CancellationRequestStore.ReadAck(connection, jobId, record.ExecutionId, attempt.RequestId).ShouldBeNull();
        auditCount(storage, jobId, "cancel-ack").ShouldBe(0);

        // The right execution still can — the fence rejects imposters, not the owner.
        driveOnPerformed(storage, connection, jobId, record.ExecutionId, new JobAbortedException());
        CancellationRequestStore.ReadAck(connection, jobId, record.ExecutionId, attempt.RequestId).ShouldNotBeNull();
        auditCount(storage, jobId, "cancel-ack").ShouldBe(1);

        buildDetector(LeaseWindow).Scan(storage, uniqueServerId()); // settle the workflow (requeue) for cleanliness
    }

    // Rolling-deploy tolerance: a cancel issued by a pre-0.5 binary (marker with no request id, plain
    // DeletedState with no commit record) is still acknowledged by the new filter's legacy path.
    [Fact]
    public void LegacyPreDeployCancel_StillAcknowledged_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var jobId = createProcessingJob(connection);

        connection.SetJobParameter(jobId, "JobControl.CancelRequested",
            """{"v":1,"by":"m.chen","at":"2026-07-19T00:00:00Z","reason":"legacy cancel"}""");
        setState(connection, jobId, new DeletedState { Reason = "Cancelled by m.chen (old binary)" });

        driveOnPerformed(storage, connection, jobId, itemsExecutionId: null, new JobAbortedException());

        var ack = AuditStore.Read(storage, limit: 50, jobId).FirstOrDefault(e => e.Action == "cancel-ack");
        ack.ShouldNotBeNull();
        ack!.Actor.ShouldBe("m.chen");
        ack.Detail!["Result"].ShouldBe("aborted");
    }

    public class RetryFixtures
    {
        public void LongRunning()
        {
        }
    }

    // Same convention as StallDetectorTests: states applied through the public transaction API whose
    // Names match the built-ins (their own constructors/serialized shapes aren't needed here).
    private sealed class ProcessingLikeState : IState
    {
        public string Name => ProcessingState.StateName;
        public string Reason => "Seeded by StallRetryTests";
        public bool IsFinal => false;
        public bool IgnoreJobLoadException => true;

        public Dictionary<string, string> SerializeData() => new()
        {
            ["StartedAt"] = JobHelper.SerializeDateTime(DateTime.UtcNow),
            ["ServerId"] = "unit-test-owner",
            ["WorkerId"] = "1",
        };
    }

    private sealed class FailedLikeState : IState
    {
        public string Name => FailedState.StateName;
        public string Reason => "Seeded body failure (StallRetryTests)";
        public bool IsFinal => false;
        public bool IgnoreJobLoadException => true;

        // The full key set FailedState itself serializes — Hangfire.PostgreSql's monitoring API indexes
        // stateData["ExceptionDetails"] unguarded, so a sparser fake would 500 every /failed read that
        // pages over this seeded row (the test database is shared with the integration host).
        public Dictionary<string, string> SerializeData() => new()
        {
            ["FailedAt"] = JobHelper.SerializeDateTime(DateTime.UtcNow),
            ["ExceptionType"] = "System.InvalidOperationException",
            ["ExceptionMessage"] = "Seeded failure",
            ["ExceptionDetails"] = "Seeded failure (StallRetryTests fixture)",
        };
    }
}
