using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Hangfire;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Microsoft.AspNetCore.Mvc.Testing;
using OpsToolkit.Hangfire.JobControl;
using Shouldly;
using Xunit;

namespace OpsToolkit.Hangfire.Host.IntegrationTests;

// Drives the §5 retry state machine's BLOCKED arm end-to-end against the real sample host (acceptance
// tests 21 + 25): a seeded silent execution with a Retry contract is flagged by the host's detector,
// stall-cancelled once its stall outlives the host's (test-tuned) StorageLeaseWindow — the seeded job
// has no real body, so no acknowledgment can ever arrive — and blocks after the AckGracePeriod. The
// ordinary requeue endpoint must then refuse with 409, and only the reason-required force-requeue
// break-glass may move the job. The seeded contract names the host's own Hangfire server as its owner,
// so the Rule-1 owner-freshness gate reads a real, fresh heartbeat from the monitoring API.
// (The full cancel→ack→requeue loop is covered at the unit level in StallRetryTests, where the
// acknowledgment is driven through the real filters deterministically.)
// [Collection("Sample host")]: see SampleHostCollection.
[Collection("Sample host")]
public class StallRetryApiTests : IClassFixture<WebApplicationFactory<Program>>
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
    private static readonly TimeSpan PollTimeout = TimeSpan.FromSeconds(45);
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(300);

    private readonly HttpClient _client;

    public StallRetryApiTests(WebApplicationFactory<Program> factory)
    {
        _client = factory.CreateClient();
    }

    [Fact]
    public async Task BlockedStall_RequeueRefused_ForceRequeueBreaksGlass_Test()
    {
        var storage = JobStorage.Current;

        // The host's own server is the seeded execution's "owner" — present in the server list with a
        // fresh heartbeat, so the Rule-1 gate that would otherwise refuse the cancel passes.
        var ownerServerId = await waitForHostServer();

        string jobId;
        string executionId;
        using (var connection = storage.GetConnection())
        {
            jobId = connection.CreateExpiredJob(
                Job.FromExpression<CancellationTestJobs>(x => x.IgnoringLoop()),
                new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromHours(1));
            using (var transaction = connection.CreateWriteTransaction())
            {
                transaction.SetJobState(jobId, new ProcessingLikeState());
                transaction.Commit();
            }

            var now = DateTime.UtcNow;
            executionId = Guid.NewGuid().ToString("N");
            LivenessStore.StartContract(connection, jobId, new BeatRecord(
                BeatRecord.CurrentVersion, executionId, StartedAt: now, TimeoutSeconds: 2,
                ServerId: ownerServerId, Seq: 1, BeatAt: now, Percent: null, Message: null,
                OnStall: StallAction.Retry, MaxRetries: 3, RetryDelaySeconds: 0));
        }

        try
        {
            // Flag (~2s observation + 2s scan) → cancel (3s StorageLeaseWindow) → blocked (5s grace,
            // no body ⇒ no ack ever). The stalled surface reports each workflow phase as it advances.
            var blocked = await waitForStalled(jobId, item => item.RetryPhase == "blocked");
            blocked.ShouldNotBeNull();
            blocked!.ExecutionId.ShouldBe(executionId);
            blocked.RetryAttempt.ShouldBe(1);
            blocked.MaxRetries.ShouldBe(3);

            // The job itself: Deleted by the governed stall-cancel, never requeued (§5 Rule 4).
            getJobState(jobId).ShouldBe("Deleted");
            var blockedAudit = await waitForAuditAction(jobId, "stall-retry-blocked");
            blockedAudit.ShouldNotBeNull();
            var cancelAudit = await waitForAuditAction(jobId, "cancel");
            cancelAudit!.Actor.ShouldBe("system:liveness");

            // The Deleted tab marks the row so the UI can swap Requeue for the break-glass.
            var deletedRow = (await getDeletedList()).FirstOrDefault(r => r.Id == jobId);
            deletedRow.ShouldNotBeNull();
            deletedRow!.Cancelled.ShouldBeTrue();
            deletedRow.CancelledBy.ShouldBe("system:liveness");
            deletedRow.StallPhase.ShouldBe("blocked");

            // Acceptance test 25, first half: the ordinary requeue is refused outright.
            var requeue = await _client.PostAsJsonAsync(
                $"/hangfire/api/runs/{jobId}/requeue", new { reason = "trying anyway", expectedState = "Deleted" });
            requeue.StatusCode.ShouldBe(HttpStatusCode.Conflict);
            (await requeue.Content.ReadAsStringAsync()).ShouldContain("stallBlocked");
            getJobState(jobId).ShouldBe("Deleted");

            // Break-glass preconditions: a reason is mandatory, and the target state must be Deleted.
            var noReason = await _client.PostAsJsonAsync(
                $"/hangfire/api/runs/{jobId}/force-requeue", new { reason = "", expectedState = "Deleted" });
            noReason.StatusCode.ShouldBe(HttpStatusCode.BadRequest);
            var wrongState = await _client.PostAsJsonAsync(
                $"/hangfire/api/runs/{jobId}/force-requeue", new { reason = "r", expectedState = "Failed" });
            wrongState.StatusCode.ShouldBe(HttpStatusCode.BadRequest);

            // Acceptance test 25, second half: the break-glass moves the job and is audited as such.
            var force = await _client.PostAsJsonAsync(
                $"/hangfire/api/runs/{jobId}/force-requeue",
                new { reason = "integration test — worker recycled, safe to re-run", expectedState = "Deleted" });
            force.StatusCode.ShouldBe(HttpStatusCode.OK);

            (await waitForStateOtherThan(jobId, "Deleted")).ShouldNotBe("Deleted");
            var forceAudit = await waitForAuditAction(jobId, "force-requeue-unacknowledged-stall");
            forceAudit.ShouldNotBeNull();
            forceAudit!.Outcome.ShouldBe("ok");
            forceAudit.Reason.ShouldBe("integration test — worker recycled, safe to re-run");
            forceAudit.Detail!["ExecutionId"].ShouldBe(executionId);

            // The surfaced entry retires with the break-glass (identity-scoped retirement).
            var gone = await waitForStalledGone(jobId);
            gone.ShouldBeTrue();
        }
        finally
        {
            // The force-requeued job re-runs IgnoringLoop for real (~4s) and settles terminally on its
            // own; nothing further to clean up beyond letting it finish.
            await waitForStateOtherThan(jobId, "Enqueued");
        }
    }

    [Fact]
    public async Task ForceRequeue_OfNonBlockedJob_Conflicts_Test()
    {
        // A plain governed cancel (no liveness contract at all) is Deleted but not blocked — the
        // break-glass refuses it and points at the ordinary requeue action.
        var jobId = BackgroundJob.Schedule<CancellationTestJobs>(x => x.IgnoringLoop(), TimeSpan.FromHours(1));
        (await _client.PostAsJsonAsync(
            $"/hangfire/api/runs/{jobId}/cancel", new { reason = "setup", expectedState = "Scheduled" }))
            .EnsureSuccessStatusCode();

        var force = await _client.PostAsJsonAsync(
            $"/hangfire/api/runs/{jobId}/force-requeue", new { reason = "should not work", expectedState = "Deleted" });
        force.StatusCode.ShouldBe(HttpStatusCode.Conflict);
        getJobState(jobId).ShouldBe("Deleted");
    }

    // OPS-003 regression: Rule 4 says no queue row may be added before the cancelled execution
    // acknowledges — in BOTH pre-ack phases. This seeds the durable state immediately after the cancel
    // commit (the cancel-requested grace window) and exercises the real HTTP endpoint; the guard once
    // recognized only PhaseBlocked and returned 200 here.
    [Fact]
    public async Task CancelRequestedWithoutAck_OrdinaryRequeueIsRefused_Test()
    {
        var storage = JobStorage.Current;
        var ownerServerId = await waitForHostServer();
        var requestId = Guid.NewGuid().ToString("N");
        var executionId = Guid.NewGuid().ToString("N");
        var now = DateTime.UtcNow;
        string jobId;

        using (var connection = storage.GetConnection())
        {
            jobId = connection.CreateExpiredJob(
                Job.FromExpression<DemoJobs>(x => x.Heartbeat()),
                new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromHours(1));
            LivenessStore.StartContract(connection, jobId, new BeatRecord(
                BeatRecord.CurrentVersion, executionId, StartedAt: now, TimeoutSeconds: 60,
                ServerId: ownerServerId, Seq: 1, BeatAt: now, Percent: null, Message: null,
                OnStall: StallAction.Retry, MaxRetries: 3, RetryDelaySeconds: 0));
            LivenessStore.WriteStall(connection, jobId, new StallMarker(
                StallMarker.CurrentVersion, executionId, now, Seq: 1,
                AcknowledgedBy: null, AcknowledgedAt: null, AcknowledgeReason: null));
            LivenessStore.AddStalledMember(connection, jobId, executionId);
            LivenessStore.WriteStallAttempt(connection, jobId, new StallAttemptRecord(
                StallAttemptRecord.CurrentVersion, executionId, requestId,
                StallAttemptRecord.PhaseCancelRequested, AttemptNumber: 1, MaxRetries: 3,
                RetryDelaySeconds: 0, CancelRequestedAt: now, UpdatedAt: now, Detail: null));
            CancellationRequestStore.Write(
                connection, jobId, "system:liveness", now, "seeded cancel-requested phase", requestId, executionId);
            LivenessStore.AddRetryPendingMember(connection, jobId, executionId);
            using var transaction = connection.CreateWriteTransaction();
            transaction.SetJobState(jobId, new CancelledState(requestId, executionId));
            transaction.Commit();
        }

        try
        {
            var response = await _client.PostAsJsonAsync(
                $"/hangfire/api/runs/{jobId}/requeue",
                new { reason = "must wait for acknowledgment", expectedState = "Deleted" });

            response.StatusCode.ShouldBe(
                HttpStatusCode.Conflict,
                "an unacknowledged cancel-requested workflow must be guarded exactly like the blocked phase");
            getJobState(jobId).ShouldBe("Deleted");
        }
        finally
        {
            using var connection = storage.GetConnection();
            LivenessStore.RemoveRetryPendingMember(connection, jobId, executionId);
            LivenessStore.ClearStall(connection, jobId, executionId);
            LivenessStore.EndContract(connection, jobId, executionId);
        }
    }

    // OPS-003: the break-glass must be available from cancel-requested immediately — an operator who
    // has already weighed the risk is never made to wait out AckGracePeriod (the timeout records that
    // waiting ended; it does not make requeue safer).
    [Fact]
    public async Task ForceRequeue_FromCancelRequested_BreaksGlassImmediately_Test()
    {
        var storage = JobStorage.Current;
        var ownerServerId = await waitForHostServer();
        var (jobId, executionId, requestId) = seedCancelRequested(storage, ownerServerId);

        try
        {
            var force = await _client.PostAsJsonAsync(
                $"/hangfire/api/runs/{jobId}/force-requeue",
                new { reason = "worker recycled — accepting the duplicate-run risk", expectedState = "Deleted" });
            force.StatusCode.ShouldBe(HttpStatusCode.OK);

            (await waitForStateOtherThan(jobId, "Deleted")).ShouldNotBe("Deleted");
            var audit = await waitForAuditAction(jobId, "force-requeue-unacknowledged-stall");
            audit.ShouldNotBeNull();
            audit!.Outcome.ShouldBe("ok");
            audit.Detail!["SourcePhase"].ShouldBe("cancel-requested");
            audit.Detail["ExecutionId"].ShouldBe(executionId);
            audit.Detail["RequestId"].ShouldBe(requestId);

            using var connection = storage.GetConnection();
            LivenessStore.ReadStallAttempt(connection, jobId, executionId)!.Phase
                .ShouldBe(StallAttemptRecord.PhaseForceRequeued);
            (await waitForStalledGone(jobId)).ShouldBeTrue();
        }
        finally
        {
            // The force-requeued job re-runs Heartbeat for real and settles terminally on its own.
            await waitForStateOtherThan(jobId, "Enqueued");
            using var connection = storage.GetConnection();
            LivenessStore.ClearStall(connection, jobId, executionId);
            LivenessStore.EndContract(connection, jobId, executionId);
        }
    }

    // OPS-003 §"Acknowledgment arriving during force-requeue": a completed-anyway ack makes the force
    // decision stale — the body finished its work, so the override is refused, the changed outcome is
    // surfaced, and the workflow settles as completed-anyway.
    [Fact]
    public async Task ForceRequeue_AfterCompletedAnywayAck_IsRefused_Test()
    {
        var storage = JobStorage.Current;
        var ownerServerId = await waitForHostServer();
        var (jobId, executionId, requestId) = seedCancelRequested(storage, ownerServerId);

        try
        {
            using (var connection = storage.GetConnection())
            {
                CancellationRequestStore.WriteAck(connection, jobId, new CancelAckRecord(
                    CancelAckRecord.CurrentVersion, requestId, executionId,
                    CancelAckRecord.ResultCompletedAnyway, DateTime.UtcNow));
            }

            var force = await _client.PostAsJsonAsync(
                $"/hangfire/api/runs/{jobId}/force-requeue",
                new { reason = "stale override — must be refused", expectedState = "Deleted" });
            force.StatusCode.ShouldBe(HttpStatusCode.Conflict);
            (await force.Content.ReadAsStringAsync()).ShouldContain("completedAnyway");
            getJobState(jobId).ShouldBe("Deleted");

            using (var connection = storage.GetConnection())
            {
                LivenessStore.ReadStallAttempt(connection, jobId, executionId)!.Phase
                    .ShouldBe(StallAttemptRecord.PhaseCompletedAnyway);
            }

            var audit = await waitForAuditAction(jobId, "force-requeue-unacknowledged-stall");
            audit!.Outcome.ShouldBe("completed-anyway");
        }
        finally
        {
            using var connection = storage.GetConnection();
            LivenessStore.ClearStall(connection, jobId, executionId);
            LivenessStore.EndContract(connection, jobId, executionId);
        }
    }

    // OPS-003 host requirement: in governed mode the built-in Dashboard is read-only — its native
    // requeue (which knows the state name but none of OpsToolkit's cancellation/ack/audit records) is
    // unavailable, while OpsToolkit's own authorized mutation endpoints keep working.
    [Fact]
    public async Task NativeDashboard_ReadOnly_MutationUnavailable_OpsToolkitStillWorks_Test()
    {
        // A plain governed cancel of a scheduled job — Deleted, non-governed by any stall workflow, so
        // the ordinary OpsToolkit requeue is the sanctioned exit.
        var jobId = BackgroundJob.Schedule<CancellationTestJobs>(x => x.IgnoringLoop(), TimeSpan.FromHours(1));
        (await _client.PostAsJsonAsync(
            $"/hangfire/api/runs/{jobId}/cancel", new { reason = "setup", expectedState = "Scheduled" }))
            .EnsureSuccessStatusCode();

        try
        {
            // The built-in Dashboard's batch requeue for the Deleted list. Read-only mode refuses it.
            var native = await _client.PostAsync(
                "/hangfire/jobs/deleted/requeue",
                new FormUrlEncodedContent(new[] { new KeyValuePair<string, string>("jobs[]", jobId) }));
            native.StatusCode.ShouldBe(HttpStatusCode.Unauthorized);
            getJobState(jobId).ShouldBe("Deleted");

            var opsToolkit = await _client.PostAsJsonAsync(
                $"/hangfire/api/runs/{jobId}/requeue", new { reason = "sanctioned path", expectedState = "Deleted" });
            opsToolkit.StatusCode.ShouldBe(HttpStatusCode.OK);
        }
        finally
        {
            await waitForStateOtherThan(jobId, "Enqueued");
        }
    }

    // Shared seeding for the cancel-requested tests: the durable state exactly as it stands the moment
    // BeginStallCancel commits — contract, flag, workflow record, prepared marker, and the committed
    // CancelledState. The retry-pending tuple is deliberately NOT indexed: the host's detector (5s
    // test-tuned AckGracePeriod) would otherwise advance the phase mid-test, and the endpoints under
    // test read the workflow record, committed state, and ack — never that index.
    private static (string JobId, string ExecutionId, string RequestId) seedCancelRequested(
        JobStorage storage, string ownerServerId)
    {
        var requestId = Guid.NewGuid().ToString("N");
        var executionId = Guid.NewGuid().ToString("N");
        var now = DateTime.UtcNow;
        using var connection = storage.GetConnection();
        var jobId = connection.CreateExpiredJob(
            Job.FromExpression<DemoJobs>(x => x.Heartbeat()),
            new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromHours(1));
        LivenessStore.StartContract(connection, jobId, new BeatRecord(
            BeatRecord.CurrentVersion, executionId, StartedAt: now, TimeoutSeconds: 60,
            ServerId: ownerServerId, Seq: 1, BeatAt: now, Percent: null, Message: null,
            OnStall: StallAction.Retry, MaxRetries: 3, RetryDelaySeconds: 0));
        LivenessStore.WriteStall(connection, jobId, new StallMarker(
            StallMarker.CurrentVersion, executionId, now, Seq: 1,
            AcknowledgedBy: null, AcknowledgedAt: null, AcknowledgeReason: null));
        LivenessStore.AddStalledMember(connection, jobId, executionId);
        LivenessStore.WriteStallAttempt(connection, jobId, new StallAttemptRecord(
            StallAttemptRecord.CurrentVersion, executionId, requestId,
            StallAttemptRecord.PhaseCancelRequested, AttemptNumber: 1, MaxRetries: 3,
            RetryDelaySeconds: 0, CancelRequestedAt: now, UpdatedAt: now, Detail: null));
        CancellationRequestStore.Write(
            connection, jobId, "system:liveness", now, "seeded cancel-requested phase", requestId, executionId);
        using var transaction = connection.CreateWriteTransaction();
        transaction.SetJobState(jobId, new CancelledState(requestId, executionId));
        transaction.Commit();
        return (jobId, executionId, requestId);
    }

    private static async Task<string> waitForHostServer()
    {
        var deadline = DateTime.UtcNow + PollTimeout;
        do
        {
            var servers = JobStorage.Current.GetMonitoringApi().Servers();
            var fresh = servers.FirstOrDefault(s => s.Heartbeat is { } hb && DateTime.UtcNow - hb < TimeSpan.FromSeconds(50));
            if (fresh is not null) return fresh.Name;
            await Task.Delay(PollInterval);
        } while (DateTime.UtcNow < deadline);
        throw new InvalidOperationException("The sample host's Hangfire server never appeared with a fresh heartbeat.");
    }

    private static string? getJobState(string jobId)
    {
        using var connection = JobStorage.Current.GetConnection();
        return connection.GetJobData(jobId)?.State;
    }

    private static async Task<string?> waitForStateOtherThan(string jobId, string state)
    {
        var deadline = DateTime.UtcNow + PollTimeout;
        string? current;
        do
        {
            current = getJobState(jobId);
            if (!string.Equals(current, state, StringComparison.OrdinalIgnoreCase)) return current;
            await Task.Delay(PollInterval);
        } while (DateTime.UtcNow < deadline);
        return current;
    }

    private async Task<RunStalledSummaryDto?> waitForStalled(string jobId, Func<RunStalledSummaryDto, bool> predicate)
    {
        var deadline = DateTime.UtcNow + PollTimeout;
        RunStalledSummaryDto? last = null;
        do
        {
            var response = await _client.GetAsync("/hangfire/api/runs/stalled");
            response.EnsureSuccessStatusCode();
            var payload = (await response.Content.ReadFromJsonAsync<RunStalledResponseDto>(JsonOptions))!;
            last = payload.Items.FirstOrDefault(item => item.Id == jobId);
            if (last is not null && predicate(last)) return last;
            await Task.Delay(PollInterval);
        } while (DateTime.UtcNow < deadline);
        return last;
    }

    private async Task<bool> waitForStalledGone(string jobId)
    {
        var deadline = DateTime.UtcNow + PollTimeout;
        do
        {
            var response = await _client.GetAsync("/hangfire/api/runs/stalled");
            response.EnsureSuccessStatusCode();
            var payload = (await response.Content.ReadFromJsonAsync<RunStalledResponseDto>(JsonOptions))!;
            if (payload.Items.All(item => item.Id != jobId)) return true;
            await Task.Delay(PollInterval);
        } while (DateTime.UtcNow < deadline);
        return false;
    }

    private async Task<AuditEntryDto?> waitForAuditAction(string jobId, string action)
    {
        var deadline = DateTime.UtcNow + PollTimeout;
        AuditEntryDto? match;
        do
        {
            var response = await _client.GetAsync($"/hangfire/api/runs/{Uri.EscapeDataString(jobId)}/audit");
            response.EnsureSuccessStatusCode();
            var entries = (await response.Content.ReadFromJsonAsync<List<AuditEntryDto>>(JsonOptions))!;
            match = entries.FirstOrDefault(e => e.Action == action);
            if (match != null) return match;
            await Task.Delay(PollInterval);
        } while (DateTime.UtcNow < deadline);
        return match;
    }

    private async Task<List<RunDeletedSummaryDto>> getDeletedList()
    {
        var response = await _client.GetAsync("/hangfire/api/runs/deleted?count=500");
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<List<RunDeletedSummaryDto>>(JsonOptions))!;
    }

    // Local mirrors of the wire shapes — same reasoning as the other test classes' copies: these tests
    // depend on the wire contract only, the way an external consumer would.
    private sealed record RunStalledSummaryDto(
        string Id, string? JobDisplayName, string ExecutionId, string? ServerId, DateTime? StartedAt,
        DateTime StalledAt, DateTime LastBeatAt, long Seq, double? Percent, string? Message, int TimeoutSeconds,
        bool Acknowledged, string? AcknowledgedBy, DateTime? AcknowledgedAt, string? AcknowledgeReason,
        string? RetryPhase, int? RetryAttempt, int? MaxRetries);

    private sealed record RunDetectorServerStatusDto(string ServerId, DateTime LastScanAt, int ScanIntervalSeconds, bool Fresh);

    private sealed record RunDetectorStatusDto(string Status, DateTime? LastScanAt, IReadOnlyList<RunDetectorServerStatusDto> Servers);

    private sealed record RunStalledResponseDto(
        RunDetectorStatusDto Detector, long ActiveContractCount, int AcknowledgedCount, int UnacknowledgedCount,
        IReadOnlyList<RunStalledSummaryDto> Items);

    private sealed record RunDeletedSummaryDto(
        string Id, string? JobDisplayName, DateTime? DeletedAt,
        bool Cancelled, string? CancelledBy, DateTime? CancelledAt, string? CancelReason, string? StallPhase);

    private sealed record AuditEntryDto(
        int V, DateTime At, string Actor, string Action, string JobId, string? Reason, string Outcome, Dictionary<string, string>? Detail);

    // A state whose Name is "Processing", applied through the public transaction API — the seeded job
    // must look Processing to the detector without any worker involved (ProcessingState's constructor
    // is internal). No queue row exists, so no worker can ever pick the job up.
    private sealed class ProcessingLikeState : IState
    {
        public string Name => ProcessingState.StateName;
        public string Reason => "Seeded by StallRetryApiTests";
        public bool IsFinal => false;
        public bool IgnoreJobLoadException => true;

        public Dictionary<string, string> SerializeData() => new()
        {
            ["StartedAt"] = JobHelper.SerializeDateTime(DateTime.UtcNow),
            ["ServerId"] = "itest",
            ["WorkerId"] = "1",
        };
    }
}
