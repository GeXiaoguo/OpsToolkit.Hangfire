using Hangfire;
using Hangfire.Common;
using Hangfire.PostgreSql;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using Shouldly;
using Xunit;

namespace OpsToolkit.Hangfire.JobControl.Tests;

// Drives StallDetector.Scan against real Postgres-backed storage through the package's public surface.
// Contracts are seeded via LivenessStore.StartContract directly — the store trusts the caller's
// snapshot (review F7: the detector evaluates the enrolled values, never its own reflection), which is
// what allows the short 2s timeouts these tests confirm stalls with in real time; the [Heartbeat]
// 60s floor is a LivenessFilter concern covered by LivenessFilterTests. Jobs are placed in Processing
// via a Processing-named IState through the public transaction API (ProcessingState's own constructor
// is internal), with no queue row — nothing here can be picked up by a real worker.
public class StallDetectorTests
{
    private const int TimeoutSeconds = 2;
    private static readonly TimeSpan PastTimeout = TimeSpan.FromSeconds(TimeoutSeconds + 0.4);
    private static readonly TimeSpan WithinTimeout = TimeSpan.FromSeconds(1.1);

    private static JobStorage buildStorage()
        => new PostgreSqlStorage(buildConnectionString(), new PostgreSqlStorageOptions { PrepareSchemaIfNecessary = true });

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

    private static StallDetector buildDetector() => new(new JobControlOptions
    {
        Liveness = new LivenessOptions { ScanInterval = TimeSpan.FromMilliseconds(200) },
    });

    private static string uniqueServerId() => $"unit-test-detector-{Guid.NewGuid():N}";

    private static string createProcessingJob(IStorageConnection connection)
    {
        var jobId = connection.CreateExpiredJob(
            Job.FromExpression<StallFixtures>(f => f.LongRunning()),
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

    private static BeatRecord seedContract(IStorageConnection connection, string jobId, DateTime? beatAt = null)
    {
        var now = DateTime.UtcNow;
        var record = new BeatRecord(
            BeatRecord.CurrentVersion, Guid.NewGuid().ToString("N"), StartedAt: now,
            TimeoutSeconds, ServerId: "unit-test-server", Seq: 1, BeatAt: beatAt ?? now, Percent: null, Message: null);
        LivenessStore.StartContract(connection, jobId, record);
        return record;
    }

    private static int auditCount(JobStorage storage, string jobId, string action)
        => AuditStore.Read(storage, limit: 100, jobId).Count(entry => entry.Action == action);

    // Acceptance tests 1 (a beating job is never flagged) — beats land inside the timeout, so the
    // observed seq never stays unchanged long enough to confirm.
    [Fact]
    public void BeatingJob_NeverFlagged_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var jobId = createProcessingJob(connection);
        var record = seedContract(connection, jobId);
        var detector = buildDetector();
        var serverId = uniqueServerId();

        detector.Scan(storage, serverId);
        for (var seq = 2; seq <= 4; seq++)
        {
            Thread.Sleep(WithinTimeout);
            record = record with { Seq = seq, BeatAt = DateTime.UtcNow };
            LivenessStore.WriteBeat(connection, jobId, record);
            detector.Scan(storage, serverId);
        }

        LivenessStore.ReadStall(connection, jobId, record.ExecutionId).ShouldBeNull();
        LivenessStore.ReadStalledMembers(connection).ShouldNotContain(LivenessStore.ActiveMember(jobId, record.ExecutionId));
        auditCount(storage, jobId, "stall-detected").ShouldBe(0);

        LivenessStore.EndContract(connection, jobId, record.ExecutionId);
    }

    // Review C3: confirmation runs on the detector's own observation clock. The seeded beat is already
    // 10 wall-clock minutes old, yet the first sight must only start the window — flagging happens one
    // timeout of *observation* later, under the per-job lock, with exactly one stall-detected entry.
    [Fact]
    public void SilentContract_FlaggedAfterObservedTimeout_NeverOnFirstSight_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var jobId = createProcessingJob(connection);
        var record = seedContract(connection, jobId, beatAt: DateTime.UtcNow.AddMinutes(-10));
        var detector = buildDetector();
        var serverId = uniqueServerId();

        detector.Scan(storage, serverId);
        LivenessStore.ReadStall(connection, jobId, record.ExecutionId).ShouldBeNull(); // first sight is a baseline, not a verdict

        Thread.Sleep(PastTimeout);
        detector.Scan(storage, serverId);

        var marker = LivenessStore.ReadStall(connection, jobId, record.ExecutionId);
        marker.ShouldNotBeNull();
        marker!.Seq.ShouldBe(1);
        marker.AcknowledgedBy.ShouldBeNull();
        LivenessStore.ReadStalledMembers(connection).ShouldContain(LivenessStore.ActiveMember(jobId, record.ExecutionId));
        auditCount(storage, jobId, "stall-detected").ShouldBe(1);
        var entry = AuditStore.Read(storage, limit: 100, jobId).First(e => e.Action == "stall-detected");
        entry.Actor.ShouldBe("system:liveness");
        entry.Detail!["ExecutionId"].ShouldBe(record.ExecutionId);

        LivenessStore.ClearStall(connection, jobId, record.ExecutionId);
        LivenessStore.EndContract(connection, jobId, record.ExecutionId);
    }

    // A flagged execution whose beats resume is un-flagged and audited stall-recovered — durable via the
    // marker's frozen Seq, so it works even for a detector that never saw the pre-flag baseline.
    [Fact]
    public void ResumedBeat_RecoversAndAudits_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var jobId = createProcessingJob(connection);
        var record = seedContract(connection, jobId);
        var detector = buildDetector();
        var serverId = uniqueServerId();

        detector.Scan(storage, serverId);
        Thread.Sleep(PastTimeout);
        detector.Scan(storage, serverId);
        LivenessStore.ReadStall(connection, jobId, record.ExecutionId).ShouldNotBeNull();

        LivenessStore.WriteBeat(connection, jobId, record with { Seq = 2, BeatAt = DateTime.UtcNow });
        detector.Scan(storage, serverId);

        LivenessStore.ReadStall(connection, jobId, record.ExecutionId).ShouldBeNull();
        LivenessStore.ReadStalledMembers(connection).ShouldNotContain(LivenessStore.ActiveMember(jobId, record.ExecutionId));
        auditCount(storage, jobId, "stall-recovered").ShouldBe(1);

        LivenessStore.EndContract(connection, jobId, record.ExecutionId);
    }

    // Acceptance test 4: two detectors racing produce exactly one committed stall transition and one
    // stall-detected entry — the loser observes the winner's marker inside the lock and stands down.
    [Fact]
    public void TwoDetectors_SingleStallTransition_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var jobId = createProcessingJob(connection);
        var record = seedContract(connection, jobId);
        var first = buildDetector();
        var second = buildDetector();
        var firstServer = uniqueServerId();
        var secondServer = uniqueServerId();

        first.Scan(storage, firstServer);
        second.Scan(storage, secondServer);
        Thread.Sleep(PastTimeout);
        first.Scan(storage, firstServer);
        second.Scan(storage, secondServer);

        LivenessStore.ReadStall(connection, jobId, record.ExecutionId).ShouldNotBeNull();
        auditCount(storage, jobId, "stall-detected").ShouldBe(1);

        LivenessStore.ClearStall(connection, jobId, record.ExecutionId);
        LivenessStore.EndContract(connection, jobId, record.ExecutionId);
    }

    // Acceptance test 3: a detector restart delays confirmation, never suppresses it — the replacement
    // instance re-baselines on first sight and flags one observed timeout later.
    [Fact]
    public void DetectorRestart_DelaysButDoesNotSuppressConfirmation_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var jobId = createProcessingJob(connection);
        var record = seedContract(connection, jobId);
        var serverId = uniqueServerId();

        buildDetector().Scan(storage, serverId); // instance lost with its in-memory baseline

        Thread.Sleep(PastTimeout);
        var replacement = buildDetector();
        replacement.Scan(storage, serverId);
        LivenessStore.ReadStall(connection, jobId, record.ExecutionId).ShouldBeNull(); // silence it never observed doesn't count

        Thread.Sleep(PastTimeout);
        replacement.Scan(storage, serverId);
        LivenessStore.ReadStall(connection, jobId, record.ExecutionId).ShouldNotBeNull();

        LivenessStore.ClearStall(connection, jobId, record.ExecutionId);
        LivenessStore.EndContract(connection, jobId, record.ExecutionId);
    }

    // Self-heal, scoped by execution identity: a tuple whose job isn't Processing (worker crash where
    // OnPerformed never ran, or a terminal transition landing while flagged) leaves both indexes.
    [Fact]
    public void NotProcessingTuple_SelfHeals_BothIndexes_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var detector = buildDetector();
        var serverId = uniqueServerId();

        // Never entered Processing at all — leaves the active index on the first pass.
        var statelessJobId = connection.CreateExpiredJob(
            Job.FromExpression<StallFixtures>(f => f.LongRunning()),
            new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromHours(1));
        var statelessRecord = seedContract(connection, statelessJobId);
        detector.Scan(storage, serverId);
        LivenessStore.ReadActiveMembers(connection).ShouldNotContain(LivenessStore.ActiveMember(statelessJobId, statelessRecord.ExecutionId));

        // Flagged, then moved to a terminal state — both indexes retire on the next pass.
        var jobId = createProcessingJob(connection);
        var record = seedContract(connection, jobId);
        detector.Scan(storage, serverId);
        Thread.Sleep(PastTimeout);
        detector.Scan(storage, serverId);
        LivenessStore.ReadStalledMembers(connection).ShouldContain(LivenessStore.ActiveMember(jobId, record.ExecutionId));

        setState(connection, jobId, new DeletedState { Reason = "test cleanup" });
        detector.Scan(storage, serverId);
        LivenessStore.ReadActiveMembers(connection).ShouldNotContain(LivenessStore.ActiveMember(jobId, record.ExecutionId));
        LivenessStore.ReadStalledMembers(connection).ShouldNotContain(LivenessStore.ActiveMember(jobId, record.ExecutionId));
    }

    // The pointer is written only at contract start, so pointer ≠ tuple identifies an execution that
    // storage-native recovery superseded — retired by identity, while the current attempt stays indexed.
    [Fact]
    public void SupersededExecution_RetiredByIdentity_CurrentSurvives_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var jobId = createProcessingJob(connection);
        var oldAttempt = seedContract(connection, jobId);
        var newAttempt = seedContract(connection, jobId); // repoints JobControl.Liveness.Current
        var detector = buildDetector();

        detector.Scan(storage, uniqueServerId());

        LivenessStore.ReadActiveMembers(connection).ShouldNotContain(LivenessStore.ActiveMember(jobId, oldAttempt.ExecutionId));
        LivenessStore.ReadActiveMembers(connection).ShouldContain(LivenessStore.ActiveMember(jobId, newAttempt.ExecutionId));

        LivenessStore.EndContract(connection, jobId, newAttempt.ExecutionId);
    }

    // Review C5, through the filter's public surface: a new execution enrolling over a still-flagged
    // older one retires the old flag by identity and audits stall-native-refetch-observed.
    [Fact]
    public void NativeRefetch_NewEnrollment_RetiresPriorStalledTuple_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var jobId = createProcessingJob(connection);
        var oldAttempt = seedContract(connection, jobId);
        var detector = buildDetector();
        var serverId = uniqueServerId();

        detector.Scan(storage, serverId);
        Thread.Sleep(PastTimeout);
        detector.Scan(storage, serverId);
        LivenessStore.ReadStall(connection, jobId, oldAttempt.ExecutionId).ShouldNotBeNull();

        var job = Job.FromExpression<StallFixtures>(f => f.Contracted());
        var performing = new PerformingContext(new PerformContext(
            storage, connection, new BackgroundJob(jobId, job, DateTime.UtcNow), new JobCancellationToken(false)));
        new LivenessFilter(auditMaxEntries: 1000).OnPerforming(performing);

        LivenessStore.ReadStall(connection, jobId, oldAttempt.ExecutionId).ShouldBeNull();
        LivenessStore.ReadStalledMembers(connection).ShouldNotContain(LivenessStore.ActiveMember(jobId, oldAttempt.ExecutionId));
        LivenessStore.ReadActiveMembers(connection).ShouldNotContain(LivenessStore.ActiveMember(jobId, oldAttempt.ExecutionId));
        auditCount(storage, jobId, "stall-native-refetch-observed").ShouldBe(1);

        var current = LivenessStore.ReadCurrentExecutionId(connection, jobId);
        current.ShouldNotBeNull();
        current.ShouldNotBe(oldAttempt.ExecutionId);
        LivenessStore.ReadActiveMembers(connection).ShouldContain(LivenessStore.ActiveMember(jobId, current!));

        LivenessStore.EndContract(connection, jobId, current!);
    }

    // Reviews F5/C2: a successful pass renews this server's lease; freshness is judged from the recorded
    // last scan, so a stale lease (a detector that stopped) reads as not-fresh without any storage expiry.
    [Fact]
    public void DetectorLease_RenewedByScan_FreshnessFromLastScan_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var serverId = uniqueServerId();

        buildDetector().Scan(storage, serverId);

        var lease = LivenessStore.ReadDetectorLeases(connection).SingleOrDefault(l => l.ServerId == serverId);
        lease.ShouldNotBeNull();
        LivenessStore.IsDetectorLeaseFresh(lease!, DateTime.UtcNow).ShouldBeTrue();
        LivenessStore.IsDetectorLeaseFresh(lease!, DateTime.UtcNow.AddMinutes(10)).ShouldBeFalse();

        var staleServer = uniqueServerId();
        LivenessStore.RenewDetectorLease(connection, new DetectorLease(
            DetectorLease.CurrentVersion, staleServer, DateTime.UtcNow.AddMinutes(-30), 30));
        var stale = LivenessStore.ReadDetectorLeases(connection).SingleOrDefault(l => l.ServerId == staleServer);
        stale.ShouldNotBeNull();
        LivenessStore.IsDetectorLeaseFresh(stale!, DateTime.UtcNow).ShouldBeFalse();
    }

    public class StallFixtures
    {
        public void LongRunning()
        {
        }

        [Heartbeat(120)]
        public void Contracted()
        {
        }
    }

    // A state whose Name is "Processing", applied through the public transaction API —
    // ProcessingState's own constructor is internal, and these seeded jobs must look Processing to the
    // detector's self-heal check without any worker involved.
    private sealed class ProcessingLikeState : IState
    {
        public string Name => ProcessingState.StateName;
        public string Reason => "Seeded by StallDetectorTests";
        public bool IsFinal => false;
        public bool IgnoreJobLoadException => true;

        public Dictionary<string, string> SerializeData() => new()
        {
            ["StartedAt"] = JobHelper.SerializeDateTime(DateTime.UtcNow),
            ["ServerId"] = "unit-test-server",
            ["WorkerId"] = "1",
        };
    }
}
