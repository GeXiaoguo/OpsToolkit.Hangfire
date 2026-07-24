using Hangfire;
using Hangfire.Common;
using Hangfire.PostgreSql;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using Shouldly;
using Xunit;

namespace OpsToolkit.Hangfire.JobControl.Tests;

// Drives LivenessFilter and context.Beat() through real PerformingContext/PerformedContext instances
// (their constructors are public precisely so filters can be unit-tested) against real Postgres-backed
// storage. Everything here uses only the package's public surface — including the throttle test, which
// waits out the real 5s MinBeatInterval once rather than poking internal state.
public class LivenessFilterTests
{
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

    private static (PerformingContext Performing, string JobId, IStorageConnection Connection) buildPerforming(
        JobStorage storage, IStorageConnection connection, Job job)
    {
        var jobId = new BackgroundJobClient(storage).Create(job, new EnqueuedState());
        var backgroundJob = new BackgroundJob(jobId, job, DateTime.UtcNow);
        var performContext = new PerformContext(storage, connection, backgroundJob, new JobCancellationToken(false));
        return (new PerformingContext(performContext), jobId, connection);
    }

    private static PerformedContext performed(PerformingContext performing)
        => new(performing, result: null, canceled: false, exception: null);

    [Fact]
    public void ContractedJob_StartsAndRetiresContract_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var filter = new LivenessFilter(auditMaxEntries: 1000);
        var (performing, jobId, _) = buildPerforming(storage, connection, Job.FromExpression<LivenessFixtures>(f => f.Contracted()));

        filter.OnPerforming(performing);

        var beat = LivenessStore.ReadCurrentBeat(connection, jobId);
        beat.ShouldNotBeNull();
        beat!.TimeoutSeconds.ShouldBe(120);          // the executing version's contract snapshot (F7)
        beat.Seq.ShouldBe(1);                        // contract start doubles as beat #1
        LivenessStore.ReadActiveMembers(connection).ShouldContain(LivenessStore.ActiveMember(jobId, beat.ExecutionId));

        filter.OnPerformed(performed(performing));

        LivenessStore.ReadActiveMembers(connection).ShouldNotContain(LivenessStore.ActiveMember(jobId, beat.ExecutionId));
        // The record survives contract end so terminal views can show final progress.
        LivenessStore.ReadCurrentBeat(connection, jobId).ShouldNotBeNull();
    }

    // Acceptance test 2: a job with no [Heartbeat] attribute produces no liveness storage at all, and
    // Beat() on it is a no-op — bit-for-bit today's behavior (R3).
    [Fact]
    public void PlainJob_NoLivenessStorage_BeatIsNoOp_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var filter = new LivenessFilter(auditMaxEntries: 1000);
        var (performing, jobId, _) = buildPerforming(storage, connection, Job.FromExpression<LivenessFixtures>(f => f.Plain()));

        filter.OnPerforming(performing);
        performing.Beat(percent: 50, message: "should not persist");
        filter.OnPerformed(performed(performing));

        LivenessStore.ReadCurrentBeat(connection, jobId).ShouldBeNull();
        LivenessStore.ReadActiveMembers(connection).ShouldNotContain(m => m.StartsWith(jobId + "|"));
    }

    // Acceptance test 20 (eager half): a statically invalid timeout fails at attribute construction.
    [Fact]
    public void HeartbeatAttribute_EagerValidation_Test()
    {
        Should.Throw<ArgumentOutOfRangeException>(() => new HeartbeatAttribute(0));
        Should.Throw<ArgumentOutOfRangeException>(() => new HeartbeatAttribute(-5));
    }

    // Acceptance test 20 (contract-start half) + review C4: an invalid contract never throws into the
    // job — the execution runs unmonitored and the loud signal is a contract-invalid audit entry.
    [Fact]
    public void BelowFloorTimeout_RunsUnmonitored_AuditsContractInvalid_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var filter = new LivenessFilter(auditMaxEntries: 1000);
        var (performing, jobId, _) = buildPerforming(storage, connection, Job.FromExpression<LivenessFixtures>(f => f.BelowFloor()));

        Should.NotThrow(() => filter.OnPerforming(performing));
        performing.Beat(); // enrollment was refused — must be a no-op, not a crash

        LivenessStore.ReadCurrentBeat(connection, jobId).ShouldBeNull();
        var entry = AuditStore.Read(storage, limit: 50, jobId).FirstOrDefault(e => e.Action == "contract-invalid");
        entry.ShouldNotBeNull();
        entry!.Actor.ShouldBe("system:liveness");
        entry.Reason.ShouldNotBeNullOrEmpty();
    }

    // Acceptance test 28 shape: enrollment storage failure ⇒ fail-open with a loud signal — the body
    // proceeds (OnPerforming must not throw), Beat() stays a no-op, and contract-init-failed is audited.
    [Fact]
    public void EnrollmentStorageFailure_FailsOpenWithAudit_Test()
    {
        var storage = buildStorage();
        using var realConnection = storage.GetConnection();
        var job = Job.FromExpression<LivenessFixtures>(f => f.Contracted());
        var jobId = new BackgroundJobClient(storage).Create(job, new EnqueuedState());
        var backgroundJob = new BackgroundJob(jobId, job, DateTime.UtcNow);
        var throwingConnection = new ParameterWriteThrowingConnection(realConnection);
        var performing = new PerformingContext(
            new PerformContext(storage, throwingConnection, backgroundJob, new JobCancellationToken(false)));
        var filter = new LivenessFilter(auditMaxEntries: 1000);

        Should.NotThrow(() => filter.OnPerforming(performing));
        Should.NotThrow(() => performing.Beat(percent: 10));
        Should.NotThrow(() => filter.OnPerformed(performed(performing)));

        LivenessStore.ReadCurrentBeat(realConnection, jobId).ShouldBeNull();
        LivenessStore.ReadActiveMembers(realConnection).ShouldNotContain(m => m.StartsWith(jobId + "|"));
        var entry = AuditStore.Read(storage, limit: 50, jobId).FirstOrDefault(e => e.Action == "contract-init-failed");
        entry.ShouldNotBeNull();
        entry!.Actor.ShouldBe("system:liveness");
    }

    // Throttle + payload normalization through the public surface: beats inside MinBeatInterval are
    // coalesced (seq unchanged); after the interval one write lands with clamped percent and truncated
    // message; null percent/message keep the previously reported values.
    [Fact]
    public void Beat_ThrottlesCoalescesAndNormalizes_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var filter = new LivenessFilter(auditMaxEntries: 1000);
        var (performing, jobId, _) = buildPerforming(storage, connection, Job.FromExpression<LivenessFixtures>(f => f.Contracted()));
        filter.OnPerforming(performing);

        performing.Beat(percent: 10, message: "too soon");
        LivenessStore.ReadCurrentBeat(connection, jobId)!.Seq.ShouldBe(1); // coalesced — within MinBeatInterval of start

        Thread.Sleep(TimeSpan.FromSeconds(5.5)); // wait out the real MinBeatInterval once

        performing.Beat(percent: 250, message: new string('x', 600));
        var beat = LivenessStore.ReadCurrentBeat(connection, jobId)!;
        beat.Seq.ShouldBe(2);
        beat.Percent.ShouldBe(100);               // clamped to 0..100
        beat.Message!.Length.ShouldBe(512);       // truncated to MaxMessageLength

        performing.Beat(percent: double.NaN);     // NaN ignored; within interval again — coalesced
        LivenessStore.ReadCurrentBeat(connection, jobId)!.Seq.ShouldBe(2);

        filter.OnPerformed(performed(performing));
    }

    // The stall policy is part of the contract snapshot (review F7): the detector must apply the values
    // the executing version enrolled with, so they have to land in the beat record at contract start.
    [Fact]
    public void RetryPolicy_SnapshottedIntoContract_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var filter = new LivenessFilter(auditMaxEntries: 1000);
        var (performing, jobId, _) = buildPerforming(storage, connection, Job.FromExpression<LivenessFixtures>(f => f.RetryContracted()));

        filter.OnPerforming(performing);

        var beat = LivenessStore.ReadCurrentBeat(connection, jobId);
        beat.ShouldNotBeNull();
        beat!.OnStall.ShouldBe(StallAction.Retry);
        beat.MaxRetries.ShouldBe(2);
        beat.RetryDelaySeconds.ShouldBe(30);

        filter.OnPerformed(performed(performing));
    }

    // MaxRetries/RetryDelaySeconds are settable attribute properties, so their gate is contract start —
    // the same C4 rule as the timeout floor: unmonitored + contract-invalid audit, never a thrown
    // exception into the job.
    [Fact]
    public void NegativeRetryValues_RunUnmonitored_AuditContractInvalid_Test()
    {
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var filter = new LivenessFilter(auditMaxEntries: 1000);
        var (performing, jobId, _) = buildPerforming(storage, connection, Job.FromExpression<LivenessFixtures>(f => f.NegativeRetries()));

        Should.NotThrow(() => filter.OnPerforming(performing));

        LivenessStore.ReadCurrentBeat(connection, jobId).ShouldBeNull();
        var entry = AuditStore.Read(storage, limit: 50, jobId).FirstOrDefault(e => e.Action == "contract-invalid");
        entry.ShouldNotBeNull();
        entry!.Reason!.ShouldContain("MaxRetries");
    }

    public class LivenessFixtures
    {
        [Heartbeat(120)]
        public void Contracted()
        {
        }

        public void Plain()
        {
        }

        [Heartbeat(30)] // positive (passes eager validation) but below the 60s contract-start floor
        public void BelowFloor()
        {
        }

        [Heartbeat(120, OnStall = StallAction.Retry, MaxRetries = 2, RetryDelaySeconds = 30)]
        public void RetryContracted()
        {
        }

        [Heartbeat(120, OnStall = StallAction.Retry, MaxRetries = -1)]
        public void NegativeRetries()
        {
        }
    }

    // Minimal delegating connection whose job-parameter writes fail — the enrollment-failure fixture.
    private sealed class ParameterWriteThrowingConnection : JobStorageConnection
    {
        private readonly IStorageConnection _inner;

        public ParameterWriteThrowingConnection(IStorageConnection inner) => _inner = inner;

        public override void SetJobParameter(string id, string name, string value)
            => throw new InvalidOperationException("Simulated storage failure (liveness test fixture).");

        public override IWriteOnlyTransaction CreateWriteTransaction() => _inner.CreateWriteTransaction();
        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout) => _inner.AcquireDistributedLock(resource, timeout);
        public override string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
            => _inner.CreateExpiredJob(job, parameters, createdAt, expireIn);
        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken) => _inner.FetchNextJob(queues, cancellationToken);
        public override string GetJobParameter(string id, string name) => _inner.GetJobParameter(id, name);
        public override JobData GetJobData(string jobId) => _inner.GetJobData(jobId);
        public override StateData GetStateData(string jobId) => _inner.GetStateData(jobId);
        public override void AnnounceServer(string serverId, ServerContext context) => _inner.AnnounceServer(serverId, context);
        public override void RemoveServer(string serverId) => _inner.RemoveServer(serverId);
        public override void Heartbeat(string serverId) => _inner.Heartbeat(serverId);
        public override int RemoveTimedOutServers(TimeSpan timeOut) => _inner.RemoveTimedOutServers(timeOut);
        public override HashSet<string> GetAllItemsFromSet(string key) => _inner.GetAllItemsFromSet(key);
        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
            => _inner.GetFirstByLowestScoreFromSet(key, fromScore, toScore);
        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
            => _inner.SetRangeInHash(key, keyValuePairs);
        public override Dictionary<string, string> GetAllEntriesFromHash(string key) => _inner.GetAllEntriesFromHash(key);
    }
}
