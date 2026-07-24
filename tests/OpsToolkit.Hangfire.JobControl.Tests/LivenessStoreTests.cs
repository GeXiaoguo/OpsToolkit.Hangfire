using Hangfire;
using Hangfire.Common;
using Hangfire.PostgreSql;
using Hangfire.States;
using Hangfire.Storage;
using Shouldly;
using Xunit;

namespace OpsToolkit.Hangfire.JobControl.Tests;

// Exercises LivenessStore's execution-scoped records against a real Postgres-backed JobStorage — same
// rationale as CancellationRequestStoreTests: job parameters need a real created job (numeric ids on
// Hangfire.PostgreSql), and the execution-tuple isolation contract (review B3: a zombie attempt can
// neither overwrite the current attempt's record nor un-index it) is exactly what these tests pin.
public class LivenessStoreTests
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

    private static string createJob(JobStorage storage)
        => new BackgroundJobClient(storage).Create(Job.FromExpression(() => TestJob.Run()), new EnqueuedState());

    private static BeatRecord record(string executionId, long seq = 1, double? percent = null, string? message = null)
    {
        var at = new DateTime(2026, 7, 19, 1, 2, 3, DateTimeKind.Utc);
        return new BeatRecord(BeatRecord.CurrentVersion, executionId, at, TimeoutSeconds: 120, ServerId: "test-server", seq, at, percent, message);
    }

    [Fact]
    public void StartContract_RoundTrip_Test()
    {
        var storage = buildStorage();
        var jobId = createJob(storage);
        using var connection = storage.GetConnection();

        LivenessStore.ReadCurrentBeat(connection, jobId).ShouldBeNull();

        var start = record("aaaa1111");
        LivenessStore.StartContract(connection, jobId, start);

        LivenessStore.ReadCurrentBeat(connection, jobId).ShouldBe(start);
        LivenessStore.ReadBeat(connection, jobId, "aaaa1111").ShouldBe(start);
        LivenessStore.ReadActiveMembers(connection).ShouldContain(LivenessStore.ActiveMember(jobId, "aaaa1111"));
    }

    // Acceptance test 5 (liveness plan review): a stale beat from execution A cannot overwrite or
    // recover execution B — A's write lands on A's own record; the current pointer stays on B.
    [Fact]
    public void StaleExecutionBeat_CannotOverwriteCurrent_Test()
    {
        var storage = buildStorage();
        var jobId = createJob(storage);
        using var connection = storage.GetConnection();

        LivenessStore.StartContract(connection, jobId, record("execa"));
        var current = record("execb");
        LivenessStore.StartContract(connection, jobId, current);

        LivenessStore.WriteBeat(connection, jobId, record("execa", seq: 99, percent: 50, message: "zombie"));

        LivenessStore.ReadCurrentBeat(connection, jobId).ShouldBe(current);
        LivenessStore.ReadBeat(connection, jobId, "execa")!.Seq.ShouldBe(99);
    }

    // Acceptance test 24: old attempt A running OnPerformed must leave B's active tuple intact —
    // removal is by exact tuple, never by job id.
    [Fact]
    public void EndContract_RemovesExactTupleOnly_Test()
    {
        var storage = buildStorage();
        var jobId = createJob(storage);
        using var connection = storage.GetConnection();

        LivenessStore.StartContract(connection, jobId, record("execa"));
        LivenessStore.StartContract(connection, jobId, record("execb"));

        LivenessStore.EndContract(connection, jobId, "execa");

        var members = LivenessStore.ReadActiveMembers(connection);
        members.ShouldNotContain(LivenessStore.ActiveMember(jobId, "execa"));
        members.ShouldContain(LivenessStore.ActiveMember(jobId, "execb"));

        LivenessStore.EndContract(connection, jobId, "execb");
        LivenessStore.ReadActiveMembers(connection).ShouldNotContain(LivenessStore.ActiveMember(jobId, "execb"));
    }

    [Theory]
    [InlineData("42|abc", "42", "abc")]
    [InlineData("has|pipe|abc", "has|pipe", "abc")] // split on the LAST separator — job-id shape isn't ours to assume
    public void TryParseActiveMember_Valid_Test(string member, string jobId, string executionId)
        => LivenessStore.TryParseActiveMember(member).ShouldBe((jobId, executionId));

    [Theory]
    [InlineData("no-separator")]
    [InlineData("|leading")]
    [InlineData("trailing|")]
    public void TryParseActiveMember_Invalid_Test(string member)
        => LivenessStore.TryParseActiveMember(member).ShouldBeNull();

    public static class TestJob
    {
        public static void Run()
        {
        }
    }
}
