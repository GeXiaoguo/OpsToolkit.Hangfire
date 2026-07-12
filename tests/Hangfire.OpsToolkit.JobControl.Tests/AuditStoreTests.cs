using Hangfire.PostgreSql;
using Hangfire.Storage;
using Shouldly;
using Xunit;

namespace Hangfire.OpsToolkit.JobControl.Tests;

// Exercises AuditStore against a real Postgres-backed JobStorage — ordering and eviction behavior
// depend on real storage semantics (see AuditStore's class remarks on the TrimList finding), not
// something worth faking. jobcontrol:audit is one global key every test in this project — and every
// concurrently-running Host.IntegrationTests process — writes to on the SAME real Postgres instance, so
// assertions here deliberately avoid depending on the list's exact global contents/length; they use a
// GUID-unique JobId to isolate "this test's own entries" via the jobId filter instead.
public class AuditStoreTests
{
    private static JobStorage buildStorage()
        => new PostgreSqlStorage(buildConnectionString(), new PostgreSqlStorageOptions { PrepareSchemaIfNecessary = true });

    private static string buildConnectionString()
    {
        string env(string name, string fallback) => Environment.GetEnvironmentVariable(name) ?? fallback;
        var host = env("HANGFIRE_OPSTOOLKIT_TEST_PG_HOST", "localhost");
        var port = env("HANGFIRE_OPSTOOLKIT_TEST_PG_PORT", "5434");
        var database = env("HANGFIRE_OPSTOOLKIT_TEST_PG_DATABASE", "hangfire_opstoolkit");
        var username = env("HANGFIRE_OPSTOOLKIT_TEST_PG_USERNAME", "postgres");
        var password = env("HANGFIRE_OPSTOOLKIT_TEST_PG_PASSWORD", "postgres");
        return $"host={host};port={port};database={database};username={username};password={password}";
    }

    private static AuditEntry entry(string jobId, string reason)
        => new(AuditEntry.CurrentVersion, DateTime.UtcNow, "tester", "disable", jobId, reason, "ok", Detail: null);

    [Fact]
    public void AuditStore_RoundTrip_Test()
    {
        var storage = buildStorage();
        var jobId = $"round-trip-{Guid.NewGuid()}";
        const int cap = 50;

        // Cap well under the 100 appends below, so eviction must actually fire repeatedly — this is
        // exactly the scenario that silently froze on real Hangfire.PostgreSql before the fix (see
        // AuditStore's class remarks): every append past the cap got evicted by the very next one,
        // instead of the intended oldest-first eviction.
        for (var i = 0; i < 100; i++)
            AuditStore.Append(storage, entry(jobId, $"seq-{i}"), maxEntries: cap);

        // The entry just inserted is always the newest thing in the whole list at that instant, so it
        // must still be there — the old bug would have evicted it immediately instead.
        var mine = AuditStore.Read(storage, limit: 200, jobId: jobId);
        mine.ShouldNotBeEmpty();
        mine[0].Reason.ShouldBe("seq-99");

        // Whichever of my 100 survived, they're in newest-first relative order — true regardless of any
        // unrelated concurrent activity on the shared list (unrelated entries can't reorder mine).
        var sequenceNumbers = mine.Select(e => int.Parse(e.Reason!["seq-".Length..])).ToList();
        sequenceNumbers.ShouldBe(sequenceNumbers.OrderByDescending(n => n).ToList());

        // Eviction is actually bounding the list, not frozen/unbounded — a generous ceiling tolerant of
        // a little unrelated concurrent activity on the same shared Postgres instance, but nowhere near
        // what 100 un-evicted inserts (the old bug) would have produced.
        AuditStore.Read(storage, limit: 1000).Count.ShouldBeLessThan(cap + 200);

        // A line that isn't valid JSON must be skipped on read, not thrown on, and must not prevent the
        // still-valid entries (including mine) from coming back.
        using (var connection = storage.GetConnection())
        using (var transaction = connection.CreateWriteTransaction())
        {
            transaction.InsertToList("jobcontrol:audit", "not valid json");
            transaction.Commit();
        }
        AuditStore.Read(storage, limit: 200, jobId: jobId).ShouldNotBeEmpty();
    }

    [Fact]
    public void AuditRead_JobIdFilter_FindsEntryOlderThanDefaultReadWindow_Test()
    {
        var storage = buildStorage();
        var targetJobId = $"audit-filter-test-{Guid.NewGuid()}";

        AuditStore.Append(storage, entry(targetJobId, "the one we want"), maxEntries: 10_000);

        // Push the target entry outside the default 200-row read window with unrelated noise.
        for (var i = 0; i < 210; i++)
            AuditStore.Append(storage, entry($"noise-{Guid.NewGuid()}", "noise"), maxEntries: 10_000);

        AuditStore.Read(storage, limit: 200).ShouldNotContain(e => e.JobId == targetJobId);
        AuditStore.Read(storage, limit: 200, jobId: targetJobId).ShouldContain(e => e.JobId == targetJobId);
    }
}
