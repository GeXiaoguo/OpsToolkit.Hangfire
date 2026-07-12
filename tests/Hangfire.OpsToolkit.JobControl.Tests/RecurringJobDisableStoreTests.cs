using Hangfire;
using Hangfire.Common;
using Hangfire.PostgreSql;
using Hangfire.Server;
using Hangfire.Storage;
using Shouldly;
using Xunit;

namespace Hangfire.OpsToolkit.JobControl.Tests;

// Exercises RecurringJobDisableStore and DisabledRecurringJobFilter against a real Postgres-backed
// JobStorage — the durability claim (the flag survives AddOrUpdate re-registration) and the filter's
// cancel decision both depend on real Hangfire hash-merge behaviour, not something worth faking.
public class RecurringJobDisableStoreTests
{
    // This project owns its own Postgres database (docker-compose.postgres.yaml), so the schema must
    // be installed here rather than assumed from a shared host.
    private static JobStorage buildStorage()
        => new PostgreSqlStorage(buildConnectionString(), new PostgreSqlStorageOptions { PrepareSchemaIfNecessary = true });

    // Same local Postgres tests/Hangfire.OpsToolkit.Host/docker-compose.postgres.yaml starts.
    // Override via env vars for CI.
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

    [Fact]
    public void SetDisabled_SurvivesReRegistration_Test()
    {
        var storage = buildStorage();
        var manager = new RecurringJobManager(storage);
        var jobId = $"disable-store-test-{Guid.NewGuid()}";
        manager.AddOrUpdate<TestJob>(jobId, x => x.Run(), Cron.Never());

        try
        {
            using (var connection = storage.GetConnection())
                RecurringJobDisableStore.IsDisabled(connection, jobId).ShouldBeFalse();

            RecurringJobDisableStore.SetDisabled(storage, jobId, disabled: true, "tester", "testing", DateTime.UtcNow).ShouldBeTrue();

            using (var connection = storage.GetConnection())
                RecurringJobDisableStore.IsDisabled(connection, jobId).ShouldBeTrue();

            // The whole point of storing the flag as an unknown hash field: re-registration must not clear it.
            manager.AddOrUpdate<TestJob>(jobId, x => x.Run(), Cron.Never());

            using (var connection = storage.GetConnection())
            {
                RecurringJobDisableStore.IsDisabled(connection, jobId).ShouldBeTrue();
                var status = RecurringJobDisableStore.GetStatus(connection, jobId)!;
                status.Disabled.ShouldBeTrue();
                status.By.ShouldBe("tester");
                status.Reason.ShouldBe("testing");
            }

            RecurringJobDisableStore.SetDisabled(storage, jobId, disabled: false, "tester2", null, DateTime.UtcNow).ShouldBeTrue();

            using (var connection = storage.GetConnection())
                RecurringJobDisableStore.IsDisabled(connection, jobId).ShouldBeFalse();
        }
        finally
        {
            manager.RemoveIfExists(jobId);
        }
    }

    [Fact]
    public void SetDisabled_UnknownJob_ReturnsFalseWithoutWriting_Test()
    {
        var storage = buildStorage();
        var jobId = $"disable-store-test-missing-{Guid.NewGuid()}";

        RecurringJobDisableStore.SetDisabled(storage, jobId, disabled: true, "tester", "testing", DateTime.UtcNow).ShouldBeFalse();

        using var connection = storage.GetConnection();
        connection.GetAllEntriesFromHash($"recurring-job:{jobId}").ShouldBeNull();
    }

    [Fact]
    public void OnPerforming_CancelsWhenRecurringJobIsDisabled_Test()
    {
        var storage = buildStorage();
        var manager = new RecurringJobManager(storage);
        var jobId = $"disable-filter-test-{Guid.NewGuid()}";
        manager.AddOrUpdate<TestJob>(jobId, x => x.Run(), Cron.Never());

        try
        {
            RecurringJobDisableStore.SetDisabled(storage, jobId, disabled: true, "tester", "testing", DateTime.UtcNow).ShouldBeTrue();

            performFilterFor(storage, jobId).Canceled.ShouldBeTrue();
        }
        finally
        {
            manager.RemoveIfExists(jobId);
        }
    }

    [Fact]
    public void OnPerforming_RunsWhenRecurringJobIsEnabled_Test()
    {
        var storage = buildStorage();
        var manager = new RecurringJobManager(storage);
        var jobId = $"disable-filter-test-{Guid.NewGuid()}";
        manager.AddOrUpdate<TestJob>(jobId, x => x.Run(), Cron.Never());

        try
        {
            performFilterFor(storage, jobId).Canceled.ShouldBeFalse();
        }
        finally
        {
            manager.RemoveIfExists(jobId);
        }
    }

    [Fact]
    public void OnPerforming_RunsAdHocJobWithNoRecurringJobId_Test()
    {
        // A manual/one-off enqueue (e.g. a future manual-invoke force-run) carries no RecurringJobId,
        // so the filter must not touch it regardless of any recurring job's disabled state.
        var storage = buildStorage();
        using var connection = storage.GetConnection();
        var backgroundJob = new BackgroundJob(
            Guid.NewGuid().ToString(),
            Job.FromExpression<TestJob>(x => x.Run()),
            DateTime.UtcNow,
            new Dictionary<string, string>());
        var performingContext = new PerformingContext(new PerformContext(storage, connection, backgroundJob, new JobCancellationToken(false)));

        new DisabledRecurringJobFilter().OnPerforming(performingContext);

        performingContext.Canceled.ShouldBeFalse();
    }

    // Builds a PerformingContext stamped with RecurringJobId (as Hangfire itself does on every
    // scheduler- and dashboard-triggered fire) and runs the filter against it directly — exercises the
    // real cancel decision without needing a live BackgroundJobServer to process the job.
    private static PerformingContext performFilterFor(JobStorage storage, string recurringJobId)
    {
        using var connection = storage.GetConnection();
        var backgroundJob = new BackgroundJob(
            Guid.NewGuid().ToString(),
            Job.FromExpression<TestJob>(x => x.Run()),
            DateTime.UtcNow,
            new Dictionary<string, string> { ["RecurringJobId"] = SerializationHelper.Serialize(recurringJobId) });
        var performingContext = new PerformingContext(new PerformContext(storage, connection, backgroundJob, new JobCancellationToken(false)));

        new DisabledRecurringJobFilter().OnPerforming(performingContext);
        return performingContext;
    }

    public class TestJob
    {
        public void Run() { }
    }
}
