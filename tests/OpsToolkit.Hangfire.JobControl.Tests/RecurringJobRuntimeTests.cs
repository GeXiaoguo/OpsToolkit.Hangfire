using Hangfire;
using Hangfire.Common;
using Hangfire.PostgreSql;
using Hangfire.Storage;
using Shouldly;
using Xunit;

namespace OpsToolkit.Hangfire.JobControl.Tests;

// Exercises RecurringJobRuntimeStore and RecurringJobRegistrar.Apply against real Postgres-backed
// JobStorage — override durability across re-registration ("simulated restart" = a second Apply) is
// exactly the claim that depends on real storage behavior.
//
// Isolated in its OWN Hangfire schema, unlike the other storage test classes: Apply reconciles over
// *every* runtime row and — with removeUndeclared — deletes *every* recurring job no definition
// covers. In the shared schema that would race the parallel-running test classes (xUnit parallelizes
// classes) and wipe the demo host's seeded jobs; in a dedicated schema the sweep can only ever see
// this class's own state.
public class RecurringJobRuntimeTests
{
    private static JobStorage buildStorage()
        => new PostgreSqlStorage(buildConnectionString(), new PostgreSqlStorageOptions
        {
            PrepareSchemaIfNecessary = true,
            SchemaName = "hangfire_runtime_tests",
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

    private static string currentCronOf(JobStorage storage, string jobId)
    {
        using var connection = storage.GetConnection();
        return connection.GetAllEntriesFromHash($"recurring-job:{jobId}")!["Cron"];
    }

    // What Hangfire would actually run: the projected job payload deserialized through Hangfire's own
    // storage format — the assertion that argument overrides land in the invocation, not just in a row.
    private static Job projectedJobOf(JobStorage storage, string jobId)
    {
        using var connection = storage.GetConnection();
        var payload = connection.GetAllEntriesFromHash($"recurring-job:{jobId}")!["Job"];
        return InvocationData.DeserializePayload(payload).DeserializeJob();
    }

    [Fact]
    public void SetCronOverride_RoundTrips_AndRemoveDeletesTheRow_Test()
    {
        var storage = buildStorage();
        var jobId = $"runtime-store-test-{Guid.NewGuid()}";

        try
        {
            RecurringJobRuntimeStore.SetCronOverride(storage, jobId, "*/5 * * * *", "tester", "testing", DateTime.UtcNow);

            using (var connection = storage.GetConnection())
            {
                var row = RecurringJobRuntimeStore.Load(connection, jobId)!;
                row.Cron.ShouldBe("*/5 * * * *");
                row.UpdatedBy.ShouldBe("tester");
                row.Reason.ShouldBe("testing");
                row.IsInvalidated.ShouldBeFalse();
                RecurringJobRuntimeStore.LoadAll(connection).ShouldContain(r => r.JobId == jobId);
            }

            RecurringJobRuntimeStore.RemoveOverride(storage, jobId).ShouldBeTrue();

            using (var connection = storage.GetConnection())
            {
                RecurringJobRuntimeStore.Load(connection, jobId).ShouldBeNull();
                RecurringJobRuntimeStore.LoadAll(connection).ShouldNotContain(r => r.JobId == jobId);
            }

            RecurringJobRuntimeStore.RemoveOverride(storage, jobId).ShouldBeFalse();
        }
        finally
        {
            RecurringJobRuntimeStore.RemoveOverride(storage, jobId);
        }
    }

    [Fact]
    public void MarkInvalidated_IsSoft_AndSetCronOverrideRevalidates_Test()
    {
        var storage = buildStorage();
        var jobId = $"runtime-store-test-{Guid.NewGuid()}";

        try
        {
            RecurringJobRuntimeStore.SetCronOverride(storage, jobId, "*/5 * * * *", "tester", "testing", DateTime.UtcNow);
            RecurringJobRuntimeStore.MarkInvalidated(storage, jobId, RecurringJobRuntimeStore.InvalidatedJobRemoved, DateTime.UtcNow);

            using (var connection = storage.GetConnection())
            {
                var row = RecurringJobRuntimeStore.Load(connection, jobId)!;
                row.IsInvalidated.ShouldBeTrue();
                row.InvalidatedReason.ShouldBe(RecurringJobRuntimeStore.InvalidatedJobRemoved);
                row.Cron.ShouldBe("*/5 * * * *"); // soft: value and metadata survive the mark
            }

            // An operator writing a fresh value is the strongest re-validation.
            RecurringJobRuntimeStore.SetCronOverride(storage, jobId, "*/7 * * * *", "tester", "again", DateTime.UtcNow);

            using (var connection = storage.GetConnection())
                RecurringJobRuntimeStore.Load(connection, jobId)!.IsInvalidated.ShouldBeFalse();
        }
        finally
        {
            RecurringJobRuntimeStore.RemoveOverride(storage, jobId);
        }
    }

    [Fact]
    public void Apply_ProjectsOverride_AndItSurvivesReApply_Test()
    {
        var storage = buildStorage();
        var jobId = $"runtime-apply-test-{Guid.NewGuid()}";
        var registrar = new RecurringJobRegistrar()
            .Register<TestJob>(jobId, x => x.Run(), "0 3 * * *");

        try
        {
            registrar.Apply(storage).Errors.ShouldBeEmpty();
            currentCronOf(storage, jobId).ShouldBe("0 3 * * *");

            RecurringJobRuntimeStore.SetCronOverride(storage, jobId, "*/5 * * * *", "tester", "testing", DateTime.UtcNow);

            // Simulated restarts: registration re-runs, and the override — not the code default —
            // must be what wins, every time. This is the durability property a plain AddOrUpdate lacks.
            var summary = registrar.Apply(storage);
            summary.Errors.ShouldBeEmpty();
            summary.OverriddenJobIds.ShouldContain(jobId);
            currentCronOf(storage, jobId).ShouldBe("*/5 * * * *");

            registrar.Apply(storage);
            currentCronOf(storage, jobId).ShouldBe("*/5 * * * *");

            // Reset restores the code default at the next projection.
            RecurringJobRuntimeStore.RemoveOverride(storage, jobId).ShouldBeTrue();
            registrar.Apply(storage);
            currentCronOf(storage, jobId).ShouldBe("0 3 * * *");
        }
        finally
        {
            RecurringJobRuntimeStore.RemoveOverride(storage, jobId);
            new RecurringJobManager(storage).RemoveIfExists(jobId);
        }
    }

    [Fact]
    public void Apply_JobRemovedThenRolledBack_InvalidatesThenRevalidatesTheOverride_Test()
    {
        var storage = buildStorage();
        var jobId = $"runtime-rollback-test-{Guid.NewGuid()}";
        var withJob = new RecurringJobRegistrar().Register<TestJob>(jobId, x => x.Run(), "0 3 * * *");
        var withoutJob = new RecurringJobRegistrar();

        try
        {
            withJob.Apply(storage);
            RecurringJobRuntimeStore.SetCronOverride(storage, jobId, "*/5 * * * *", "tester", "testing", DateTime.UtcNow);
            withJob.Apply(storage);

            // Deploy that deletes the job from code: the row is soft-invalidated, never deleted.
            var summary = withoutJob.Apply(storage);
            summary.InvalidatedJobIds.ShouldContain($"{jobId}: {RecurringJobRuntimeStore.InvalidatedJobRemoved}");
            using (var connection = storage.GetConnection())
                RecurringJobRuntimeStore.Load(connection, jobId)!.IsInvalidated.ShouldBeTrue();

            // Rollback redeclares the job: the override resumes without any operator action.
            var rollback = new RecurringJobRegistrar().Register<TestJob>(jobId, x => x.Run(), "0 3 * * *");
            var rollbackSummary = rollback.Apply(storage);
            rollbackSummary.RevalidatedJobIds.ShouldContain(jobId);
            currentCronOf(storage, jobId).ShouldBe("*/5 * * * *");
            using (var connection = storage.GetConnection())
                RecurringJobRuntimeStore.Load(connection, jobId)!.IsInvalidated.ShouldBeFalse();
        }
        finally
        {
            RecurringJobRuntimeStore.RemoveOverride(storage, jobId);
            new RecurringJobManager(storage).RemoveIfExists(jobId);
        }
    }

    [Fact]
    public void Apply_BadCronOverride_GoesDormantAndCodeDefaultRuns_Test()
    {
        // The store doesn't validate cron (the endpoint validates by projecting first) — a row can
        // still hold a bad value via direct writes or a Hangfire version change tightening the parser.
        // Startup must survive it: mark bad-cron, fall back to the code default, never throw.
        var storage = buildStorage();
        var jobId = $"runtime-badcron-test-{Guid.NewGuid()}";
        var registrar = new RecurringJobRegistrar().Register<TestJob>(jobId, x => x.Run(), "0 3 * * *");

        try
        {
            RecurringJobRuntimeStore.SetCronOverride(storage, jobId, "95 99 * * *", "tester", "testing", DateTime.UtcNow);

            var summary = registrar.Apply(storage);

            summary.InvalidatedJobIds.ShouldContain($"{jobId}: {RecurringJobRuntimeStore.InvalidatedBadCron}");
            summary.Errors.ShouldContain(e => e.StartsWith(jobId));
            currentCronOf(storage, jobId).ShouldBe("0 3 * * *");
            using (var connection = storage.GetConnection())
            {
                var row = RecurringJobRuntimeStore.Load(connection, jobId)!;
                row.IsInvalidated.ShouldBeTrue();
                row.InvalidatedReason.ShouldBe(RecurringJobRuntimeStore.InvalidatedBadCron);
            }
        }
        finally
        {
            RecurringJobRuntimeStore.RemoveOverride(storage, jobId);
            new RecurringJobManager(storage).RemoveIfExists(jobId);
        }
    }

    [Fact]
    public void SetArgsOverride_SharesTheRowWithCron_AndPerFieldClearsArePartial_Test()
    {
        var storage = buildStorage();
        var jobId = $"runtime-args-store-test-{Guid.NewGuid()}";

        try
        {
            RecurringJobRuntimeStore.SetCronOverride(storage, jobId, "*/5 * * * *", "tester", "testing", DateTime.UtcNow);
            RecurringJobRuntimeStore.SetArgsOverride(storage, jobId, """{"daysToKeep":7}""", "tester", "args too", DateTime.UtcNow);

            using (var connection = storage.GetConnection())
            {
                var row = RecurringJobRuntimeStore.Load(connection, jobId)!;
                row.Cron.ShouldBe("*/5 * * * *");
                row.ArgsJson.ShouldBe("""{"daysToKeep":7}""");
                row.Reason.ShouldBe("args too"); // one metadata set — the most recent write
            }

            // Clearing one field leaves the other override (and the row) in place...
            RecurringJobRuntimeStore.ClearCronOverride(storage, jobId, "tester", "cron reset", DateTime.UtcNow).ShouldBeTrue();
            using (var connection = storage.GetConnection())
            {
                var row = RecurringJobRuntimeStore.Load(connection, jobId)!;
                row.Cron.ShouldBeNull();
                row.ArgsJson.ShouldBe("""{"daysToKeep":7}""");
            }
            // ...a second clear of the same field has nothing to reset...
            RecurringJobRuntimeStore.ClearCronOverride(storage, jobId, "tester", "again", DateTime.UtcNow).ShouldBeFalse();

            // ...and clearing the last remaining field removes the row entirely.
            RecurringJobRuntimeStore.ClearArgsOverride(storage, jobId, "tester", "args reset", DateTime.UtcNow).ShouldBeTrue();
            using (var connection = storage.GetConnection())
            {
                RecurringJobRuntimeStore.Load(connection, jobId).ShouldBeNull();
                RecurringJobRuntimeStore.LoadAll(connection).ShouldNotContain(r => r.JobId == jobId);
            }
        }
        finally
        {
            RecurringJobRuntimeStore.RemoveOverride(storage, jobId);
        }
    }

    [Fact]
    public void Apply_ProjectsArgsOverrideIntoTheInvocation_AndItSurvivesReApply_Test()
    {
        var storage = buildStorage();
        var jobId = $"runtime-args-apply-test-{Guid.NewGuid()}";
        var registrar = new RecurringJobRegistrar()
            .Register<TestJob>(jobId, x => x.Sweep(30), "0 3 * * *");

        try
        {
            registrar.Apply(storage).Errors.ShouldBeEmpty();
            projectedJobOf(storage, jobId).Args[0].ShouldBe(30); // expression-baked code default

            RecurringJobRuntimeStore.SetArgsOverride(storage, jobId, """{"daysToKeep":7}""", "tester", "testing", DateTime.UtcNow);

            var summary = registrar.Apply(storage);
            summary.Errors.ShouldBeEmpty();
            summary.OverriddenJobIds.ShouldContain(jobId);
            projectedJobOf(storage, jobId).Args[0].ShouldBe(7);

            registrar.Apply(storage);
            projectedJobOf(storage, jobId).Args[0].ShouldBe(7); // simulated restart — override wins again

            RecurringJobRuntimeStore.ClearArgsOverride(storage, jobId, "tester", "reset", DateTime.UtcNow).ShouldBeTrue();
            registrar.Apply(storage);
            projectedJobOf(storage, jobId).Args[0].ShouldBe(30);
        }
        finally
        {
            RecurringJobRuntimeStore.RemoveOverride(storage, jobId);
            new RecurringJobManager(storage).RemoveIfExists(jobId);
        }
    }

    [Fact]
    public void Apply_BadArgs_SendTheWholeRowDormant_FullCodeDefaultsRun_Test()
    {
        // The unit rule: the row's valid cron override must NOT half-apply while its args are broken —
        // "override dormant" always means the full code defaults are what runs.
        var storage = buildStorage();
        var jobId = $"runtime-badargs-test-{Guid.NewGuid()}";
        var registrar = new RecurringJobRegistrar().Register<TestJob>(jobId, x => x.Sweep(30), "0 3 * * *");

        try
        {
            RecurringJobRuntimeStore.SetCronOverride(storage, jobId, "*/5 * * * *", "tester", "testing", DateTime.UtcNow);
            RecurringJobRuntimeStore.SetArgsOverride(storage, jobId, """{"retentionDays":7}""", "tester", "renamed param", DateTime.UtcNow);

            var summary = registrar.Apply(storage);

            summary.InvalidatedJobIds.ShouldContain($"{jobId}: {RecurringJobRuntimeStore.InvalidatedBadArgs}");
            summary.Errors.ShouldContain(e => e.Contains("unknown parameter 'retentionDays'"));
            currentCronOf(storage, jobId).ShouldBe("0 3 * * *");
            projectedJobOf(storage, jobId).Args[0].ShouldBe(30);

            // An operator writing bindable values re-validates the row and the cron override resumes too.
            RecurringJobRuntimeStore.SetArgsOverride(storage, jobId, """{"daysToKeep":7}""", "tester", "fixed", DateTime.UtcNow);
            var fixedSummary = registrar.Apply(storage);
            fixedSummary.Errors.ShouldBeEmpty();
            currentCronOf(storage, jobId).ShouldBe("*/5 * * * *");
            projectedJobOf(storage, jobId).Args[0].ShouldBe(7);
        }
        finally
        {
            RecurringJobRuntimeStore.RemoveOverride(storage, jobId);
            new RecurringJobManager(storage).RemoveIfExists(jobId);
        }
    }

    [Fact]
    public void Apply_RemoveUndeclared_IsOptIn_Test()
    {
        var storage = buildStorage();
        var manager = new RecurringJobManager(storage);
        var undeclaredId = $"runtime-undeclared-test-{Guid.NewGuid()}";
        var declaredId = $"runtime-declared-test-{Guid.NewGuid()}";
        var registrar = new RecurringJobRegistrar().Register<TestJob>(declaredId, x => x.Run(), "0 3 * * *");

        try
        {
            manager.AddOrUpdate<TestJob>(undeclaredId, x => x.Run(), Cron.Never());

            // Default: reported, not removed.
            var summary = registrar.Apply(storage);
            summary.UndeclaredJobIds.ShouldContain(undeclaredId);
            summary.RemovedUndeclaredJobIds.ShouldBeEmpty();
            using (var connection = storage.GetConnection())
                connection.GetAllEntriesFromHash($"recurring-job:{undeclaredId}").ShouldNotBeNull();

            // Opt-in: code owns the recurring-job set.
            var removing = registrar.Apply(storage, removeUndeclared: true);
            removing.RemovedUndeclaredJobIds.ShouldContain(undeclaredId);
            using (var connection = storage.GetConnection())
                connection.GetAllEntriesFromHash($"recurring-job:{undeclaredId}").ShouldBeNull();
        }
        finally
        {
            manager.RemoveIfExists(undeclaredId);
            manager.RemoveIfExists(declaredId);
        }
    }

    public class TestJob
    {
        public void Run() { }

        public void Sweep(int daysToKeep) { }
    }
}
