using Hangfire.Common;
using Shouldly;
using Xunit;

namespace OpsToolkit.Hangfire.JobControl.Tests;

// Pure — no storage, no clock. The reconciler plans; RecurringJobRegistrar.Apply executes (its
// storage-dependent behavior is covered in RecurringJobRuntimeTests).
public class RecurringJobRuntimeReconcilerTests
{
    private static RecurringJobTypeDefinition definition(string id, string cron = "0 3 * * *")
        => new(id, Job.FromExpression<TestJob>(x => x.Run()), cron, TimeZoneInfo.Utc);

    private static RecurringJobRuntimeRow row(
        string jobId, string? cron, string? invalidatedAt = null, string? invalidatedReason = null, string? argsJson = null)
        => new(jobId, cron, argsJson, "tester", "2026-07-01T00:00:00Z", "why", invalidatedAt, invalidatedReason);

    [Fact]
    public void EffectiveCron_IsOverrideWhenRowPresent_DefaultOtherwise_Test()
    {
        var defs = new[] { definition("with-override"), definition("without-override") };
        var rows = new[] { row("with-override", "*/5 * * * *") };

        var plan = RecurringJobRuntimeReconciler.Reconcile(defs, rows, Array.Empty<string>());

        plan.Projections.Count.ShouldBe(2);
        plan.Projections.Single(p => p.Definition.Id == "with-override").EffectiveCron.ShouldBe("*/5 * * * *");
        plan.Projections.Single(p => p.Definition.Id == "without-override").EffectiveCron.ShouldBe("0 3 * * *");
        plan.MarkJobRemoved.ShouldBeEmpty();
    }

    [Fact]
    public void EmptyCronInRow_IsNotAnOverride_Test()
    {
        // A row storing only args (or a cleared field written as "") must not project an empty cron.
        var plan = RecurringJobRuntimeReconciler.Reconcile(
            new[] { definition("job") }, new[] { row("job", "") }, Array.Empty<string>());

        var projection = plan.Projections.Single();
        projection.OverrideCron.ShouldBeNull();
        projection.HasOverride.ShouldBeFalse();
        projection.EffectiveCron.ShouldBe("0 3 * * *");
    }

    [Fact]
    public void ArgsOnlyRow_IsAnOverride_WithTheCodeDefaultCron_Test()
    {
        var plan = RecurringJobRuntimeReconciler.Reconcile(
            new[] { definition("job") }, new[] { row("job", cron: null, argsJson: """{"daysToKeep":7}""") }, Array.Empty<string>());

        var projection = plan.Projections.Single();
        projection.OverrideCron.ShouldBeNull();
        projection.OverrideArgsJson.ShouldBe("""{"daysToKeep":7}""");
        projection.HasOverride.ShouldBeTrue();
        projection.EffectiveCron.ShouldBe("0 3 * * *");
    }

    [Fact]
    public void RowWithoutDefinition_IsMarkedJobRemoved_Test()
    {
        var plan = RecurringJobRuntimeReconciler.Reconcile(
            new[] { definition("kept") },
            new[] { row("kept", "*/5 * * * *"), row("deleted-from-code", "*/9 * * * *") },
            Array.Empty<string>());

        plan.MarkJobRemoved.ShouldBe(new[] { "deleted-from-code" });
    }

    [Fact]
    public void RowAlreadyMarkedJobRemoved_IsNotReMarked_Test()
    {
        // Re-marking every startup would churn the invalidation timestamp without saying anything new.
        var plan = RecurringJobRuntimeReconciler.Reconcile(
            Array.Empty<RecurringJobTypeDefinition>(),
            new[] { row("gone", "*/5 * * * *", "2026-07-02T00:00:00Z", RecurringJobRuntimeStore.InvalidatedJobRemoved) },
            Array.Empty<string>());

        plan.MarkJobRemoved.ShouldBeEmpty();
    }

    [Fact]
    public void InvalidatedRowWhoseJobIsDeclaredAgain_BecomesProjectionCandidate_Test()
    {
        // The rollback path: the job disappeared (row soft-invalidated), then a rollback re-declared
        // it — the row must resurface as the override so Apply can re-validate it on success.
        var plan = RecurringJobRuntimeReconciler.Reconcile(
            new[] { definition("rolled-back") },
            new[] { row("rolled-back", "*/5 * * * *", "2026-07-02T00:00:00Z", RecurringJobRuntimeStore.InvalidatedJobRemoved) },
            Array.Empty<string>());

        var projection = plan.Projections.Single();
        projection.OverrideCron.ShouldBe("*/5 * * * *");
        projection.Row!.IsInvalidated.ShouldBeTrue();
        plan.MarkJobRemoved.ShouldBeEmpty();
    }

    [Fact]
    public void StorageJobsWithoutDefinitions_AreListedUndeclared_Test()
    {
        var plan = RecurringJobRuntimeReconciler.Reconcile(
            new[] { definition("declared") },
            Array.Empty<RecurringJobRuntimeRow>(),
            new[] { "declared", "dashboard-pilot", "legacy-job" });

        plan.UndeclaredJobIds.ShouldBe(new[] { "dashboard-pilot", "legacy-job" });
    }

    public class TestJob
    {
        public void Run() { }
    }
}
