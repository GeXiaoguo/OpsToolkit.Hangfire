namespace OpsToolkit.Hangfire.JobControl;

/// <summary>
/// Pure planning half of reconciliation: <c>(code definitions, stored override rows, storage job ids)
/// → plan</c>. No storage, no clock, no Hangfire calls — fully unit-testable. It runs over <i>all</i>
/// rows on every pass, so row validity always reflects the currently deployed code: a row whose job
/// disappeared is marked for soft-invalidation, and an invalidated row whose job is declared again
/// (rollback, rename-back) becomes a projection candidate whose invalidation clears once it projects.
///
/// Cron validity is deliberately <b>not</b> judged here: the executing side
/// (<see cref="RecurringJobRegistrar.Apply"/>) discovers it by attempting the projection itself —
/// Hangfire's <c>AddOrUpdate</c> validates through the same parser the scheduler runs, so "valid"
/// can never drift from what Hangfire actually accepts.
/// </summary>
public static class RecurringJobRuntimeReconciler
{
    public static ReconcilePlan Reconcile(
        IReadOnlyList<RecurringJobTypeDefinition> definitions,
        IReadOnlyList<RecurringJobRuntimeRow> rows,
        IReadOnlyCollection<string> storageJobIds)
    {
        var rowsById = rows.ToDictionary(row => row.JobId);
        var declaredIds = definitions.Select(definition => definition.Id).ToHashSet();

        var projections = definitions
            .Select(definition => new ReconcileProjection(definition, rowsById.GetValueOrDefault(definition.Id)))
            .ToList();

        // Only rows not already carrying the job-removed mark — re-marking every pass would churn the
        // timestamp without conveying anything new.
        var markJobRemoved = rows
            .Where(row => !declaredIds.Contains(row.JobId))
            .Where(row => row.InvalidatedReason != RecurringJobRuntimeStore.InvalidatedJobRemoved)
            .Select(row => row.JobId)
            .ToList();

        var undeclared = storageJobIds.Where(id => !declaredIds.Contains(id)).ToList();

        return new ReconcilePlan(projections, markJobRemoved, undeclared);
    }
}

/// <summary>
/// One definition to project, paired with its stored override row (null when none). An override wins
/// when present and non-empty — including a currently-invalidated row, which the executor retries and
/// re-validates on success (the rollback path) or re-marks on failure (bad-cron/bad-args stays
/// dormant). Effective <i>args</i> aren't computed here: binding the JSON to the method signature is
/// <see cref="JobArgs.Bind"/>'s job, and only the executor can act on its failure.
/// </summary>
public sealed record ReconcileProjection(RecurringJobTypeDefinition Definition, RecurringJobRuntimeRow? Row)
{
    public string? OverrideCron => string.IsNullOrEmpty(Row?.Cron) ? null : Row!.Cron;

    public string? OverrideArgsJson => string.IsNullOrEmpty(Row?.ArgsJson) ? null : Row!.ArgsJson;

    public bool HasOverride => OverrideCron is not null || OverrideArgsJson is not null;

    public string EffectiveCron => OverrideCron ?? Definition.CronDefault;
}

/// <summary>
/// What a reconcile pass should do. <see cref="UndeclaredJobIds"/> (recurring jobs present in storage
/// but not declared in code) is informational unless the executor was explicitly opted into removal.
/// </summary>
public sealed record ReconcilePlan(
    IReadOnlyList<ReconcileProjection> Projections,
    IReadOnlyList<string> MarkJobRemoved,
    IReadOnlyList<string> UndeclaredJobIds);
