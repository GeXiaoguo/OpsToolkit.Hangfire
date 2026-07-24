using System.Linq.Expressions;
using Hangfire;
using Hangfire.Common;

namespace OpsToolkit.Hangfire.JobControl;

/// <summary>
/// The declaration site for code-owned recurring jobs, replacing a host's direct
/// <c>RecurringJob.AddOrUpdate</c> calls: <see cref="Register{T}(string,Expression{Action{T}},string,TimeZoneInfo?)"/>
/// records each job's <see cref="RecurringJobTypeDefinition"/>, then one <see cref="Apply"/> projects
/// <c>effective = operator override ?? code default</c> into Hangfire for every definition — the
/// schedule and the parameter values both. That split is what makes an operator's change durable: a
/// plain <c>AddOrUpdate</c> re-asserts the code's cron and arguments at every deploy, while a
/// registrar host re-asserts the <i>effective</i> values instead.
///
/// Share the instance with the HTTP plane via <see cref="JobControlOptions.Registrar"/> — the
/// override endpoints need the definitions to rebuild the <see cref="Job"/> (the stored payload is
/// never trusted) and to answer what the code default is.
///
/// Rollout caveat for hosts running old and new versions simultaneously (canary/rolling deploys): a
/// pod still running plain <c>AddOrUpdate</c> registration clobbers overrides in Hangfire storage on
/// restart, and nothing re-projects them until a registrar-version startup, an override edit, or the
/// reconcile endpoint. Self-resolves once the rollout that introduces the registrar completes.
/// </summary>
public sealed class RecurringJobRegistrar
{
    private readonly List<RecurringJobTypeDefinition> _definitions = new();

    /// <summary>Registration order, which is also projection order.</summary>
    public IReadOnlyList<RecurringJobTypeDefinition> Definitions => _definitions;

    public RecurringJobTypeDefinition? Find(string id) =>
        _definitions.FirstOrDefault(definition => definition.Id == id);

    public RecurringJobRegistrar Register<T>(
        string id, Expression<Action<T>> methodCall, string cronDefault, TimeZoneInfo? timeZone = null)
        => add(id, Job.FromExpression(methodCall), cronDefault, timeZone);

    public RecurringJobRegistrar Register<T>(
        string id, Expression<Func<T, Task>> methodCall, string cronDefault, TimeZoneInfo? timeZone = null)
        => add(id, Job.FromExpression(methodCall), cronDefault, timeZone);

    private RecurringJobRegistrar add(string id, Job job, string cronDefault, TimeZoneInfo? timeZone)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(cronDefault);
        if (Find(id) is not null)
            throw new InvalidOperationException(
                $"Recurring job '{id}' is already registered — duplicate ids would make the last registration silently win.");

        _definitions.Add(new RecurringJobTypeDefinition(id, job, cronDefault, timeZone ?? TimeZoneInfo.Utc));
        return this;
    }

    /// <summary>
    /// Reconciles stored overrides against the definitions and projects every job. Call once at
    /// startup after all <c>Register</c> calls, and again on demand via the reconcile endpoint.
    ///
    /// A failing projection never throws out of this method — a bad stored row must not crash-loop the
    /// host at startup. An override whose cron Hangfire rejects is soft-invalidated (<c>bad-cron</c>)
    /// and the job falls back to its code default; any other projection failure is captured in
    /// <see cref="ReconcileSummary.Errors"/> and the job keeps whatever storage holds.
    /// </summary>
    /// <param name="storage">The Hangfire storage to reconcile against and project into.</param>
    /// <param name="options">Audit retention configuration; defaults apply when null.</param>
    /// <param name="actor">Recorded as the audit actor — the HTTP reconcile endpoint passes the request identity.</param>
    /// <param name="removeUndeclared">
    /// Opt-in "code owns the recurring-job set": remove every recurring job in storage that no
    /// <c>Register</c> call declared. Off by default because it deletes jobs a host registered outside
    /// this registrar (plain <c>AddOrUpdate</c>, dashboard-created pilots). Each removal is audited.
    /// </param>
    /// <param name="auditEvenIfUnchanged">
    /// A summary audit entry is always written when the pass changed something (invalidated,
    /// revalidated, or removed anything, or hit errors); pass true to record a no-change pass too —
    /// the HTTP reconcile endpoint does, so an operator's explicit action always leaves a trace,
    /// while routine startups don't flood the trail.
    /// </param>
    public ReconcileSummary Apply(
        JobStorage storage,
        JobControlOptions? options = null,
        string actor = "system:startup",
        bool removeUndeclared = false,
        bool auditEvenIfUnchanged = false)
    {
        var jobControlOptions = options ?? new JobControlOptions();

        ReconcilePlan plan;
        using (var connection = storage.GetConnection())
        {
            plan = RecurringJobRuntimeReconciler.Reconcile(
                _definitions,
                RecurringJobRuntimeStore.LoadAll(connection),
                connection.GetAllItemsFromSet("recurring-jobs"));
        }

        var manager = new RecurringJobManager(storage);
        var overridden = new List<string>();
        var invalidated = new List<string>();
        var revalidated = new List<string>();
        var removed = new List<string>();
        var errors = new List<string>();

        foreach (var projection in plan.Projections)
        {
            var definition = projection.Definition;
            try
            {
                var outcome = ProjectEffective(storage, manager, definition, projection.Row);
                if (outcome.DormantReason is { } dormantReason)
                {
                    invalidated.Add($"{definition.Id}: {dormantReason}");
                    errors.Add($"{definition.Id}: {outcome.Error}");
                }
                else if (outcome.OverrideApplied)
                {
                    overridden.Add(definition.Id);
                    if (projection.Row!.IsInvalidated)
                    {
                        RecurringJobRuntimeStore.ClearInvalidated(storage, definition.Id);
                        revalidated.Add(definition.Id);
                    }
                }
            }
            catch (Exception ex)
            {
                errors.Add($"{definition.Id}: {ex.Message}");
            }
        }

        foreach (var jobId in plan.MarkJobRemoved)
        {
            RecurringJobRuntimeStore.MarkInvalidated(
                storage, jobId, RecurringJobRuntimeStore.InvalidatedJobRemoved, DateTime.UtcNow);
            invalidated.Add($"{jobId}: {RecurringJobRuntimeStore.InvalidatedJobRemoved}");
        }

        if (removeUndeclared)
        {
            foreach (var jobId in plan.UndeclaredJobIds)
            {
                manager.RemoveIfExists(jobId);
                removed.Add(jobId);
                AuditStore.Append(storage, new AuditEntry(
                    AuditEntry.CurrentVersion, DateTime.UtcNow, actor, "remove-undeclared", jobId,
                    Reason: "recurring job not declared in code", "ok", Detail: null),
                    jobControlOptions.AuditMaxEntries);
            }
        }

        var summary = new ReconcileSummary(
            plan.Projections.Count, overridden, invalidated, revalidated, removed, plan.UndeclaredJobIds, errors);

        var changed = invalidated.Count > 0 || revalidated.Count > 0 || removed.Count > 0 || errors.Count > 0;
        if (changed || auditEvenIfUnchanged)
        {
            AuditStore.Append(storage, new AuditEntry(
                AuditEntry.CurrentVersion, DateTime.UtcNow, actor, "reconcile", JobId: "*", Reason: null,
                errors.Count == 0 ? "ok" : "errors", detail(summary)),
                jobControlOptions.AuditMaxEntries);
        }

        return summary;
    }

    /// <summary>
    /// Projects one definition's <c>effective = override ?? default</c> from its stored row — the
    /// single place the per-row semantics live, shared by <see cref="Apply"/> and the override
    /// endpoints so they cannot drift. The row projects <b>as a unit</b>: a failing part
    /// (unbindable args, unparseable cron) marks the whole row dormant and the full code defaults
    /// run — "override dormant" must never mean "half an override applied". Args are judged first
    /// (a pure <see cref="JobArgs.Bind"/>), cron by the projection attempt itself (Hangfire's
    /// <c>AddOrUpdate</c> parses before writing). Never throws for a bad stored value; anything
    /// else (storage down) propagates to the caller.
    /// </summary>
    internal static ProjectionOutcome ProjectEffective(
        JobStorage storage, RecurringJobManager manager, RecurringJobTypeDefinition definition, RecurringJobRuntimeRow? row)
    {
        var projection = new ReconcileProjection(definition, row);
        if (!projection.HasOverride)
        {
            project(manager, definition, definition.Job, definition.CronDefault);
            return new ProjectionOutcome(OverrideApplied: false, DormantReason: null, Error: null);
        }

        var binding = projection.OverrideArgsJson is { } argsJson ? JobArgs.Bind(definition, argsJson) : null;
        if (binding is { Succeeded: false })
        {
            RecurringJobRuntimeStore.MarkInvalidated(
                storage, definition.Id, RecurringJobRuntimeStore.InvalidatedBadArgs, DateTime.UtcNow);
            project(manager, definition, definition.Job, definition.CronDefault);
            return new ProjectionOutcome(false, RecurringJobRuntimeStore.InvalidatedBadArgs,
                $"override args rejected — {string.Join("; ", binding.Errors)}");
        }
        var job = binding is null
            ? definition.Job
            : new Job(definition.Job.Type, definition.Job.Method, binding.Args!.ToArray()!);

        if (projection.OverrideCron is { } overrideCron)
        {
            try
            {
                project(manager, definition, job, overrideCron);
            }
            catch (ArgumentException ex)
            {
                RecurringJobRuntimeStore.MarkInvalidated(
                    storage, definition.Id, RecurringJobRuntimeStore.InvalidatedBadCron, DateTime.UtcNow);
                project(manager, definition, definition.Job, definition.CronDefault);
                return new ProjectionOutcome(false, RecurringJobRuntimeStore.InvalidatedBadCron,
                    $"override cron '{overrideCron}' rejected — {ex.Message}");
            }
        }
        else
        {
            project(manager, definition, job, definition.CronDefault);
        }
        return new ProjectionOutcome(OverrideApplied: true, DormantReason: null, Error: null);
    }

    private static void project(RecurringJobManager manager, RecurringJobTypeDefinition definition, Job job, string cron) =>
        manager.AddOrUpdate(definition.Id, job, cron, new RecurringJobOptions { TimeZone = definition.TimeZone });

    // The audit list is human-scale, so the ids themselves fit; counts alone would send the reader
    // back to logs that may have rotated.
    private static Dictionary<string, string> detail(ReconcileSummary summary)
    {
        var result = new Dictionary<string, string> { ["Projected"] = summary.Projected.ToString() };
        addIfAny("Overridden", summary.OverriddenJobIds);
        addIfAny("Invalidated", summary.InvalidatedJobIds);
        addIfAny("Revalidated", summary.RevalidatedJobIds);
        addIfAny("Removed", summary.RemovedUndeclaredJobIds);
        addIfAny("Errors", summary.Errors);
        return result;

        void addIfAny(string key, IReadOnlyList<string> values)
        {
            if (values.Count > 0) result[key] = string.Join(", ", values);
        }
    }
}

/// <summary>
/// What projecting one row did. <see cref="DormantReason"/> is the invalidation mark written when a
/// part of the override failed (<see cref="RecurringJobRuntimeStore.InvalidatedBadArgs"/>/<see
/// cref="RecurringJobRuntimeStore.InvalidatedBadCron"/>), with <see cref="Error"/> carrying the
/// human-readable detail; both null when nothing failed. <see cref="OverrideApplied"/> is true only
/// when an override value (schedule and/or args) actually landed in Hangfire.
/// </summary>
internal sealed record ProjectionOutcome(bool OverrideApplied, string? DormantReason, string? Error);

/// <summary>
/// What one <see cref="RecurringJobRegistrar.Apply"/> pass did. <see cref="InvalidatedJobIds"/> and
/// <see cref="Errors"/> entries are <c>"jobId: detail"</c> strings.
/// <see cref="UndeclaredJobIds"/> lists storage jobs no definition covers, whether or not removal was
/// requested; <see cref="RemovedUndeclaredJobIds"/> is what was actually removed.
/// </summary>
public sealed record ReconcileSummary(
    int Projected,
    IReadOnlyList<string> OverriddenJobIds,
    IReadOnlyList<string> InvalidatedJobIds,
    IReadOnlyList<string> RevalidatedJobIds,
    IReadOnlyList<string> RemovedUndeclaredJobIds,
    IReadOnlyList<string> UndeclaredJobIds,
    IReadOnlyList<string> Errors);
