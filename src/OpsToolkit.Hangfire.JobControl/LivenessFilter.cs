using System.Collections.Concurrent;
using System.Reflection;
using Hangfire;
using Hangfire.Logging;
using Hangfire.Server;

namespace OpsToolkit.Hangfire.JobControl;

/// <summary>
/// Contract plane of liveness: starts a contracted execution's beat record at <c>OnPerforming</c> and
/// retires its active-index tuple at <c>OnPerformed</c>. Registered by
/// <see cref="GlobalConfigurationExtensions.UseJobControl"/> with the same idempotent guard as the other
/// filters, at default order — i.e. after <see cref="DisabledRecurringJobFilter"/> (order −1), so a
/// disable-skip cancels before this filter runs and skipped occurrences never enter the active set.
/// Inert for any job without <see cref="HeartbeatAttribute"/>: no reads beyond a cached reflection
/// lookup, no writes, no behavior change.
///
/// <b>Never throws.</b> <c>BackgroundJobPerformer</c> routes a filter exception into normal job-failure
/// handling (<c>InvokeOnPerforming</c>/<c>InvokeOnPerformed</c> → <c>HandleJobPerformanceException</c>),
/// which would fail — and, under <c>[AutomaticRetry]</c>, retry-storm — a production job over a liveness
/// problem. Liveness is monitoring; it fails open, with a loud signal (review C4):
/// <c>contract-invalid</c> (bad attribute values) and <c>contract-init-failed</c> (storage failure while
/// enrolling) audit entries mark an execution that is running unmonitored.
/// </summary>
public sealed class LivenessFilter : IServerFilter
{
    /// <summary>
    /// Contract-start floor for <see cref="HeartbeatAttribute.TimeoutSeconds"/> — the F9 resolution's
    /// <c>max(2 × MinBeatInterval, 2 × ScanInterval)</c> at the defaults (5s beat throttle, 30s detector
    /// scan). Kept a constant rather than derived from <see cref="LivenessOptions"/>: the filter and the
    /// detector may run in different processes with different configuration, and a floor that moved with
    /// one process's options would silently change which contracts another process's detector honors.
    /// </summary>
    internal const int TimeoutFloorSeconds = 60;

    private sealed record ContractResolution(HeartbeatAttribute? Attribute, string? InvalidReason)
    {
        public static readonly ContractResolution None = new(null, null);
    }

    private static readonly ConcurrentDictionary<MethodInfo, ContractResolution> ResolutionCache = new();

    // contract-invalid is a static misconfiguration: audit it once per method per process (the audit
    // list is human-scale; a minutely job must not flood it with an identical line per run) but log an
    // error on every occurrence so the signal also lives where repetition is cheap.
    private static readonly ConcurrentDictionary<MethodInfo, bool> InvalidContractAudited = new();

    private readonly int _auditMaxEntries;
    private readonly LivenessOptions _liveness;

    public LivenessFilter(int auditMaxEntries, LivenessOptions? liveness = null)
    {
        _auditMaxEntries = auditMaxEntries;
        _liveness = liveness ?? new LivenessOptions();
    }

    public void OnPerforming(PerformingContext context)
    {
        try
        {
            StartContract(context);
        }
        catch (Exception ex)
        {
            LogProvider.GetLogger(typeof(LivenessFilter)).ErrorException(
                $"Failed to start the liveness contract for job {context.BackgroundJob.Id}; the execution runs unmonitored.", ex);
            TryAudit("contract-init-failed", context, ex.Message, alwaysAudit: true);
        }
    }

    public void OnPerformed(PerformedContext context)
    {
        try
        {
            if (!context.Items.TryGetValue(PerformContextLivenessExtensions.ExecutionIdItemKey, out var value)
                || value is not string executionId)
            {
                return;
            }

            LivenessStore.EndContract(context.Connection, context.BackgroundJob.Id, executionId);
        }
        catch (Exception ex)
        {
            // Leftover tuple self-heals: the detector removes members whose job is no longer Processing.
            LogProvider.GetLogger(typeof(LivenessFilter)).ErrorException(
                $"Failed to retire the liveness contract for job {context.BackgroundJob.Id}.", ex);
        }
    }

    private void StartContract(PerformingContext context)
    {
        var method = context.BackgroundJob.Job?.Method;
        if (method is null) return;

        var resolution = ResolutionCache.GetOrAdd(method, Resolve);
        if (resolution.Attribute is null && resolution.InvalidReason is null) return;

        if (resolution.InvalidReason is not null)
        {
            LogProvider.GetLogger(typeof(LivenessFilter)).Error(
                $"Invalid [Heartbeat] contract on {method.DeclaringType?.Name}.{method.Name}: {resolution.InvalidReason} " +
                $"Job {context.BackgroundJob.Id} runs unmonitored.");
            if (InvalidContractAudited.TryAdd(method, true))
                TryAudit("contract-invalid", context, resolution.InvalidReason, alwaysAudit: false);
            return;
        }

        var now = DateTime.UtcNow;
        var record = new BeatRecord(
            BeatRecord.CurrentVersion,
            ExecutionId: Guid.NewGuid().ToString("N"),
            StartedAt: now,
            TimeoutSeconds: resolution.Attribute!.TimeoutSeconds,
            ServerId: context.ServerId,
            Seq: 1,               // contract start doubles as beat #1 — the clock starts at Processing
            BeatAt: now,
            Percent: null,
            Message: null,
            // The stall policy is part of the contract snapshot (F7): the detector applies what the
            // executing version enrolled with, never its own reflection of a redeployed assembly.
            OnStall: resolution.Attribute!.OnStall,
            MaxRetries: resolution.Attribute.MaxRetries,
            RetryDelaySeconds: resolution.Attribute.RetryDelaySeconds);

        LivenessStore.StartContract(context.Connection, context.BackgroundJob.Id, record);

        // Items are populated only after every enrollment write succeeded: on failure Beat() stays a
        // no-op and OnPerformed retires nothing — honestly unmonitored, never half-monitored.
        context.Items[PerformContextLivenessExtensions.ExecutionIdItemKey] = record.ExecutionId;
        context.Items[PerformContextLivenessExtensions.BeatStateItemKey] = new LivenessBeatState(record, _liveness.MinBeatInterval);

        // Review C5: enrollment succeeding while an older execution of this job id is still flagged
        // stalled means storage-native invisibility recovery replaced that execution (only a fresh
        // OnPerforming reaches here, and a zombie never re-runs it). In its own guarded step, after the
        // Items handoff — a failure here must not unmonitor the new contract.
        try
        {
            RetireSupersededStalls(context, record.ExecutionId);
        }
        catch (Exception ex)
        {
            LogProvider.GetLogger(typeof(LivenessFilter)).WarnException(
                $"Failed to retire a superseded stall flag for job {context.BackgroundJob.Id}; the detector's next scan self-heals it.", ex);
        }
    }

    /// <summary>
    /// When a prior stalled tuple for the same job id exists under a different execution identity,
    /// retire it by identity and audit <c>stall-native-refetch-observed</c> — an <i>observation</i> of
    /// storage-native recovery, not proof the old body stopped (review C5). Supersede-cleanup is a stall
    /// phase transition, so the rare path that actually found one runs under the per-job lock with the
    /// marker re-read inside (review F4); a lock timeout skips — the detector self-heals the tuple.
    /// </summary>
    private void RetireSupersededStalls(PerformingContext context, string currentExecutionId)
    {
        var connection = context.Connection;
        var jobId = context.BackgroundJob.Id;
        var superseded = LivenessStore.ReadStalledMembers(connection)
            .Select(LivenessStore.TryParseActiveMember)
            .Where(tuple => tuple is not null && tuple.Value.JobId == jobId && tuple.Value.ExecutionId != currentExecutionId)
            .Select(tuple => tuple!.Value.ExecutionId)
            .ToList();
        if (superseded.Count == 0) return;

        using var _ = connection.AcquireDistributedLock(LivenessStore.JobLockResource(jobId), TimeSpan.FromSeconds(2));
        foreach (var staleExecutionId in superseded)
        {
            var marker = LivenessStore.ReadStall(connection, jobId, staleExecutionId);
            LivenessStore.ClearStall(connection, jobId, staleExecutionId);
            LivenessStore.EndContract(connection, jobId, staleExecutionId);
            if (marker is null) continue; // another actor already retired it inside its own lock — nothing to audit

            TryAuditStore(context, "stall-native-refetch-observed",
                "A new execution enrolled while an older execution's stall flag was still present — " +
                "storage-native invisibility recovery replaced the stalled attempt; its flag is retired by identity.",
                new Dictionary<string, string>
                {
                    ["SupersededExecutionId"] = staleExecutionId,
                    ["ExecutionId"] = currentExecutionId,
                });
        }
    }

    private static ContractResolution Resolve(MethodInfo method)
    {
        try
        {
            var attribute = method.GetCustomAttribute<HeartbeatAttribute>(inherit: true)
                ?? method.DeclaringType?.GetCustomAttribute<HeartbeatAttribute>(inherit: true);
            if (attribute is null) return ContractResolution.None;

            if (attribute.TimeoutSeconds < TimeoutFloorSeconds)
            {
                return new ContractResolution(null,
                    $"TimeoutSeconds must be at least {TimeoutFloorSeconds} (got {attribute.TimeoutSeconds}) — " +
                    "below twice the detector scan interval, silence and a stall are indistinguishable.");
            }

            // Settable attribute properties can't validate eagerly in the constructor the way
            // timeoutSeconds does — this is their contract-start gate (same F9/C4 rule: unmonitored +
            // audit, never a thrown exception into the job).
            if (attribute.MaxRetries < 0)
                return new ContractResolution(null, $"MaxRetries must be zero or positive (got {attribute.MaxRetries}).");
            if (attribute.RetryDelaySeconds < 0)
                return new ContractResolution(null, $"RetryDelaySeconds must be zero or positive (got {attribute.RetryDelaySeconds}).");

            return new ContractResolution(attribute, null);
        }
        catch (Exception ex)
        {
            // HeartbeatAttribute's constructor validates eagerly, so a statically invalid value throws
            // here, at materialization — deterministic, cached, and never enrolls an unsafe contract.
            return new ContractResolution(null, $"{ex.GetType().Name}: {ex.Message}");
        }
    }

    private void TryAudit(string action, PerformingContext context, string reason, bool alwaysAudit)
    {
        _ = alwaysAudit; // both actions currently audit whenever called; the flag documents caller intent
        TryAuditStore(context, action, reason, detail: null);
    }

    private void TryAuditStore(PerformingContext context, string action, string reason, Dictionary<string, string>? detail)
    {
        try
        {
            var storage = context.Storage ?? JobStorage.Current;
            var method = context.BackgroundJob.Job?.Method;
            if (method is not null)
            {
                detail ??= new Dictionary<string, string>();
                detail["Method"] = $"{method.DeclaringType?.Name}.{method.Name}";
            }
            AuditStore.Append(storage, new AuditEntry(
                AuditEntry.CurrentVersion, DateTime.UtcNow, _liveness.ActorName, action, context.BackgroundJob.Id,
                reason, "ok", detail), _auditMaxEntries);
        }
        catch (Exception ex)
        {
            LogProvider.GetLogger(typeof(LivenessFilter)).ErrorException($"Failed to record a {action} audit entry.", ex);
        }
    }
}
