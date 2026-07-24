using Hangfire.Server;

namespace OpsToolkit.Hangfire.JobControl;

/// <summary>
/// What the stall detector does once it has <b>confirmed</b> a contracted execution's silence (the flag
/// itself is always written first, and a resumed beat always recovers it).
/// </summary>
public enum StallAction
{
    /// <summary>
    /// Flag and surface only — the default. A slow-but-fine job (one long silent DB query) that got
    /// flagged recovers on its own beat; the operator decides everything else.
    /// </summary>
    Flag = 0,

    /// <summary>
    /// The governed retry workflow (liveness plan §5): cancel the hung-but-alive body through the shared
    /// cancellation protocol, wait for the execution's own acknowledgment, and only then requeue —
    /// bounded by <see cref="HeartbeatAttribute.MaxRetries"/>, delayed by
    /// <see cref="HeartbeatAttribute.RetryDelaySeconds"/>. Requires the host to declare
    /// <see cref="LivenessOptions.StorageLeaseWindow"/> and sliding storage invisibility — without
    /// either, the policy downgrades to <see cref="Flag"/> with a loud log, never a crash.
    /// </summary>
    Retry = 1,
}

/// <summary>
/// Opt-in liveness contract for a long-running job — the heartbeat arm of the long-running-job
/// capability (the cancellation arm already ships in this package). A method (or its declaring type)
/// carrying this attribute promises to call
/// <see cref="PerformContextLivenessExtensions.Beat(PerformContext, double?, string?)"/> at meaningful
/// progress points; <see cref="TimeoutSeconds"/> is how long the execution may stay silent before the
/// stall detector may flag it stalled, and <see cref="OnStall"/> is what a confirmed stall leads to
/// (flag-only by default; the governed retry workflow by opt-in).
/// A job without this attribute is untouched by liveness: no storage is written for it, nothing will
/// ever scan it, and its behavior is bit-for-bit what it is today.
/// </summary>
/// <remarks>
/// Deliberately a plain attribute rather than registrar-declared configuration or a
/// <c>JobFilterAttribute</c>: <see cref="LivenessFilter"/> resolves it by reflection at execution time
/// (cached per method), so the contract works for every host — including plain-<c>AddOrUpdate</c> hosts
/// that never adopt the registrar — and travels with the method into ad-hoc runs, including
/// manual-invoke force-runs.
/// </remarks>
[AttributeUsage(AttributeTargets.Method | AttributeTargets.Class, Inherited = true, AllowMultiple = false)]
public sealed class HeartbeatAttribute : Attribute
{
    /// <summary>
    /// <paramref name="timeoutSeconds"/> must be positive — validated here, eagerly, so a statically
    /// invalid value fails at attribute materialization (the first reflection over the method) instead
    /// of silently enrolling an unsafe contract. The higher floor relative to the beat-throttle and
    /// detector-scan intervals isn't knowable at construction; <see cref="LivenessFilter"/> enforces it
    /// at contract start, where failing means "run unmonitored + audit", never a thrown exception.
    /// </summary>
    public HeartbeatAttribute(int timeoutSeconds)
    {
        if (timeoutSeconds <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(timeoutSeconds), timeoutSeconds, "Heartbeat timeout must be a positive number of seconds.");
        }

        TimeoutSeconds = timeoutSeconds;
    }

    /// <summary>Maximum silence (no persisted beat) the execution is allowed before it counts as stalled.</summary>
    public int TimeoutSeconds { get; }

    /// <summary>
    /// What a confirmed stall leads to. Default <see cref="StallAction.Flag"/> — anything automatic is
    /// opt-in, per job, next to the code it governs.
    /// </summary>
    public StallAction OnStall { get; set; } = StallAction.Flag;

    /// <summary>
    /// Stall-retry budget for the whole background job (each retry runs as a fresh execution; the count
    /// rides on the job). Must be ≥ 0 — enforced at contract start, like the timeout floor. Zero is a
    /// meaningful policy: cancel the confirmed-hung body, never requeue ("kill on stall"). Entirely
    /// separate from <c>[AutomaticRetry]</c>'s budget — the two paths are mutually exclusive per
    /// terminal transition (liveness plan §5 Rule 5).
    /// </summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    /// Delay before a stall-retry re-runs, honored via Hangfire's own <c>ScheduledState</c> (so a
    /// detector restart can never double- or re-time it). Must be ≥ 0 — enforced at contract start.
    /// </summary>
    public int RetryDelaySeconds { get; set; } = 60;
}
