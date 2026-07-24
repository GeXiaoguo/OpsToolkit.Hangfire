namespace OpsToolkit.Hangfire.JobControl;

/// <summary>
/// Liveness configuration, nested in <see cref="JobControlOptions.Liveness"/>. Always present with
/// defaults — deliberately no null-means-off gate: the detector's only on/off switch is whether the
/// host registered it on the server (<c>AddJobControlStallDetector()</c>, or
/// <c>additionalProcesses</c> on a self-hosted <c>BackgroundJobServer</c>). Registered ⇒ running,
/// flag-only — safe with zero configuration because only contracted executions are ever scanned (an
/// empty index read per interval otherwise).
///
/// Per-job values (<see cref="HeartbeatAttribute.TimeoutSeconds"/>, <see cref="HeartbeatAttribute.OnStall"/>,
/// <see cref="HeartbeatAttribute.MaxRetries"/>, <see cref="HeartbeatAttribute.RetryDelaySeconds"/>) live
/// on the attribute; there is deliberately no global default timeout — a global default would silently
/// enroll nothing today but change the meaning of adding the attribute later (R3: opt-in stays explicit
/// and local).
/// </summary>
public sealed record LivenessOptions
{
    /// <summary>
    /// How often the <see cref="StallDetector"/> runs a scan pass. Default 30s. The contract-start
    /// timeout floor (<see cref="LivenessFilter.TimeoutFloorSeconds"/> = 2 × this default) assumes the
    /// default — a host raising this beyond 30s should keep contract timeouts at ≥ 2 × the raised value,
    /// or silence and a stall become indistinguishable within one confirmation window.
    /// </summary>
    public TimeSpan ScanInterval { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Beat write throttle: <c>Beat()</c> calls closer together than this are coalesced in memory
    /// rather than persisted (job authors may beat per loop iteration without thinking about write
    /// volume). Default 5s.
    /// </summary>
    public TimeSpan MinBeatInterval { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How long a committed stall-cancel may wait for the cancelled execution's own acknowledgment
    /// before the workflow is <b>blocked</b> (surfaced, human-only exit — no queue row is ever added
    /// without an ack, liveness plan §5 Rule 4). Default 60s — at least the host's
    /// <c>CancellationCheckInterval</c> (5s default; the watcher must get a chance to observe the abort)
    /// plus one <see cref="ScanInterval"/> (review F9).
    /// </summary>
    public TimeSpan AckGracePeriod { get; init; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Host-declared upper bound on the storage's invisibility recovery window — set it to at least the
    /// storage invisibility timeout (Hangfire.PostgreSql default: 30 minutes). <b>Required to enable
    /// <see cref="StallAction.Retry"/></b>; when null (the default), retry policies downgrade to
    /// flag-only with an error log. It is the second cancellation gate (liveness plan §5 Rule 1): a
    /// stall must outlive this window under fresh owner heartbeats before a cancel is issued — if the
    /// owner had died instead, storage-native recovery would already have replaced the execution within
    /// the window, and cancelling a possibly-dead owner's job would break that recovery.
    /// </summary>
    public TimeSpan? StorageLeaseWindow { get; init; }

    /// <summary>Actor recorded on liveness-system audit entries (contract failures, stall transitions).</summary>
    public string ActorName { get; init; } = "system:liveness";
}
