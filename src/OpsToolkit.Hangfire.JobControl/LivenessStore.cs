using System.Text.Json;
using Hangfire.Storage;

namespace OpsToolkit.Hangfire.JobControl;

/// <summary>
/// One execution's liveness record: the immutable contract snapshot written at contract start
/// (<see cref="ExecutionId"/>, <see cref="StartedAt"/>, <see cref="TimeoutSeconds"/>,
/// <see cref="ServerId"/> — review F7: the detector must evaluate the values the *executing* version
/// enrolled with, never its own reflection of a possibly different deployed version) plus the latest
/// beat (<see cref="Seq"/>, <see cref="BeatAt"/>, <see cref="Percent"/>, <see cref="Message"/>).
/// <see cref="Seq"/> is the per-execution monotonic beat counter — review C3: "unchanged" means an
/// unchanged sequence number, so progress text or future payload fields can never perturb stall
/// semantics.
/// </summary>
public sealed record BeatRecord(
    int V,
    string ExecutionId,
    DateTime StartedAt,
    int TimeoutSeconds,
    string? ServerId,
    long Seq,
    DateTime BeatAt,
    double? Percent,
    string? Message,
    StallAction OnStall = StallAction.Flag,
    int MaxRetries = 0,
    int RetryDelaySeconds = 0)
{
    // V2 added the OnStall/MaxRetries/RetryDelaySeconds policy snapshot (still review F7: the detector
    // applies the values the executing version enrolled with). A V1 record deserializes with the
    // defaults above — OnStall = Flag — which is exactly the behavior its version shipped with.
    public const int CurrentVersion = 2;
}

/// <summary>
/// One execution's stall flag, written by the <see cref="StallDetector"/> when the execution's
/// <see cref="BeatRecord.Seq"/> stayed unchanged for the contract's timeout on the detector's own
/// observation clock. <see cref="Seq"/> is the sequence number that was frozen at flag time — a later
/// beat with a higher one is the recovery signal, durable across detector restarts. Acknowledgment
/// (review F8) is <b>not</b> clearing: the stalled condition remains true (the beat <i>is</i> overdue —
/// clearing would silently re-flag); the operator's identity/reason/time ride on the flag so alerting
/// can page on unacknowledged stalls only. Retires automatically on recovery or terminal state.
/// </summary>
public sealed record StallMarker(
    int V,
    string ExecutionId,
    DateTime StalledAt,
    long Seq,
    string? AcknowledgedBy,
    DateTime? AcknowledgedAt,
    string? AcknowledgeReason)
{
    public const int CurrentVersion = 1;
}

/// <summary>
/// One execution's stall-retry workflow record (liveness plan §5 Rule 3): the durable phase the
/// post-cancel workflow is in, written before the cancel transition and advanced only under the per-job
/// lock. Everything a later detector (or a replacement after restart) needs to resume the workflow is
/// immutable here: the prepared request's id, which retry this cancel leads to
/// (<see cref="AttemptNumber"/> — the job-scoped count of retries already spent, plus one), and the
/// policy snapshot the executing version enrolled with (<see cref="MaxRetries"/>,
/// <see cref="RetryDelaySeconds"/> — copied from the beat record so the workflow never re-reads a
/// possibly-superseded contract). Terminal phases (<see cref="PhaseBlocked"/>,
/// <see cref="PhaseExhausted"/>, …) persist for the job's lifetime as the workflow's own record — the
/// requeue guard and the stalled surface key off them.
/// </summary>
public sealed record StallAttemptRecord(
    int V,
    string ExecutionId,
    string RequestId,
    string Phase,
    int AttemptNumber,
    int MaxRetries,
    int RetryDelaySeconds,
    DateTime CancelRequestedAt,
    DateTime UpdatedAt,
    string? Detail)
{
    public const int CurrentVersion = 1;

    /// <summary>Cancel prepared and committed; waiting for the execution's acknowledgment.</summary>
    public const string PhaseCancelRequested = "cancel-requested";

    /// <summary>No matching ack within the grace period, or the owner went absent/stale after the cancel committed — surfaced, human-only exit (§5 Rule 4).</summary>
    public const string PhaseBlocked = "blocked";

    /// <summary>Acknowledged, but the retry budget is spent — the job stays Deleted (§5 state machine).</summary>
    public const string PhaseExhausted = "exhausted";

    /// <summary>Acknowledged and requeued — the terminal success of one workflow round.</summary>
    public const string PhaseRetried = "retried";

    /// <summary>Another Hangfire transition won against the cancel — identity-scoped cleanup only (review F6).</summary>
    public const string PhaseSuperseded = "superseded";

    /// <summary>The body completed despite the cancel — nothing left to retry; the cancel-ack entry is the record.</summary>
    public const string PhaseCompletedAnyway = "completed-anyway";

    /// <summary>An operator break-glassed past the blocked state via force-requeue (§5 Rule 4).</summary>
    public const string PhaseForceRequeued = "force-requeued";
}

/// <summary>
/// One detector server's health lease (reviews F5/C2): renewed after every successful scan pass, so
/// "no detector running" is <i>visible</i> as degraded status — never mistaken for "no stalls". Stored
/// as a hash under <c>jobcontrol:liveness:detector:{serverId}</c> with a storage expiry several scan
/// intervals long (expiry-self-healed); freshness at read time is judged from
/// <see cref="LastScanAt"/> via <see cref="LivenessStore.IsDetectorLeaseFresh"/>, so status stays
/// honest even on a storage without the extended expiry API.
/// </summary>
public sealed record DetectorLease(int V, string ServerId, DateTime LastScanAt, int ScanIntervalSeconds)
{
    public const int CurrentVersion = 1;
}

/// <summary>
/// Reads and writes liveness state in Hangfire's own storage through core interface members — the same
/// no-table/no-migration approach as <see cref="CancellationRequestStore"/>, whose bespoke JSON
/// (de)serialization rationale (avoiding the double-encoding convenience wrappers) applies here verbatim.
///
/// Everything is <b>execution-scoped</b> (review B3): the beat record lives under
/// <c>JobControl.Beat:{executionId}</c> and the active index stores <c>{jobId}|{executionId}</c> tuples,
/// so a zombie attempt can only ever write its own records and retire its own tuple — it cannot
/// overwrite or un-index the current attempt. Storage-native invisibility recovery makes such overlap
/// possible for a single background-job id even with no unsafe requeue anywhere in this package.
/// The one deliberately last-writer-wins cell is the current-execution pointer
/// (<c>JobControl.Liveness.Current</c>), written only by <see cref="StartContract"/>: a zombie never
/// re-runs <c>OnPerforming</c>, so the latest contract start is by definition the current attempt
/// (review C1's "guarded current pointer"). Job parameters expire with the job, so none of this needs a
/// cleanup process; the beat record is intentionally <i>not</i> cleared at contract end, letting
/// terminal views show an execution's final progress.
/// </summary>
public static class LivenessStore
{
    private const string ActiveSetKey = "jobcontrol:liveness:active";
    private const string StalledSetKey = "jobcontrol:liveness:stalled";
    private const string RetryPendingSetKey = "jobcontrol:liveness:retry-pending";
    private const string DetectorIndexSetKey = "jobcontrol:liveness:detectors";
    private const string CurrentPointerParameterName = "JobControl.Liveness.Current";
    private const string StallRetryCountParameterName = "JobControl.StallRetryCount";

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private static string BeatParameterName(string executionId) => $"JobControl.Beat:{executionId}";

    private static string StallParameterName(string executionId) => $"JobControl.Stalled:{executionId}";

    private static string StallAttemptParameterName(string executionId) => $"JobControl.StallAttempt:{executionId}";

    private static string DetectorLeaseKey(string serverId) => $"jobcontrol:liveness:detector:{serverId}";

    /// <summary>
    /// Resource name of the narrowly-scoped per-job distributed lock every stall <b>phase transition</b>
    /// runs under (review F4): core job-parameter writes have no compare-and-set, so "guarded by marker
    /// presence" alone would be a check-then-write race between detectors on different servers. State is
    /// re-read inside the lock; acquisition uses a short timeout and failure skips the job for that scan
    /// — another owner has it. Lock-free by design: <c>Beat()</c> and contract-end cleanup, whose writes
    /// are execution-scoped (review B3).
    /// </summary>
    internal static string JobLockResource(string jobId) => $"jobcontrol:liveness:job:{jobId}";

    public static string ActiveMember(string jobId, string executionId) => $"{jobId}|{executionId}";

    /// <summary>
    /// Splits an active-index member back into its tuple. The separator split is on the <b>last</b>
    /// <c>'|'</c>: execution ids are hex GUIDs this store minted (never contain one), while a job id's
    /// shape belongs to the storage provider and is not ours to assume.
    /// </summary>
    public static (string JobId, string ExecutionId)? TryParseActiveMember(string member)
    {
        var separator = member.LastIndexOf('|');
        if (separator <= 0 || separator == member.Length - 1) return null;
        return (member[..separator], member[(separator + 1)..]);
    }

    /// <summary>
    /// Writes the contract-start record (which doubles as beat #1 — the liveness clock starts at
    /// Processing, not enqueue), points the current-execution pointer at it, and indexes the tuple.
    /// Not atomic across the three writes; the caller (<see cref="LivenessFilter"/>) treats any failure
    /// as contract-init-failed and leaves the execution honestly unmonitored — a partially written
    /// record is unreferenced leftovers that expire with the job, never a half-monitored contract.
    /// </summary>
    public static void StartContract(IStorageConnection connection, string jobId, BeatRecord start)
    {
        connection.SetJobParameter(jobId, BeatParameterName(start.ExecutionId), JsonSerializer.Serialize(start, JsonOptions));
        connection.SetJobParameter(jobId, CurrentPointerParameterName, start.ExecutionId);

        using var transaction = connection.CreateWriteTransaction();
        transaction.AddToSet(ActiveSetKey, ActiveMember(jobId, start.ExecutionId));
        transaction.Commit();
    }

    /// <summary>Updates the execution's own record only — stale-execution isolation by construction.</summary>
    public static void WriteBeat(IStorageConnection connection, string jobId, BeatRecord record)
        => connection.SetJobParameter(jobId, BeatParameterName(record.ExecutionId), JsonSerializer.Serialize(record, JsonOptions));

    /// <summary>Null when absent, cleared, or unparsable — a corrupt/foreign value must not throw (the <see cref="CancellationRequestStore.Read"/> rule).</summary>
    public static BeatRecord? ReadBeat(IStorageConnection connection, string jobId, string executionId)
    {
        var raw = connection.GetJobParameter(jobId, BeatParameterName(executionId));
        if (string.IsNullOrEmpty(raw)) return null;

        try
        {
            return JsonSerializer.Deserialize<BeatRecord>(raw, JsonOptions);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>The record the current-execution pointer designates — what UI projection displays.</summary>
    public static BeatRecord? ReadCurrentBeat(IStorageConnection connection, string jobId)
    {
        var executionId = connection.GetJobParameter(jobId, CurrentPointerParameterName);
        return string.IsNullOrEmpty(executionId) ? null : ReadBeat(connection, jobId, executionId);
    }

    /// <summary>
    /// Retires exactly this execution's tuple from both indexes (review B3: an old attempt's
    /// <c>OnPerformed</c> must not be able to un-index a newer attempt, which a job-id-keyed removal
    /// would) — the stalled index too, since terminal state is one of its documented removal causes.
    /// The beat record, stall marker, and pointer are left in place deliberately — job parameters expire
    /// with the job, and terminal views can keep showing an execution's final progress.
    /// </summary>
    public static void EndContract(IStorageConnection connection, string jobId, string executionId)
    {
        using var transaction = connection.CreateWriteTransaction();
        var member = ActiveMember(jobId, executionId);
        transaction.RemoveFromSet(ActiveSetKey, member);
        transaction.RemoveFromSet(StalledSetKey, member);
        transaction.Commit();
    }

    /// <summary>
    /// Removes exactly this execution's tuple from the <b>active</b> index only — the detector's
    /// self-heal for an execution that left Processing. Distinct from <see cref="EndContract"/> (which
    /// also retires the stalled tuple) because a stall-cancelled execution must stay <i>surfaced</i>
    /// while its post-cancel workflow runs (liveness plan §5 state machine: Cancel requested / Blocked /
    /// Exhausted all keep the stalled entry) — the stalled index has its own retention sweep.
    /// </summary>
    public static void RemoveActiveMember(IStorageConnection connection, string jobId, string executionId)
    {
        using var transaction = connection.CreateWriteTransaction();
        transaction.RemoveFromSet(ActiveSetKey, ActiveMember(jobId, executionId));
        transaction.Commit();
    }

    /// <summary>All active-contract tuples — the stall detector's scan surface.</summary>
    public static IReadOnlyCollection<string> ReadActiveMembers(IStorageConnection connection)
        => connection.GetAllItemsFromSet(ActiveSetKey);

    /// <summary>
    /// Active-contract count for the stalled endpoint's payload — lets alerting distinguish "no stalls
    /// among N contracted executions" from "nothing is contracted at all" (review F5's honest-zero rule).
    /// </summary>
    public static long CountActive(IStorageConnection connection)
        => connection is JobStorageConnection extended
            ? extended.GetSetCount(ActiveSetKey)
            : connection.GetAllItemsFromSet(ActiveSetKey).Count;

    // ---- stall flag (detection PR) ----

    /// <summary>Writes (or, with acknowledgment fields set, updates) an execution's stall marker.</summary>
    public static void WriteStall(IStorageConnection connection, string jobId, StallMarker marker)
        => connection.SetJobParameter(jobId, StallParameterName(marker.ExecutionId), JsonSerializer.Serialize(marker, JsonOptions));

    /// <summary>Null when absent, cleared, or unparsable — same tolerance contract as <see cref="ReadBeat"/>.</summary>
    public static StallMarker? ReadStall(IStorageConnection connection, string jobId, string executionId)
    {
        var raw = connection.GetJobParameter(jobId, StallParameterName(executionId));
        if (string.IsNullOrEmpty(raw)) return null;

        try
        {
            return JsonSerializer.Deserialize<StallMarker>(raw, JsonOptions);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>The current-execution pointer itself — what stall acknowledgment scopes to (review F8).</summary>
    public static string? ReadCurrentExecutionId(IStorageConnection connection, string jobId)
    {
        var executionId = connection.GetJobParameter(jobId, CurrentPointerParameterName);
        return string.IsNullOrEmpty(executionId) ? null : executionId;
    }

    /// <summary>
    /// Adds the tuple to the stalled index. Idempotent (set semantics) — also the repair path when a
    /// prior flag crashed between its marker write and this index write.
    /// </summary>
    public static void AddStalledMember(IStorageConnection connection, string jobId, string executionId)
    {
        using var transaction = connection.CreateWriteTransaction();
        transaction.AddToSet(StalledSetKey, ActiveMember(jobId, executionId));
        transaction.Commit();
    }

    /// <summary>
    /// Recovery/supersession cleanup: clears the execution's stall marker and un-indexes its tuple.
    /// Marker first — the inconsistent crash window then holds an indexed tuple with no marker, which
    /// readers skip and the detector's next pass removes, rather than a marker with no index entry that
    /// nothing would ever retire.
    /// </summary>
    public static void ClearStall(IStorageConnection connection, string jobId, string executionId)
    {
        connection.SetJobParameter(jobId, StallParameterName(executionId), "");
        using var transaction = connection.CreateWriteTransaction();
        transaction.RemoveFromSet(StalledSetKey, ActiveMember(jobId, executionId));
        transaction.Commit();
    }

    /// <summary>Removes a tuple from the stalled index only — the detector's self-heal for an index entry whose marker is gone.</summary>
    public static void RemoveStalledMember(IStorageConnection connection, string jobId, string executionId)
    {
        using var transaction = connection.CreateWriteTransaction();
        transaction.RemoveFromSet(StalledSetKey, ActiveMember(jobId, executionId));
        transaction.Commit();
    }

    /// <summary>All flagged tuples — the stalled endpoint's read surface and the dashboard metric's count source.</summary>
    public static IReadOnlyCollection<string> ReadStalledMembers(IStorageConnection connection)
        => connection.GetAllItemsFromSet(StalledSetKey);

    /// <summary>Stalled-tuple count — the built-in dashboard's one liveness integration (a metric tile).</summary>
    public static long CountStalled(IStorageConnection connection)
        => connection is JobStorageConnection extended
            ? extended.GetSetCount(StalledSetKey)
            : connection.GetAllItemsFromSet(StalledSetKey).Count;

    // ---- stall-retry workflow (retry PR, liveness plan §5) ----

    /// <summary>Writes (or advances the phase of) an execution's stall-retry workflow record.</summary>
    public static void WriteStallAttempt(IStorageConnection connection, string jobId, StallAttemptRecord attempt)
        => connection.SetJobParameter(jobId, StallAttemptParameterName(attempt.ExecutionId), JsonSerializer.Serialize(attempt, JsonOptions));

    /// <summary>Null when absent, cleared, or unparsable — same tolerance contract as <see cref="ReadBeat"/>.</summary>
    public static StallAttemptRecord? ReadStallAttempt(IStorageConnection connection, string jobId, string executionId)
    {
        var raw = connection.GetJobParameter(jobId, StallAttemptParameterName(executionId));
        if (string.IsNullOrEmpty(raw)) return null;

        try
        {
            return JsonSerializer.Deserialize<StallAttemptRecord>(raw, JsonOptions);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>
    /// Retires a prepared workflow record whose cancel transition lost (liveness plan §5 Rule 2 step 4)
    /// — empty-string overwrite, the same "reads back as absent" convention as
    /// <see cref="CancellationRequestStore.Clear"/>. Terminal phases are never cleared, only advanced —
    /// they are the workflow's durable record.
    /// </summary>
    public static void ClearStallAttempt(IStorageConnection connection, string jobId, string executionId)
        => connection.SetJobParameter(jobId, StallAttemptParameterName(executionId), "");

    /// <summary>
    /// Stall retries already spent on this background job. Job-scoped, unlike everything else here,
    /// because each retry runs as a fresh execution id — the budget must survive the identity change.
    /// The counterpart of Hangfire's own <c>RetryCount</c>, on the mutually exclusive path (§5 Rule 5).
    /// </summary>
    public static int ReadStallRetryCount(IStorageConnection connection, string jobId)
    {
        var raw = connection.GetJobParameter(jobId, StallRetryCountParameterName);
        return int.TryParse(raw, out var count) && count >= 0 ? count : 0;
    }

    public static void WriteStallRetryCount(IStorageConnection connection, string jobId, int count)
        => connection.SetJobParameter(jobId, StallRetryCountParameterName, count.ToString());

    /// <summary>
    /// Indexes a tuple whose cancel is being committed — from here on the workflow's driver is this set,
    /// not the active index (review F1: the self-heal rule removes a Deleted job from <c>active</c> on
    /// the next pass, so pending entries must survive independently, across detector and server restarts).
    /// </summary>
    public static void AddRetryPendingMember(IStorageConnection connection, string jobId, string executionId)
    {
        using var transaction = connection.CreateWriteTransaction();
        transaction.AddToSet(RetryPendingSetKey, ActiveMember(jobId, executionId));
        transaction.Commit();
    }

    /// <summary>Removed only on recovery, successful requeue, exhaustion, supersession, or confirmed terminal cleanup (§5 Rule 3).</summary>
    public static void RemoveRetryPendingMember(IStorageConnection connection, string jobId, string executionId)
    {
        using var transaction = connection.CreateWriteTransaction();
        transaction.RemoveFromSet(RetryPendingSetKey, ActiveMember(jobId, executionId));
        transaction.Commit();
    }

    /// <summary>All tuples with an in-flight post-cancel workflow — read by every detector pass alongside the active index (review F1).</summary>
    public static IReadOnlyCollection<string> ReadRetryPendingMembers(IStorageConnection connection)
        => connection.GetAllItemsFromSet(RetryPendingSetKey);

    // ---- detector health leases (reviews F5/C2) ----

    /// <summary>
    /// Renews this server's lease after a successful scan pass. The hash write and index add are core
    /// interface members; the storage-side expiry (hygiene for a server that never comes back) is the
    /// extended API and degrades to no-op — freshness is judged from <see cref="DetectorLease.LastScanAt"/>
    /// at read time either way, so a dead detector reads as stale regardless.
    /// </summary>
    public static void RenewDetectorLease(IStorageConnection connection, DetectorLease lease)
    {
        connection.SetRangeInHash(DetectorLeaseKey(lease.ServerId), new Dictionary<string, string>
        {
            ["v"] = lease.V.ToString(),
            ["serverId"] = lease.ServerId,
            ["lastScanAt"] = lease.LastScanAt.ToString("O"),
            ["scanIntervalSeconds"] = lease.ScanIntervalSeconds.ToString(),
        });

        using var transaction = connection.CreateWriteTransaction();
        transaction.AddToSet(DetectorIndexSetKey, lease.ServerId);
        if (transaction is JobStorageTransaction extended)
            extended.ExpireHash(DetectorLeaseKey(lease.ServerId), TimeSpan.FromSeconds(10 * Math.Max(lease.ScanIntervalSeconds, 30)));
        transaction.Commit();
    }

    /// <summary>
    /// All live detector leases. An index member whose hash expired or fails to parse is skipped here
    /// and physically removed by <see cref="PruneDetectorIndex"/> on the detector's own passes — reads
    /// stay pure.
    /// </summary>
    public static IReadOnlyList<DetectorLease> ReadDetectorLeases(IStorageConnection connection)
    {
        var leases = new List<DetectorLease>();
        foreach (var serverId in connection.GetAllItemsFromSet(DetectorIndexSetKey))
        {
            var lease = ReadDetectorLease(connection, serverId);
            if (lease is not null) leases.Add(lease);
        }

        return leases;
    }

    /// <summary>Removes index members whose lease hash has expired — the C2 self-heal, run by the detector each pass.</summary>
    public static void PruneDetectorIndex(IStorageConnection connection)
    {
        foreach (var serverId in connection.GetAllItemsFromSet(DetectorIndexSetKey))
        {
            if (ReadDetectorLease(connection, serverId) is not null) continue;

            using var transaction = connection.CreateWriteTransaction();
            transaction.RemoveFromSet(DetectorIndexSetKey, serverId);
            transaction.Commit();
        }
    }

    /// <summary>
    /// Freshness rule shared by the stalled endpoint and dashboards: a lease older than three scan
    /// intervals (floored at the 30s default, so an aggressively tuned-down demo interval doesn't flap)
    /// no longer proves a running detector.
    /// </summary>
    public static bool IsDetectorLeaseFresh(DetectorLease lease, DateTime utcNow)
        => utcNow - lease.LastScanAt <= TimeSpan.FromSeconds(3 * Math.Max(lease.ScanIntervalSeconds, 30));

    private static DetectorLease? ReadDetectorLease(IStorageConnection connection, string serverId)
    {
        var hash = connection.GetAllEntriesFromHash(DetectorLeaseKey(serverId));
        if (hash is null) return null;

        if (!hash.TryGetValue("v", out var rawVersion) || !int.TryParse(rawVersion, out var version)) return null;
        if (!hash.TryGetValue("lastScanAt", out var rawLastScanAt)
            || !DateTime.TryParse(rawLastScanAt, null, System.Globalization.DateTimeStyles.RoundtripKind, out var lastScanAt))
        {
            return null;
        }

        var scanIntervalSeconds = hash.TryGetValue("scanIntervalSeconds", out var rawInterval) && int.TryParse(rawInterval, out var interval)
            ? interval
            : 30;
        return new DetectorLease(version, serverId, lastScanAt, scanIntervalSeconds);
    }
}
