using System.Text.Json;
using Hangfire.Server;
using Hangfire.Storage;

namespace OpsToolkit.Hangfire.JobControl;

/// <summary>
/// One recorded cancel request — see <see cref="CancellationRequestStore"/> for the storage contract.
/// <see cref="RequestId"/> and <see cref="ExecutionId"/> arrived with the linearized protocol (liveness
/// plan §5 Rule 2): the id pair correlates this <b>prepared</b> request with the
/// <see cref="CancelledState"/> transition that commits it and with the acknowledgment that settles it.
/// Both are null on a marker written by a pre-0.5 binary, whose protocol wrote the marker <i>after</i>
/// the transition — <see cref="CancellationOutcomeFilter"/> keeps a legacy path for those during a
/// rolling deploy.
/// </summary>
public sealed record CancelRequestMarker(
    int V, string By, DateTime At, string Reason, string? RequestId = null, string? ExecutionId = null)
{
    public const int CurrentVersion = 2;
}

/// <summary>
/// One settled acknowledgment of an execution-scoped cancel: the moment the cancelled body observably
/// stopped (or honestly didn't), recorded by <see cref="CancellationOutcomeFilter"/> under the exact
/// request/execution identity pair so the stall detector's requeue gate (liveness plan §5 Rule 4) can
/// read it durably. <see cref="Result"/> is the same classification the <c>cancel-ack</c> audit entry
/// carries: <c>aborted</c>, <c>faulted</c>, or <c>completed-anyway</c>.
/// </summary>
public sealed record CancelAckRecord(int V, string RequestId, string ExecutionId, string Result, DateTime At)
{
    public const int CurrentVersion = 1;

    public const string ResultAborted = "aborted";
    public const string ResultFaulted = "faulted";
    public const string ResultCompletedAnyway = "completed-anyway";
}

/// <summary>
/// Reads and writes the correlation markers used by the governed cancellation protocol — job parameters
/// on the target background job. Job parameters
/// are core-interface storage (mechanic #12): they live with the job and expire with it, so this needs
/// no cleanup process of its own. Deliberately bypasses <c>PerformContext.SetJobParameter&lt;T&gt;</c>/
/// <c>GetJobParameter&lt;T&gt;</c> (the generic convenience wrappers on <see cref="PerformContext"/>)
/// in favor of the core <see cref="IStorageConnection.SetJobParameter"/>/<see cref="IStorageConnection.GetJobParameter"/>
/// members directly — those wrappers additionally JSON-serialize whatever value is handed to them, which
/// would double-encode a value that is already our own JSON shape. Same reasoning as
/// <see cref="AuditEntry"/>'s bespoke (de)serialization.
///
/// The request marker (<c>JobControl.CancelRequested</c>) is one job-scoped cell — deliberately
/// last-writer-wins, like the liveness current-execution pointer: the latest governed cancel is what the
/// Deleted tab displays, and correctness never rides on the marker alone — an acknowledgment is valid
/// only against the identity the committed <see cref="CancelledState"/> data names. The acknowledgment
/// records are execution-scoped (review B3), one per request/execution pair.
/// </summary>
public static class CancellationRequestStore
{
    private const string ParameterName = "JobControl.CancelRequested";

    private static string AckParameterName(string executionId, string requestId)
        => $"JobControl.CancelAck:{executionId}:{requestId}";

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    /// <summary>
    /// Prepares the request marker. Only meaningful when the cancelled job is <c>Processing</c> (§2.1
    /// step 2) — a queued/scheduled cancel has no running body to acknowledge, so callers must not call
    /// this for those cases. Written <b>before</b> the state transition (liveness plan §5 Rule 2): the
    /// marker prepared alone proves nothing — the transition's own state data is the commit record — so
    /// a caller whose transition then loses must retire the marker via <see cref="ClearIfRequest"/>.
    /// </summary>
    public static void Write(
        IStorageConnection connection, string jobId, string by, DateTime at, string reason,
        string? requestId = null, string? executionId = null)
        => connection.SetJobParameter(jobId, ParameterName, JsonSerializer.Serialize(
            new CancelRequestMarker(CancelRequestMarker.CurrentVersion, by, at, reason, requestId, executionId), JsonOptions));

    /// <summary>Null when absent, cleared, or unparsable — a corrupt/foreign parameter value must not throw.</summary>
    public static CancelRequestMarker? Read(IStorageConnection connection, string jobId)
    {
        var raw = connection.GetJobParameter(jobId, ParameterName);
        if (string.IsNullOrEmpty(raw)) return null;

        try
        {
            return JsonSerializer.Deserialize<CancelRequestMarker>(raw, JsonOptions);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>
    /// Overwrites the marker with an empty string — the core <c>IStorageConnection</c> interface has no
    /// parameter-delete member, and an empty value reads back as absent (<see cref="Read"/>). Called on
    /// requeue so a cancelled-then-requeued job's next run doesn't inherit a stale marker and
    /// record a phantom <c>completed-anyway</c> ack.
    /// </summary>
    public static void Clear(IStorageConnection connection, string jobId)
        => connection.SetJobParameter(jobId, ParameterName, "");

    /// <summary>
    /// Retires a prepared request whose transition lost — but only when the marker still belongs to that
    /// request, so a losing canceller can't clear a concurrent winner's marker. (The single-cell overwrite
    /// race between two near-simultaneous cancels predates this protocol and stays display-deep: ack
    /// validity is matched against the committed state data, never the marker alone.)
    /// </summary>
    public static void ClearIfRequest(IStorageConnection connection, string jobId, string requestId)
    {
        if (string.Equals(Read(connection, jobId)?.RequestId, requestId, StringComparison.Ordinal))
            Clear(connection, jobId);
    }

    /// <summary>Records the settled acknowledgment for one request/execution pair. Idempotent by key.</summary>
    public static void WriteAck(IStorageConnection connection, string jobId, CancelAckRecord ack)
        => connection.SetJobParameter(jobId, AckParameterName(ack.ExecutionId, ack.RequestId),
            JsonSerializer.Serialize(ack, JsonOptions));

    /// <summary>Null when absent or unparsable — same tolerance contract as <see cref="Read"/>.</summary>
    public static CancelAckRecord? ReadAck(IStorageConnection connection, string jobId, string executionId, string requestId)
    {
        var raw = connection.GetJobParameter(jobId, AckParameterName(executionId, requestId));
        if (string.IsNullOrEmpty(raw)) return null;

        try
        {
            return JsonSerializer.Deserialize<CancelAckRecord>(raw, JsonOptions);
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
