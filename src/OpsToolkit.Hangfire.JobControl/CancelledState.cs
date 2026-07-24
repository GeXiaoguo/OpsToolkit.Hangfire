using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;

namespace OpsToolkit.Hangfire.JobControl;

/// <summary>
/// The commit record of the governed cancellation protocol (liveness plan §5 Rule 2, review B2): a
/// state whose <see cref="Name"/> is the plain string <c>"Deleted"</c> — so every built-in handler,
/// statistic, and dashboard rendering keyed by state name is untouched — but whose
/// <see cref="SerializeData"/> additionally carries the cancel request's identity, committed
/// <b>atomically with the transition itself</b>. That linearizes the protocol on the state: an
/// acknowledgment is valid only when the finishing execution matches both the prepared request marker
/// and this committed state data, so a fast abort can never lose its ack (the pre-existing
/// <c>abort-observed</c> near-miss) and no ack is ever valid for a cancel whose transition lost.
/// Shared by the operator cancel endpoint and the stall detector's cancel — one cancellation protocol.
/// </summary>
/// <remarks>
/// Transactional job parameters would be the alternative commit channel, but they are feature-gated
/// (<c>JobStorageFeatures.Transaction.SetJobParameter</c>) and must not be assumed — state data, a core
/// capability, is the commit record.
/// </remarks>
public sealed class CancelledState : IState
{
    /// <summary>State-data key carrying the prepared cancel request's id.</summary>
    public const string RequestIdDataKey = "CancelRequestId";

    /// <summary>
    /// State-data key carrying the liveness execution id the cancel is scoped to — absent when the
    /// cancelled job had no enrolled liveness contract (identity matching then degrades to the request
    /// id alone; there is no execution identity to fence on).
    /// </summary>
    public const string ExecutionIdDataKey = "CancelExecutionId";

    public CancelledState(string requestId, string? executionId)
    {
        RequestId = requestId ?? throw new ArgumentNullException(nameof(requestId));
        ExecutionId = executionId;
        DeletedAt = DateTime.UtcNow;
    }

    public string RequestId { get; }

    public string? ExecutionId { get; }

    public DateTime DeletedAt { get; }

    public string? Reason { get; set; }

    /// <summary>Plain <c>"Deleted"</c> — built-in state handlers and statistics match by name.</summary>
    public string Name => DeletedState.StateName;

    /// <summary>Mirrors <see cref="DeletedState.IsFinal"/> — the job's expiration clock starts.</summary>
    public bool IsFinal => true;

    /// <summary>Mirrors <see cref="DeletedState.IgnoreJobLoadException"/> — a cancel must land even for a job whose type no longer loads.</summary>
    public bool IgnoreJobLoadException => true;

    /// <summary>
    /// Whether the job's <b>current</b> state is the committed cancel carrying
    /// <paramref name="requestId"/> — the one commit check of the protocol, shared by the stall
    /// detector's workflow driver and the requeue endpoints' Rule-4 guard so the two cannot drift
    /// (the same single-implementation rule as <see cref="JobRequeue"/>).
    /// </summary>
    internal static bool IsCommittedFor(IStorageConnection connection, string jobId, string requestId)
    {
        StateData? stateData;
        try
        {
            stateData = connection.GetStateData(jobId);
        }
        catch (FormatException)
        {
            return false; // the Hangfire.PostgreSql id-shape quirk RunEndpoints documents
        }

        return stateData?.Data is not null
            && DeletedState.StateName.Equals(stateData.Name, StringComparison.OrdinalIgnoreCase)
            && stateData.Data.TryGetValue(RequestIdDataKey, out var committed)
            && string.Equals(committed, requestId, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>DeletedAt</c> keeps the built-in dashboard's Deleted list rendering (it reads that key from
    /// the state data); the two cancel keys are this protocol's commit record.
    /// </summary>
    public Dictionary<string, string> SerializeData()
    {
        var data = new Dictionary<string, string>
        {
            ["DeletedAt"] = JobHelper.SerializeDateTime(DeletedAt),
            [RequestIdDataKey] = RequestId,
        };
        if (ExecutionId is not null) data[ExecutionIdDataKey] = ExecutionId;
        return data;
    }
}
