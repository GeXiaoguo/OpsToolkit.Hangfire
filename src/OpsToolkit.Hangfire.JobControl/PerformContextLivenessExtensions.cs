using Hangfire.Logging;
using Hangfire.Server;

namespace OpsToolkit.Hangfire.JobControl;

/// <summary>
/// In-flight, per-execution beat state shared between <see cref="LivenessFilter.OnPerforming"/> (which
/// creates it) and <see cref="PerformContextLivenessExtensions.Beat"/> (which advances it) via
/// <c>PerformContext.Items</c>. Holding the record here — not re-reading storage per beat — is what
/// makes a beat one PK write, and holding the <b>execution id</b> here (immutable for the execution's
/// lifetime, review F3) is what makes every write execution-owned.
/// </summary>
internal sealed class LivenessBeatState
{
    public LivenessBeatState(BeatRecord record, TimeSpan minBeatInterval)
    {
        Record = record;
        LastPersistedAt = record.BeatAt;
        MinBeatInterval = minBeatInterval;
    }

    public object SyncRoot { get; } = new();
    public BeatRecord Record { get; set; }
    public DateTime LastPersistedAt { get; set; }

    /// <summary>
    /// The enrolling filter's configured write throttle (<see cref="LivenessOptions.MinBeatInterval"/>),
    /// snapshotted here so <c>Beat()</c> — a static extension with no options access — stays free of
    /// global state.
    /// </summary>
    public TimeSpan MinBeatInterval { get; }
}

public static class PerformContextLivenessExtensions
{
    /// <summary>
    /// <c>PerformContext.Items</c> key under which <see cref="LivenessFilter"/> retains the enrolled
    /// execution's id for the execution's lifetime (review F3). Public because it is part of the
    /// cancellation protocol's contract: <see cref="CancellationOutcomeFilter"/> fences execution-scoped
    /// acknowledgments on it, and hosts writing their own server filters (or tests building contexts by
    /// hand) need the same identity.
    /// </summary>
    public const string ExecutionIdItemKey = "JobControl.Liveness.ExecutionId";

    internal const string BeatStateItemKey = "JobControl.Liveness.BeatState";
    internal const int MaxMessageLength = 512;

    /// <summary>
    /// Records a liveness heartbeat — optionally carrying progress — for the current execution. Call it
    /// at meaningful progress points (per loop iteration is fine: writes are throttled to one per
    /// <see cref="LivenessOptions.MinBeatInterval"/>, calls in between are coalesced in memory). Never
    /// call it from a timer: a hung body that keeps "beating" by timer is exactly the blindspot this
    /// contract exists to close — silence is the signal.
    ///
    /// A no-op on a job without <see cref="HeartbeatAttribute"/> (calling it does not start a contract —
    /// opt-in stays explicit and local) and on an execution whose enrollment failed. Never throws: a
    /// storage blip while persisting a beat must not fail a six-hour job — it is logged, and the next
    /// accepted beat writes the up-to-date record anyway.
    /// </summary>
    /// <param name="context">The executing job's context (Hangfire injects it into a job-method parameter of this type).</param>
    /// <param name="percent">
    /// Progress percentage. Clamped to 0..100; NaN/infinity are ignored. Null keeps the previously
    /// reported value — a liveness-only <c>Beat()</c> never erases progress.
    /// </param>
    /// <param name="message">
    /// Short progress message (e.g. <c>"batch 3/7"</c>), truncated to <see cref="MaxMessageLength"/>
    /// characters. Null keeps the previously reported message.
    /// </param>
    public static void Beat(this PerformContext context, double? percent = null, string? message = null)
    {
        if (context is null) throw new ArgumentNullException(nameof(context));

        try
        {
            if (!context.Items.TryGetValue(BeatStateItemKey, out var raw) || raw is not LivenessBeatState state)
                return;

            var now = DateTime.UtcNow;
            lock (state.SyncRoot)
            {
                if (now - state.LastPersistedAt < state.MinBeatInterval) return;

                var record = state.Record with
                {
                    Seq = state.Record.Seq + 1,
                    BeatAt = now,
                    Percent = NormalizePercent(percent) ?? state.Record.Percent,
                    Message = NormalizeMessage(message) ?? state.Record.Message,
                };
                LivenessStore.WriteBeat(context.Connection, context.BackgroundJob.Id, record);
                state.Record = record;
                state.LastPersistedAt = now;
            }
        }
        catch (Exception ex)
        {
            LogProvider.GetLogger(typeof(PerformContextLivenessExtensions)).WarnException(
                $"Failed to persist a heartbeat for job {context.BackgroundJob.Id}; the job continues.", ex);
        }
    }

    private static double? NormalizePercent(double? percent)
    {
        if (percent is null || double.IsNaN(percent.Value) || double.IsInfinity(percent.Value)) return null;
        return Math.Clamp(percent.Value, 0d, 100d);
    }

    private static string? NormalizeMessage(string? message)
    {
        if (message is null) return null;
        return message.Length <= MaxMessageLength ? message : message[..MaxMessageLength];
    }
}
