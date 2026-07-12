using Hangfire;
using Hangfire.Storage;

namespace Hangfire.OpsToolkit.JobControl;

/// <summary>
/// Reads/writes a durable "disabled" flag on Hangfire's own <c>recurring-job:{id}</c> hash, as an
/// unknown custom field. <c>RecurringJob.AddOrUpdate</c> diffs and rewrites only its own known fields
/// (Cron, Job, TimeZoneId, ...) via <c>SetRangeInHash</c> — a merge, never a whole-hash rewrite — so
/// this flag survives every app-restart re-registration for free. Only <c>RemoveIfExists</c> clears it,
/// which is correct: delete the job, the flag goes with it.
///
/// Field names carry a dotted <c>JobControl.</c> prefix so they can never collide with Hangfire's own
/// hash fields (Cron, Job, TimeZoneId, ...), current or future.
/// </summary>
public static class RecurringJobDisableStore
{
    private const string DisabledField = "JobControl.Disabled";
    private const string DisabledByField = "JobControl.DisabledBy";
    private const string DisabledAtField = "JobControl.DisabledAt";
    private const string DisabledReasonField = "JobControl.DisabledReason";

    private static string Hash(string jobId) => $"recurring-job:{jobId}";

    public static bool IsDisabled(IStorageConnection connection, string jobId)
    {
        var hash = connection.GetAllEntriesFromHash(Hash(jobId));
        return hash != null && hash.TryGetValue(DisabledField, out var value) && value == "true";
    }

    public static RecurringJobDisableStatus? GetStatus(IStorageConnection connection, string jobId)
    {
        var hash = connection.GetAllEntriesFromHash(Hash(jobId));
        if (hash == null) return null;

        return new RecurringJobDisableStatus(
            Disabled: hash.TryGetValue(DisabledField, out var v) && v == "true",
            By: hash.GetValueOrDefault(DisabledByField),
            At: hash.GetValueOrDefault(DisabledAtField),
            Reason: hash.GetValueOrDefault(DisabledReasonField));
    }

    /// <summary>
    /// Writes (or clears) the flag. Returns false without writing when <paramref name="jobId"/> doesn't
    /// exist — writing anyway would mint an orphan hash no dashboard list page shows, while the caller
    /// reports success.
    /// </summary>
    public static bool SetDisabled(JobStorage storage, string jobId, bool disabled, string actor, string? reason, DateTime utcNow)
        => SetDisabled(storage, jobId, disabled, actor, reason, utcNow, auditEntry: null, auditMaxEntries: 0);

    /// <summary>
    /// Same as the five-argument overload, but appends <paramref name="auditEntry"/> in the <b>same</b>
    /// transaction as the flag write when <paramref name="jobId"/> exists — the action and its audit
    /// record commit atomically and can never diverge. The not-found outcome is <i>not</i> audited
    /// here: this method returns without opening a transaction, so the caller (which already knows the
    /// job wasn't found) records that entry itself via <see cref="AuditStore.Append(JobStorage,AuditEntry,int)"/>.
    /// </summary>
    public static bool SetDisabled(
        JobStorage storage, string jobId, bool disabled, string actor, string? reason, DateTime utcNow,
        AuditEntry? auditEntry, int auditMaxEntries)
    {
        using var connection = storage.GetConnection();
        var existing = connection.GetAllEntriesFromHash(Hash(jobId));
        if (existing == null || !existing.ContainsKey("Job"))
            return false;

        using var transaction = connection.CreateWriteTransaction();
        transaction.SetRangeInHash(Hash(jobId), new Dictionary<string, string>
        {
            [DisabledField] = disabled ? "true" : "false",
            [DisabledByField] = actor,
            [DisabledAtField] = utcNow.ToString("O"),
            [DisabledReasonField] = reason ?? "",
        });
        if (auditEntry is not null)
            AuditStore.Append(connection, transaction, auditEntry, auditMaxEntries);
        transaction.Commit();
        return true;
    }
}

/// <summary>
/// Describes the most recent disable/enable toggle in either direction — fields are named for the
/// flag, not the action, since enabling rewrites them too.
/// </summary>
public sealed record RecurringJobDisableStatus(bool Disabled, string? By, string? At, string? Reason);
