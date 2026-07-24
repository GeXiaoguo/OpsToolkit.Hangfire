using Hangfire;
using Hangfire.Storage;

namespace OpsToolkit.Hangfire.JobControl;

/// <summary>
/// Persists operator overrides (schedule and parameter values) in Hangfire's own storage — a
/// toolkit-owned hash per job (<c>jobcontrol:runtime:{jobId}</c>) plus an index set
/// (<c>jobcontrol:runtime</c>) so the rows are enumerable, all through core
/// <see cref="IStorageConnection"/>/<see cref="IWriteOnlyTransaction"/>
/// members. No table, no migration, no host storage decision — the same trick
/// <see cref="RecurringJobDisableStore"/> and <see cref="AuditStore"/> use.
///
/// Deliberately <b>not</b> fields on the <c>recurring-job:{id}</c> hash (where the disable flag
/// lives): that hash dies with <c>RemoveIfExists</c>, while an override must survive its job being
/// removed and re-added — the rollback path. A row is therefore never hard-deleted by reconciliation;
/// it is <i>soft-invalidated</i> (marked, kept) when it no longer matches the deployed code, and
/// re-validated when a rollback or rename-back makes it match again. Only an operator reset removes it.
/// </summary>
public static class RecurringJobRuntimeStore
{
    private const string IndexSetKey = "jobcontrol:runtime";

    private const string CronField = "Cron";
    private const string ArgsField = "Args";
    private const string UpdatedByField = "UpdatedBy";
    private const string UpdatedAtField = "UpdatedAt";
    private const string ReasonField = "Reason";
    private const string InvalidatedAtField = "InvalidatedAt";
    private const string InvalidatedReasonField = "InvalidatedReason";

    /// <summary>The job this row belongs to no longer appears in the code-declared definitions.</summary>
    public const string InvalidatedJobRemoved = "job-removed";

    /// <summary>The stored cron no longer parses under the deployed Hangfire version.</summary>
    public const string InvalidatedBadCron = "bad-cron";

    /// <summary>The stored parameter values no longer bind to the deployed method signature.</summary>
    public const string InvalidatedBadArgs = "bad-args";

    private static string HashKey(string jobId) => $"jobcontrol:runtime:{jobId}";

    public static IReadOnlyList<RecurringJobRuntimeRow> LoadAll(IStorageConnection connection)
    {
        var jobIds = connection.GetAllItemsFromSet(IndexSetKey);
        var rows = new List<RecurringJobRuntimeRow>(jobIds.Count);
        foreach (var jobId in jobIds)
        {
            var row = Load(connection, jobId);
            if (row is not null) rows.Add(row);
        }
        return rows;
    }

    public static RecurringJobRuntimeRow? Load(IStorageConnection connection, string jobId)
    {
        var hash = connection.GetAllEntriesFromHash(HashKey(jobId));
        if (hash == null || hash.Count == 0) return null;

        // Cleared fields are written as "" (SetRangeInHash merges; there is no per-field delete in
        // the core transaction API) — normalize to null so consumers get one representation of "no
        // override" instead of checking both.
        return new RecurringJobRuntimeRow(
            jobId,
            nullIfEmpty(hash.GetValueOrDefault(CronField)),
            nullIfEmpty(hash.GetValueOrDefault(ArgsField)),
            hash.GetValueOrDefault(UpdatedByField) ?? "unknown",
            hash.GetValueOrDefault(UpdatedAtField) ?? "",
            hash.GetValueOrDefault(ReasonField),
            hash.GetValueOrDefault(InvalidatedAtField),
            hash.GetValueOrDefault(InvalidatedReasonField));
    }

    private static string? nullIfEmpty(string? value) => string.IsNullOrEmpty(value) ? null : value;

    /// <summary>
    /// Writes (or replaces) the cron override. Clears any invalidation marks — an operator writing a
    /// fresh value against the current code is the strongest possible re-validation. When
    /// <paramref name="auditEntry"/> is provided it commits in the same transaction, so the action and
    /// its record can never diverge (the <see cref="RecurringJobDisableStore"/> pattern).
    /// </summary>
    public static void SetCronOverride(
        JobStorage storage, string jobId, string cron, string actor, string? reason, DateTime utcNow,
        AuditEntry? auditEntry = null, int auditMaxEntries = 0)
        => setOverrideField(storage, jobId, CronField, cron, actor, reason, utcNow, auditEntry, auditMaxEntries);

    /// <summary>
    /// Writes (or replaces) the parameter-values override — a JSON object of parameter name → value,
    /// already validated by the caller (<see cref="JobArgs.Bind"/>, project-first like cron). Same
    /// invalidation-clearing and same-transaction-audit contract as <see cref="SetCronOverride"/>.
    /// </summary>
    public static void SetArgsOverride(
        JobStorage storage, string jobId, string argsJson, string actor, string? reason, DateTime utcNow,
        AuditEntry? auditEntry = null, int auditMaxEntries = 0)
        => setOverrideField(storage, jobId, ArgsField, argsJson, actor, reason, utcNow, auditEntry, auditMaxEntries);

    private static void setOverrideField(
        JobStorage storage, string jobId, string field, string value, string actor, string? reason, DateTime utcNow,
        AuditEntry? auditEntry, int auditMaxEntries)
    {
        using var connection = storage.GetConnection();
        using var transaction = connection.CreateWriteTransaction();
        transaction.SetRangeInHash(HashKey(jobId), new Dictionary<string, string>
        {
            [field] = value,
            [UpdatedByField] = actor,
            [UpdatedAtField] = utcNow.ToString("O"),
            [ReasonField] = reason ?? "",
            [InvalidatedAtField] = "",
            [InvalidatedReasonField] = "",
        });
        transaction.AddToSet(IndexSetKey, jobId);
        if (auditEntry is not null)
            AuditStore.Append(connection, transaction, auditEntry, auditMaxEntries);
        transaction.Commit();
    }

    /// <summary>
    /// Resets the cron override — back to the code-default schedule. Returns false (without writing
    /// an audit entry) when no cron override exists, so the caller can 404 instead of recording a
    /// false success. A parameter override on the same row survives; the row itself is removed only
    /// when nothing remains on it, so reconciliation never re-inspects empty rows.
    /// </summary>
    public static bool ClearCronOverride(
        JobStorage storage, string jobId, string actor, string? reason, DateTime utcNow,
        AuditEntry? auditEntry = null, int auditMaxEntries = 0)
        => clearOverrideField(storage, jobId, CronField, row => row.Cron, row => row.ArgsJson is not null,
            actor, reason, utcNow, auditEntry, auditMaxEntries);

    /// <summary>
    /// Resets the parameter-values override — back to the code-default arguments. Same contract as
    /// <see cref="ClearCronOverride"/>, with a cron override on the same row surviving.
    /// </summary>
    public static bool ClearArgsOverride(
        JobStorage storage, string jobId, string actor, string? reason, DateTime utcNow,
        AuditEntry? auditEntry = null, int auditMaxEntries = 0)
        => clearOverrideField(storage, jobId, ArgsField, row => row.ArgsJson, row => row.Cron is not null,
            actor, reason, utcNow, auditEntry, auditMaxEntries);

    private static bool clearOverrideField(
        JobStorage storage, string jobId, string field,
        Func<RecurringJobRuntimeRow, string?> fieldValue, Func<RecurringJobRuntimeRow, bool> otherFieldRemains,
        string actor, string? reason, DateTime utcNow, AuditEntry? auditEntry, int auditMaxEntries)
    {
        using var connection = storage.GetConnection();
        var row = Load(connection, jobId);
        if (row is null || fieldValue(row) is null) return false;

        using var transaction = connection.CreateWriteTransaction();
        if (otherFieldRemains(row))
        {
            // Merge-clear: "" reads back as null (see Load). The metadata now describes the reset —
            // the most recent operator action on the row — and any dormancy mark is cleared so the
            // surviving override gets a fresh projection attempt.
            transaction.SetRangeInHash(HashKey(jobId), new Dictionary<string, string>
            {
                [field] = "",
                [UpdatedByField] = actor,
                [UpdatedAtField] = utcNow.ToString("O"),
                [ReasonField] = reason ?? "",
                [InvalidatedAtField] = "",
                [InvalidatedReasonField] = "",
            });
        }
        else
        {
            transaction.RemoveHash(HashKey(jobId));
            transaction.RemoveFromSet(IndexSetKey, jobId);
        }
        if (auditEntry is not null)
            AuditStore.Append(connection, transaction, auditEntry, auditMaxEntries);
        transaction.Commit();
        return true;
    }

    /// <summary>
    /// Removes the row entirely, both overrides at once — test cleanup and administrative escape
    /// hatch; the operator-facing resets are the per-field <see cref="ClearCronOverride"/>/
    /// <see cref="ClearArgsOverride"/>. Returns false (without writing an audit entry) when no row
    /// exists, so the caller can 404 instead of recording a false success.
    /// </summary>
    public static bool RemoveOverride(
        JobStorage storage, string jobId, AuditEntry? auditEntry = null, int auditMaxEntries = 0)
    {
        using var connection = storage.GetConnection();
        if (Load(connection, jobId) is null) return false;

        using var transaction = connection.CreateWriteTransaction();
        transaction.RemoveHash(HashKey(jobId));
        transaction.RemoveFromSet(IndexSetKey, jobId);
        if (auditEntry is not null)
            AuditStore.Append(connection, transaction, auditEntry, auditMaxEntries);
        transaction.Commit();
        return true;
    }

    /// <summary>
    /// Soft-invalidation: mark, never delete. The row stays put so a rollback (or a rename-back) that
    /// makes it match the code again can resume the override via <see cref="ClearInvalidated"/>.
    /// A merge write — the override value and its operator metadata are left intact.
    /// </summary>
    public static void MarkInvalidated(JobStorage storage, string jobId, string reason, DateTime utcNow)
    {
        using var connection = storage.GetConnection();
        using var transaction = connection.CreateWriteTransaction();
        transaction.SetRangeInHash(HashKey(jobId), new Dictionary<string, string>
        {
            [InvalidatedAtField] = utcNow.ToString("O"),
            [InvalidatedReasonField] = reason,
        });
        transaction.Commit();
    }

    public static void ClearInvalidated(JobStorage storage, string jobId)
    {
        using var connection = storage.GetConnection();
        using var transaction = connection.CreateWriteTransaction();
        transaction.SetRangeInHash(HashKey(jobId), new Dictionary<string, string>
        {
            [InvalidatedAtField] = "",
            [InvalidatedReasonField] = "",
        });
        transaction.Commit();
    }
}

/// <summary>
/// One stored override row: an optional cron override and an optional parameter-values override
/// (<see cref="ArgsJson"/>, a JSON object of parameter name → value), sharing one set of operator
/// metadata — whoever touched the row last. Timestamps are ISO-8601 strings, kept as stored (the same
/// presentation contract as <see cref="RecurringJobDisableStatus"/>). <see cref="InvalidatedAt"/>/
/// <see cref="InvalidatedReason"/> are null or empty when the row is live; a non-empty value means the
/// row is dormant and the code defaults are being projected instead — the row projects as a unit, so
/// one bad part never leaves the other half-applied.
/// </summary>
public sealed record RecurringJobRuntimeRow(
    string JobId,
    string? Cron,
    string? ArgsJson,
    string UpdatedBy,
    string UpdatedAt,
    string? Reason,
    string? InvalidatedAt,
    string? InvalidatedReason)
{
    public bool IsInvalidated => !string.IsNullOrEmpty(InvalidatedAt);
}
