using System.Text.Json;
using Hangfire;
using Hangfire.Storage;

namespace OpsToolkit.Hangfire.JobControl;

/// <summary>
/// Persists <see cref="AuditEntry"/> lines in a Hangfire storage <b>list</b> under
/// <c>jobcontrol:audit</c> — the same "state lives in Hangfire's own storage" trick
/// <see cref="RecurringJobDisableStore"/> uses, applied to an append-only log instead of a hash field.
/// <c>InsertToList</c> is a core <see cref="IWriteOnlyTransaction"/> member every storage implements;
/// list reads are the "extended API" on <see cref="JobStorageConnection"/>, which Hangfire.PostgreSql
/// and Hangfire.SqlServer both provide. No table, no migration, no host storage decision.
/// </summary>
/// <remarks>
/// Retention deliberately does <b>not</b> use <c>TrimList</c>, despite it looking like the obvious
/// primitive. Verified empirically (a probe against real Hangfire.PostgreSql 1.20.13, not just source
/// reading): Hangfire.SqlServer's <c>TrimList(key, 0, n-1)</c> keeps the <i>newest</i> n rows (it numbers
/// <c>order by Id desc</c> — see SqlServerWriteOnlyTransaction.cs:386), but Hangfire.PostgreSql's
/// <c>TrimList</c> keeps the <i>oldest</i> n instead — the opposite direction, even though both
/// providers' <c>GetRangeFromList</c>/<c>GetAllItemsFromList</c> agree (newest-first). Calling
/// <c>TrimList(key, 0, maxEntries-1)</c> after every insert therefore looked correct in isolation but
/// froze the Postgres-backed list at its first <c>maxEntries</c> entries forever — every entry after
/// that point was inserted and then immediately evicted by the very next trim, silently. Instead,
/// eviction here reads the list (the one operation both providers agree on) to find precisely the
/// entries that must go, then removes them by value — no assumption about which direction any given
/// storage's <c>TrimList</c> counts from.
/// </remarks>
public static class AuditStore
{
    private const string ListKey = "jobcontrol:audit";

    // How far past the cap a single Append tolerates finding before giving up on evicting the rest in
    // this call — self-healing over a few more calls, not a hard limit. Append always leaves the list
    // at-or-under the cap, so a real caller is never more than 1 over; this is slack for the unusual
    // case (concurrent writers, or maxEntries lowered between calls).
    private const int EvictionSlack = 32;

    /// <summary>
    /// Caller-owned transaction — lets disable/enable commit the flag write and the audit entry
    /// atomically, so the action and its record can never diverge. <paramref name="connection"/> must be
    /// the same connection <paramref name="transaction"/> was created from (it's read from, not written
    /// to, to compute what the cap requires evicting). Retention is a count cap, not a TTL: an audit log
    /// should not silently expire.
    /// </summary>
    public static void Append(IStorageConnection connection, IWriteOnlyTransaction transaction, AuditEntry entry, int maxEntries)
    {
        foreach (var stale in findEvictable(connection, maxEntries))
            transaction.RemoveFromList(ListKey, stale);
        transaction.InsertToList(ListKey, AuditEntry.Serialize(entry));
    }

    /// <summary>Convenience for actions with no transaction of their own (trigger / delete / not-found outcomes).</summary>
    public static void Append(JobStorage storage, AuditEntry entry, int maxEntries)
    {
        using var connection = storage.GetConnection();
        using var transaction = connection.CreateWriteTransaction();
        Append(connection, transaction, entry, maxEntries);
        transaction.Commit();
    }

    // The entries at position >= maxEntries-1 today land at position >= maxEntries once the new entry
    // is inserted — precisely the ones that must go to keep the post-insert list at the cap.
    // Storage that doesn't expose the extended read API degrades to "no eviction" (list grows
    // unbounded there) rather than failing the write — same policy as Read's degrade-to-501: capture
    // (a core-interface write) always works, only the read-dependent behavior (history view, and here,
    // retention) can't function without it.
    private static IReadOnlyList<string> findEvictable(IStorageConnection connection, int maxEntries)
    {
        if (connection is not JobStorageConnection listReads) return Array.Empty<string>();

        var start = Math.Max(0, maxEntries - 1);
        try
        {
            return listReads.GetRangeFromList(ListKey, start, start + EvictionSlack);
        }
        catch (NotSupportedException)
        {
            return Array.Empty<string>();
        }
    }

    /// <summary>
    /// Reads back entries, newest first. Without <paramref name="jobId"/>, this is a bounded
    /// <c>GetRangeFromList</c> over the top <paramref name="limit"/> rows. With it, the filter is a
    /// scan of the whole (count-capped) list — cheap because the list is human-scale by design: it
    /// holds operator actions and, later, real definition diffs only, not machine-volume data.
    /// </summary>
    /// <exception cref="NotSupportedException">
    /// The storage's connection doesn't expose the extended list-read API. Capture still works (list
    /// writes are a core interface member); only read-back degrades.
    /// </exception>
    public static IReadOnlyList<AuditEntry> Read(JobStorage storage, int limit, string? jobId = null)
    {
        using var connection = storage.GetConnection();
        if (connection is not JobStorageConnection listReads)
        {
            throw new NotSupportedException(
                "Audit read-back needs a storage exposing the extended list API (Hangfire.PostgreSql and Hangfire.SqlServer both do).");
        }

        // Ordering — newest at index 0. Confirmed for both GetRangeFromList and GetAllItemsFromList on
        // Hangfire.SqlServer (source: order by Id desc) and empirically on real Hangfire.PostgreSql
        // (see AuditStore's class remarks) — unlike TrimList, the two providers agree here.
        var raw = jobId is null
            ? listReads.GetRangeFromList(ListKey, 0, limit - 1)
            : listReads.GetAllItemsFromList(ListKey);

        var entries = raw.Select(AuditEntry.TryDeserialize).Where(entry => entry is not null).Select(entry => entry!);

        return jobId is null
            ? entries.ToList()
            : entries.Where(entry => matchesJobId(entry, jobId)).Take(limit).ToList();
    }

    // §5: a jobId query matches the entry's own target id, or either direction of the recurring-job
    // <-> background-job correlation stamped in Detail (TryGetRecurringJobId below; the trigger
    // endpoint's own Detail["BackgroundJobId"] seed is the other direction) — so a recurring job's
    // drawer finds run-level interventions (cancel/requeue/delete-run/cancel-ack/abort-observed)
    // against its executions with no UI changes, and a background job id finds both the human trigger
    // that created it and everything done to it since.
    private static bool matchesJobId(AuditEntry entry, string jobId)
    {
        if (entry.JobId == jobId) return true;
        if (entry.Detail is null) return false;
        return (entry.Detail.TryGetValue("RecurringJobId", out var recurringJobId) && recurringJobId == jobId)
            || (entry.Detail.TryGetValue("BackgroundJobId", out var backgroundJobId) && backgroundJobId == jobId);
    }

    /// <summary>
    /// The recurring-job id a background job was created from, if any — Hangfire's own
    /// <c>RecurringJobId</c> job parameter (stamped by <c>RecurringJobExtensions.TriggerRecurringJob</c>,
    /// not this library), falling back to this library's own manual-invoke stamp
    /// (<see cref="JobControlEndpoints.ManualInvokeOfParameterName"/>) so a run created by the invoke
    /// endpoint correlates to its recurring definition the same way a scheduled run does. Read back so
    /// a run-level audit entry (cancel/requeue/delete-run/cancel-ack/
    /// abort-observed) can carry <c>Detail["RecurringJobId"]</c> — §5's recurring correlation, the
    /// read-side counterpart of the trigger endpoint's own <c>Detail["BackgroundJobId"]</c> seed.
    /// </summary>
    /// <remarks>
    /// Job parameters set via <c>CreateContext.Parameters</c> (which is how Hangfire itself stamps this
    /// one — <c>RecurringJobExtensions.TriggerRecurringJob</c>) go through
    /// <c>SerializationHelper.Serialize(value, SerializationOption.User)</c> before landing in storage —
    /// verified empirically (a real triggered job's raw parameter reads back as the JSON string literal
    /// <c>"heartbeat-every-minute"</c>, quotes included), unlike <see cref="CancellationRequestStore"/>'s
    /// own marker, which this library writes directly via the core connection API with no such wrapping.
    /// A bare-string read here would never equal a caller's un-quoted recurring job id, silently breaking
    /// the correlation match in <see cref="matchesJobId"/>.
    /// </remarks>
    public static string? TryGetRecurringJobId(IStorageConnection connection, string jobId)
    {
        var raw = connection.GetJobParameter(jobId, "RecurringJobId");
        if (string.IsNullOrEmpty(raw))
            raw = connection.GetJobParameter(jobId, JobControlEndpoints.ManualInvokeOfParameterName);
        if (string.IsNullOrEmpty(raw)) return null;

        try
        {
            return JsonSerializer.Deserialize<string>(raw);
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
