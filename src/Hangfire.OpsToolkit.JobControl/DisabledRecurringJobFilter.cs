using Hangfire.Common;
using Hangfire.Server;

namespace Hangfire.OpsToolkit.JobControl;

/// <summary>
/// Skips execution of a recurring job whose disabled flag is set (see
/// <see cref="RecurringJobDisableStore"/>). Cancelling in <c>OnPerforming</c> makes Hangfire record a
/// <see cref="Hangfire.States.DeletedState"/> with no exception — landing in the Deleted tab without
/// paging (a host's own failure-alert filter can typically distinguish this from a real failure by
/// checking for <c>ExceptionInfo</c>, which a cancel never has).
///
/// The dashboard's own "Trigger now" also carries <c>RecurringJobId</c>, so it is skipped too for a
/// disabled job — the safe default. A manual-invoke feature (host-specific) would instead use an ad-hoc
/// <c>BackgroundJob.Create</c> with no <c>RecurringJobId</c>, so it deliberately bypasses this filter —
/// the intended force-run escape hatch.
///
/// Registered via <see cref="GlobalConfigurationExtensions.UseJobControl"/>, which pins the filter
/// order below other execution-gating filters (e.g. <c>[DisableConcurrentExecution]</c>) so the cancel
/// happens before any distributed lock is taken.
/// </summary>
public sealed class DisabledRecurringJobFilter : IServerFilter
{
    public void OnPerforming(PerformingContext context)
    {
        // Snapshot read (allowStale: true) — no extra storage round-trip; the parameter is stamped once
        // at creation and never changes.
        var recurringJobId = context.GetJobParameter<string>("RecurringJobId", allowStale: true);
        if (string.IsNullOrEmpty(recurringJobId)) return;

        if (RecurringJobDisableStore.IsDisabled(context.Connection, recurringJobId))
            context.Canceled = true;
    }

    public void OnPerformed(PerformedContext context) { }
}
