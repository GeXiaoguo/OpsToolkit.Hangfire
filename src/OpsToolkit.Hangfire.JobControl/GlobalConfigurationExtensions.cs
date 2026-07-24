using Hangfire;
using Hangfire.Common;
using Hangfire.Dashboard;

namespace OpsToolkit.Hangfire.JobControl;

/// <summary>
/// Server plane of job control, following the Hangfire extension convention (cf. Hangfire.Console's
/// <c>UseConsole()</c>): chain onto the <c>IGlobalConfiguration</c> passed to <c>AddHangfire</c>. The
/// HTTP plane (API + UI) is mapped separately via <see cref="JobControlEndpoints.MapJobControl"/>.
/// </summary>
public static class GlobalConfigurationExtensions
{
    /// <summary>
    /// Registers <see cref="DisabledRecurringJobFilter"/> globally at order -1 — before a method-level
    /// <c>[DisableConcurrentExecution]</c> (default order -1; Global scope sorts ahead of Method scope
    /// at equal order) takes its distributed lock — plus <see cref="CancellationOutcomeFilter"/> and
    /// <see cref="LivenessFilter"/> at default order (after the disable filter, so a disable-skip never
    /// starts a liveness contract; relative order between the two doesn't matter).
    /// Idempotent: <c>GlobalJobFilters</c> is process-global, so any prior instance of each filter is
    /// replaced rather than stacked — safe when the host is built more than once in a process (e.g.
    /// integration tests).
    /// </summary>
    public static IGlobalConfiguration UseJobControl(this IGlobalConfiguration configuration, JobControlOptions? options = null)
    {
        var jobControlOptions = options ?? new JobControlOptions();

        replace(new DisabledRecurringJobFilter(), order: -1);
        replace(new CancellationOutcomeFilter(jobControlOptions.AuditMaxEntries), order: null);
        replace(new LivenessFilter(jobControlOptions.AuditMaxEntries, jobControlOptions.Liveness), order: null);

        // The built-in dashboard's one liveness integration: a stalled-count tile on its home page —
        // one set-count read per render through the stable UseDashboardMetric seam. Anything richer
        // natively would mean replacing dashboard pages wholesale (version-fragile); the Runs UI stays
        // the rich surface. Guarded like the filters: the home page's metric list is process-global and
        // append-only (and internal, so not inspectable), so a second UseJobControl (integration tests
        // build the host repeatedly in one process) must not stack tiles.
        if (Interlocked.CompareExchange(ref _stalledMetricRegistered, 1, 0) == 0)
            configuration.UseDashboardMetric(StalledJobsMetric);

        return configuration;

        static void replace<TFilter>(TFilter filter, int? order) where TFilter : class
        {
            var existing = GlobalJobFilters.Filters
                .Select(f => f.Instance)
                .OfType<TFilter>()
                .FirstOrDefault();
            if (existing != null)
                GlobalJobFilters.Filters.Remove(existing);

            if (order.HasValue) GlobalJobFilters.Filters.Add(filter, order.Value);
            else GlobalJobFilters.Filters.Add(filter);
        }
    }

    private static int _stalledMetricRegistered;

    private static readonly DashboardMetric StalledJobsMetric = new(
        "jobcontrol:liveness:stalled-count",
        "Stalled jobs",
        static page =>
        {
            try
            {
                using var connection = page.Storage.GetConnection();
                var stalled = LivenessStore.CountStalled(connection);
                return new Metric(stalled)
                {
                    Style = stalled > 0 ? MetricStyle.Warning : MetricStyle.Default,
                    Highlighted = stalled > 0,
                    Title = stalled > 0 ? "Executions flagged stalled — review them on the Job Runs page." : null,
                };
            }
            catch
            {
                // The tile must never take down the dashboard home page over a storage hiccup.
                return new Metric("—");
            }
        });
}
