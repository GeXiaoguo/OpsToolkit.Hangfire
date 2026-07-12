using Hangfire.Common;

namespace Hangfire.OpsToolkit.JobControl;

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
    /// at equal order) takes its distributed lock. Idempotent: <c>GlobalJobFilters</c> is
    /// process-global, so any prior instance is replaced rather than stacked — safe when the host is
    /// built more than once in a process (e.g. integration tests).
    /// </summary>
    public static IGlobalConfiguration UseJobControl(this IGlobalConfiguration configuration)
    {
        var existingFilter = GlobalJobFilters.Filters
            .Select(filter => filter.Instance)
            .OfType<DisabledRecurringJobFilter>()
            .FirstOrDefault();
        if (existingFilter != null)
            GlobalJobFilters.Filters.Remove(existingFilter);
        GlobalJobFilters.Filters.Add(new DisabledRecurringJobFilter(), order: -1);
        return configuration;
    }
}
