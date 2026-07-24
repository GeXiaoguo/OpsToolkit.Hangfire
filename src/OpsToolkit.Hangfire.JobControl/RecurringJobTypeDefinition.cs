using Hangfire.Common;

namespace OpsToolkit.Hangfire.JobControl;

/// <summary>
/// A recurring job's code-declared identity: what to run and the schedule the code ships with. Held
/// in memory only — rebuilt from the registration expressions on every process start, never persisted —
/// which is what makes projection safe across method renames and signature changes: the stored job
/// payload is never trusted, the definition always reflects the currently deployed code.
///
/// <see cref="CronDefault"/> is the code default, not necessarily what is scheduled: an operator
/// override stored by <see cref="RecurringJobRuntimeStore"/> takes precedence until reset
/// (<c>effective = override ?? default</c>, applied by <see cref="RecurringJobRegistrar.Apply"/>).
/// </summary>
public sealed record RecurringJobTypeDefinition(
    string Id,
    Job Job,
    string CronDefault,
    TimeZoneInfo TimeZone);
