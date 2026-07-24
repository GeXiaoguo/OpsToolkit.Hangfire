using Hangfire.Server;
using Microsoft.Extensions.DependencyInjection;

namespace OpsToolkit.Hangfire.JobControl;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <see cref="StallDetector"/> on the Hangfire server: a thin
    /// <c>AddSingleton&lt;IBackgroundProcess&gt;</c> — <c>Hangfire.AspNetCore</c> hands every
    /// DI-registered <see cref="IBackgroundProcess"/> to the server it starts, so registered ⇒ running,
    /// flag-only, no separate enable flag. Safe with zero configuration: only contracted executions are
    /// ever scanned (an empty index read per <see cref="LivenessOptions.ScanInterval"/> otherwise).
    /// Self-hosted servers (plain <c>new BackgroundJobServer(...)</c>) construct a
    /// <see cref="StallDetector"/> themselves and pass it via <c>additionalProcesses</c> instead.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <param name="options">
    /// The same options record the host passes to <c>UseJobControl</c>/<c>MapJobControl</c> — the
    /// detector reads <see cref="JobControlOptions.Liveness"/> and <see cref="JobControlOptions.AuditMaxEntries"/>.
    /// </param>
    public static IServiceCollection AddJobControlStallDetector(this IServiceCollection services, JobControlOptions? options = null)
        => services.AddSingleton<IBackgroundProcess>(new StallDetector(options));
}
