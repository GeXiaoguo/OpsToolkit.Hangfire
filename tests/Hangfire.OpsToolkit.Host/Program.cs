using Hangfire;
using Hangfire.Dashboard;
using Hangfire.OpsToolkit.JobControl;
using Hangfire.PostgreSql;

var builder = WebApplication.CreateBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("Hangfire")
    ?? "host=localhost;port=5434;database=hangfire_opstoolkit;username=postgres;password=postgres";

builder.Services.AddAuthentication();
builder.Services.AddAuthorization(options =>
{
    // Demo-only: anyone can view and manage. A real host wires these to its own identity/roles —
    // MapJobControl takes policy names precisely so this swap is the only thing that changes.
    options.AddPolicy(Policies.View, policy => policy.RequireAssertion(_ => true));
    options.AddPolicy(Policies.Manage, policy => policy.RequireAssertion(_ => true));
});

builder.Services.AddHangfire(config => config
    .SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
    .UseSimpleAssemblyNameTypeSerializer()
    .UseRecommendedSerializerSettings()
    .UsePostgreSqlStorage(options => options.UseNpgsqlConnection(connectionString))
    .UseJobControl());

builder.Services.AddHangfireServer();

var app = builder.Build();

app.UseAuthentication();
app.UseAuthorization();

// MapHangfireDashboard (endpoint routing), not UseHangfireDashboard (a Map()-branch middleware that
// would exclusively own the whole /hangfire/* prefix and swallow MapJobControl's routes below it).
app.MapHangfireDashboard("/hangfire", new DashboardOptions
{
    Authorization = new[] { new AllowAllDashboardAuthorizationFilter() },
});

app.MapJobControl(
    viewPolicy: Policies.View,
    managePolicy: Policies.Manage);

// A few demo recurring jobs so there's something to see/disable/enable/trigger from
// /hangfire/job-control (or the built-in /hangfire dashboard) right after `dotnet run`.
RecurringJob.AddOrUpdate<DemoJobs>("heartbeat-every-minute", job => job.Heartbeat(), Cron.Minutely());
RecurringJob.AddOrUpdate<DemoJobs>("nightly-report", job => job.NightlyReport(), Cron.Daily());
RecurringJob.AddOrUpdate<DemoJobs>("flaky-every-2-minutes", job => job.SometimesFails(), "*/2 * * * *");

app.Run();

// Exposed for Microsoft.AspNetCore.Mvc.Testing's WebApplicationFactory<Program> in the integration
// test project.
public partial class Program { }

internal static class Policies
{
    public const string View = "OpsToolkit.View";
    public const string Manage = "OpsToolkit.Manage";
}

internal sealed class AllowAllDashboardAuthorizationFilter : IDashboardAuthorizationFilter
{
    public bool Authorize(DashboardContext context) => true;
}

public class DemoJobs
{
    public void Heartbeat() => Console.WriteLine($"[{DateTime.UtcNow:O}] heartbeat");

    public void NightlyReport() => Console.WriteLine($"[{DateTime.UtcNow:O}] nightly report generated");

    // Fails ~1 in 3 runs — gives you something to disable from the job-control UI and watch stop
    // paging, versus a real failure you'd want to keep retrying.
    public void SometimesFails()
    {
        if (Random.Shared.Next(3) == 0)
            throw new InvalidOperationException(
                "Simulated transient failure — try disabling this job from /hangfire/job-control.");
        Console.WriteLine($"[{DateTime.UtcNow:O}] flaky job ran fine this time");
    }
}
