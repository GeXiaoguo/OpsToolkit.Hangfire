using Hangfire;
using Hangfire.Dashboard;
using Hangfire.Server;
using OpsToolkit.Hangfire.JobControl;
using Hangfire.PostgreSql;

var builder = WebApplication.CreateBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("Hangfire")
    ?? "host=localhost;port=5434;database=opstoolkit_hangfire;username=postgres;password=postgres";

builder.Services.AddAuthentication();
builder.Services.AddAuthorization(options =>
{
    // Demo-only: anyone can view and manage. A real host wires these to its own identity/roles —
    // MapJobControl takes policy names precisely so this swap is the only thing that changes.
    options.AddPolicy(Policies.View, policy => policy.RequireAssertion(_ => true));
    options.AddPolicy(Policies.Manage, policy => policy.RequireAssertion(_ => true));
});

// The registrar is created before the options record because the schedule-override endpoints reach
// the definitions through JobControlOptions.Registrar — same instance, both planes.
var registrar = new RecurringJobRegistrar();
var jobControlOptions = new JobControlOptions
{
    Registrar = registrar,
    // Tuned down from the 30s default so the stall-detection integration/manual tests (which wait for
    // a scan to confirm a stall) run in a reasonable time — same reasoning as
    // CancellationCheckInterval below. An idle pass is two empty set reads, so this is cheap even at 2s.
    Liveness = new LivenessOptions
    {
        ScanInterval = TimeSpan.FromSeconds(2),
        // Demo/test-tuned §5 retry gates. A production host sets StorageLeaseWindow to AT LEAST its
        // storage invisibility timeout (Postgres default: 30 minutes) — it is the "a dead owner would
        // already have been natively recovered by now" proof window, and 3s only proves that against
        // this host's short test timeline. AckGracePeriod likewise trades the 60s default for fast
        // blocked-path tests; keep it ≥ CancellationCheckInterval + one ScanInterval.
        StorageLeaseWindow = TimeSpan.FromSeconds(3),
        AckGracePeriod = TimeSpan.FromSeconds(5),
    },
};

builder.Services.AddHangfire(config => config
    .SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
    .UseSimpleAssemblyNameTypeSerializer()
    .UseRecommendedSerializerSettings()
    // Sliding invisibility is the §1 host prerequisite for long-running jobs (and required for
    // StallAction.Retry — the detector downgrades retry to flag-only on a positive non-sliding
    // detection): with the fixed default, any job outliving 30 minutes is handed to a second worker.
    .UsePostgreSqlStorage(
        options => options.UseNpgsqlConnection(connectionString),
        new Hangfire.PostgreSql.PostgreSqlStorageOptions { UseSlidingInvisibilityTimeout = true })
    .UseJobControl(jobControlOptions));

// Detection plane of liveness: registered ⇒ running (flag-only). Hangfire.AspNetCore hands every
// DI-registered IBackgroundProcess to the server it starts.
builder.Services.AddJobControlStallDetector(jobControlOptions);

builder.Services.AddHangfireServer(options =>
{
    // Tuned down from the 5s default so the cancel-protocol integration/manual tests (which wait for the
    // watcher to observe an abort) run in a
    // reasonable time. A production host should weigh this against the per-tick GetStateData cost per
    // watched token before copying this value.
    options.CancellationCheckInterval = TimeSpan.FromSeconds(1);
});

var app = builder.Build();

app.UseAuthentication();
app.UseAuthorization();

// MapHangfireDashboard (endpoint routing), not UseHangfireDashboard (a Map()-branch middleware that
// would exclusively own the whole /hangfire/* prefix and swallow MapJobControl's routes below it).
// Governed mode (OPS-003): the built-in Dashboard stays the read plane — queues, servers, states,
// history — while every operator MUTATION goes through OpsToolkit, whose endpoints carry the
// expected-state checks, cancellation acknowledgments, per-job locking, and audit records the native
// requeue/delete buttons know nothing about. A host that keeps the Dashboard writable is in
// compatibility mode: native actions can requeue an unacknowledged cancellation and bypass every
// OpsToolkit guarantee (see README "Built-in Dashboard hosting requirement").
app.MapHangfireDashboard("/hangfire", new DashboardOptions
{
    Authorization = new[] { new AllowAllDashboardAuthorizationFilter() },
    IsReadOnlyFunc = _ => true,
});

app.MapJobControl(
    viewPolicy: Policies.View,
    managePolicy: Policies.Manage,
    options: jobControlOptions);

// A few demo recurring jobs so there's something to see/disable/enable/trigger from
// /hangfire/job-control/recurring (or the built-in /hangfire dashboard) right after `dotnet run`.
// Declared through the registrar (not RecurringJob.AddOrUpdate) so their schedules are
// operator-overridable from the Recurring page's Schedule tab, durably across restarts.
registrar
    .Register<DemoJobs>("heartbeat-every-minute", job => job.Heartbeat(), Cron.Minutely())
    .Register<DemoJobs>("nightly-report", job => job.NightlyReport(), Cron.Daily())
    .Register<DemoJobs>("flaky-every-2-minutes", job => job.SometimesFails(), "*/2 * * * *")
    // Parameterized: the expression-baked arguments (30, false) become the code-default parameter
    // values, editable from the Recurring page's Parameters/Invoke tabs; the token is server-injected.
    .Register<DemoJobs>("retention-sweep", job => job.RetentionSweep(30, false, CancellationToken.None), Cron.Daily(2));

// Never fire on their own (Cron.Never()) — seeded purely so "Trigger now" on the Recurring page can put
// one in the Processing tab on demand for local verification.
registrar
    .Register<CancellationTestJobs>("cancel-demo-token-honoring", job => job.HonoringLoop(default), Cron.Never())
    .Register<CancellationTestJobs>("cancel-demo-token-ignoring", job => job.IgnoringLoop(), Cron.Never())
    // The [Heartbeat] contract demo: trigger it, then watch the Runs page Processing tab's Progress
    // column fill over ~a minute. The null! is the PerformContext placeholder Hangfire fills at perform
    // time (the Hangfire.Console convention).
    .Register<LongRunningJobs>("liveness-demo-slow-batch", job => job.SlowBatch(null!, CancellationToken.None), Cron.Never())
    // The stall-detection demo: trigger it, watch the Runs page flag it Stalled a bit over a minute in
    // (60s contract timeout of detector observation), acknowledge it, then watch it recover on its own.
    .Register<LongRunningJobs>("liveness-demo-goes-silent", job => job.GoesSilentThenRecovers(null!, CancellationToken.None), Cron.Never())
    // The stall-RETRY demo (§5): beats briefly then hangs on its token forever. Flagged ~60s in,
    // stall-cancelled once the stall outlives the (demo-tuned) StorageLeaseWindow, the abort acks,
    // and the job is re-run 15s later — the second hang exhausts the budget (MaxRetries = 1) and the
    // run stays Deleted, surfaced on /runs/stalled.
    .Register<LongRunningJobs>("liveness-demo-retry-on-stall", job => job.HangsUntilRetried(null!, CancellationToken.None), Cron.Never());

// Deliberately NOT registrar-declared: exercises the Declared=false path (Schedule tab disabled for
// this row) and shows that undeclared jobs survive Apply — removeUndeclared stays opt-in and off.
RecurringJob.AddOrUpdate<DemoJobs>("dashboard-pilot-job", job => job.Heartbeat(), Cron.Never());

// Reconcile + project effective = override ?? code default. Never throws for a bad stored row.
registrar.Apply(JobStorage.Current, jobControlOptions);

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

    // Parameterized fixture for the parameter-override feature (ParameterOverrideApiTests + manual
    // verification): two operator-editable values and one server-injected token.
    public void RetentionSweep(int daysToKeep, bool dryRun, CancellationToken token) =>
        Console.WriteLine($"[{DateTime.UtcNow:O}] retention sweep: keep {daysToKeep} days (dryRun={dryRun})");

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

// Fixtures for the liveness beat contract (PR 1: beats + progress display). SlowBatch is the manual
// demo; SteadyBeats is the fast fixture LivenessApiTests enqueues directly — it must outlive the 5s
// beat-write throttle so at least one progress-carrying beat persists while the test polls /processing.
public class LongRunningJobs
{
    [Heartbeat(120)]
    public async Task SlowBatch(PerformContext context, CancellationToken token)
    {
        const int batches = 12;
        for (var i = 1; i <= batches; i++)
        {
            await Task.Delay(TimeSpan.FromSeconds(5), token);
            context.Beat(percent: i * 100.0 / batches, message: $"batch {i}/{batches}");
        }
    }

    [Heartbeat(90)]
    public void SteadyBeats(PerformContext context)
    {
        for (var i = 1; i <= 45; i++) // ~9s at the 200ms step
        {
            Thread.Sleep(TimeSpan.FromMilliseconds(200));
            context.Beat(percent: i * 100.0 / 45, message: $"step {i}/45");
        }
    }

    // Manual stall-retry demo (§5 of the liveness plan): beats twice, then hangs forever — but on its
    // token, so the governed stall-cancel aborts it promptly (the OperationCanceledException surfaces
    // as the JobAbortedException acknowledgment). MaxRetries 1 ⇒ the first stall retries 15s later,
    // the second exhausts the budget and the run stays Deleted, surfaced on the stalled endpoint.
    [Heartbeat(60, OnStall = StallAction.Retry, MaxRetries = 1, RetryDelaySeconds = 15)]
    public async Task HangsUntilRetried(PerformContext context, CancellationToken token)
    {
        for (var i = 1; i <= 2; i++)
        {
            await Task.Delay(TimeSpan.FromSeconds(5), token);
            context.Beat(percent: i * 10, message: $"warm-up {i}/2 — will hang after this");
        }

        await Task.Delay(Timeout.InfiniteTimeSpan, token); // the silent hang — only a cancel ends it
    }

    // Manual stall-detection demo: beats for ~15s, hangs silently for 2.5 minutes (flagged Stalled
    // ~60s of detector observation in — long enough to try Acknowledge), then beats again so the
    // stall-recovered path is visible too. Waits on the token so a cancel still lands promptly.
    [Heartbeat(60)]
    public void GoesSilentThenRecovers(PerformContext context, CancellationToken token)
    {
        for (var i = 1; i <= 3; i++)
        {
            if (token.WaitHandle.WaitOne(TimeSpan.FromSeconds(5))) return;
            context.Beat(percent: i * 5, message: $"warm-up {i}/3");
        }

        if (token.WaitHandle.WaitOne(TimeSpan.FromMinutes(2.5))) return; // the silent hang

        context.Beat(percent: 90, message: "recovered — wrapping up");
        if (token.WaitHandle.WaitOne(TimeSpan.FromSeconds(6))) return;
        context.Beat(percent: 100, message: "done");
    }
}

// Fixtures for the cancel protocol — one job that observes an
// abort promptly (flows its token into awaited work, mechanic #3) and one that can't (no token at all,
// so it runs to completion regardless of any cancel request — the "completed anyway" case, §2.3). The
// cancel-protocol integration tests enqueue these directly (BackgroundJob.Enqueue); the demo host also
// registers them as never-firing recurring jobs so "Trigger now" can seed one on demand for manual
// verification against the Job Runs UI.
public class CancellationTestJobs
{
    public async Task HonoringLoop(CancellationToken token)
    {
        for (var i = 0; i < 3000; i++) // up to ~60s at the 200ms step below — tests cancel it well before that
        {
            await Task.Delay(TimeSpan.FromMilliseconds(200), token);
        }
    }

    public void IgnoringLoop()
    {
        Thread.Sleep(TimeSpan.FromSeconds(4)); // long enough for a test to cancel it mid-flight
    }

    // Deterministic Failed fixture for the requeue/delete tests (RequeueDeleteApiTests) — no automatic
    // retry, so it lands in Failed on the very first attempt instead of cycling through Scheduled
    // backoff first.
    [AutomaticRetry(Attempts = 0)]
    public void AlwaysFails() => throw new InvalidOperationException("Seeded failure for requeue/delete tests.");
}
