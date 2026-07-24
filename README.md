# OpsToolkit.Hangfire

Operational controls for Hangfire installations that need durable operator state, explicit authorization, and an audit trail.

The first module, `OpsToolkit.Hangfire.JobControl`, provides:

- durable disable and enable for recurring jobs;
- operator schedule and parameter-value overrides that survive deploys (`effective = override ?? code default`), with reset and on-demand reconcile;
- schema-driven manual invoke with edited parameter values — the force-run that executes even while a job is disabled;
- governed trigger and delete actions;
- a Job Runs dashboard for queued, processing, scheduled, succeeded, failed, and deleted jobs;
- reasoned, race-protected cancel, requeue, and delete actions;
- cancellation acknowledgment for processing jobs;
- an opt-in `[Heartbeat]` liveness contract for long-running jobs, with `context.Beat()` progress reporting surfaced on the Runs dashboard;
- stall detection for contracted jobs — a scanning background process, a stalled view with detector health, operator acknowledgment, and a dashboard count tile (flag-only by default);
- opt-in, governed retry-on-stall — the confirmed-hung body is cancelled through the shared, audited cancellation protocol and re-run only on its own acknowledgment, within a per-job budget; an unacknowledged cancel blocks requeue behind an audited break-glass;
- a count-retained operator audit trail; and
- two embedded, zero-build operator UIs.

## Install

```bash
dotnet add package OpsToolkit.Hangfire.JobControl
```

[View `OpsToolkit.Hangfire.JobControl` on NuGet.org](https://www.nuget.org/packages/OpsToolkit.Hangfire.JobControl/).

## Quick start

```csharp
var jobControl = new JobControlOptions
{
    ActorProvider = context =>
        context.User.FindFirst("email")?.Value ?? "unknown",
};

builder.Services.AddHangfire(configuration => configuration
    .UsePostgreSqlStorage(/* ... */)
    .UseJobControl(jobControl));

builder.Services.AddJobControlStallDetector(jobControl); // optional: stall detection for [Heartbeat] jobs

app.MapJobControl(
    viewPolicy: "CanViewJobs",
    managePolicy: "CanManageJobs",
    apiBase: "/hangfire/api",
    options: jobControl);
```

`UseJobControl()` installs the server-side filters that enforce recurring-job disable and acknowledge processing-job cancellation. `MapJobControl()` maps the recurring and run APIs plus both UIs. `AddJobControlStallDetector()` registers the background process that flags contracted long-running jobs whose heartbeats stop. Pass the same options everywhere so server- and HTTP-plane behavior agrees.

To let operators override job schedules and parameter values durably, declare recurring jobs through a `RecurringJobRegistrar` instead of `RecurringJob.AddOrUpdate` and share it via the options:

```csharp
var registrar = new RecurringJobRegistrar();
var jobControl = new JobControlOptions { Registrar = registrar };
// ... UseJobControl(jobControl) + MapJobControl(..., options: jobControl) as above ...

registrar.Register<ReportingJobs>("nightly-report", x => x.Run(), Cron.Daily());
registrar.Apply(JobStorage.Current, jobControl);   // projects effective = override ?? code default
```

Without a registrar every other capability still works; the override and invoke endpoints respond 501 and the UI hides those controls. See [Recurring Jobs control](HangfireRecurringJobs.md) for reconciliation, rollback behavior, and rollout caveats.

`apiBase` is one shared root. JobControl maps recurring-job endpoints beneath
`{apiBase}/recurring` and run endpoints beneath `{apiBase}/runs`. Consumers that require unrelated
paths can map the lower-level `MapJobControlApi()` and `MapJobRunsApi()` methods directly.

> **0.3.0 routing change:** in 0.2.x, `apiBase` represented the complete recurring API path and
> `runsApiBase` configured runs separately. In 0.3.0, `apiBase` is their shared root and
> `runsApiBase` is removed from `MapJobControl()`. `DefaultApiBase` now names the shared root; use
> `DefaultRecurringApiBase` when calling a lower-level recurring mapper.

The default pages are:

- `/hangfire/job-control/recurring` - recurring-job controls and history
- `/hangfire/job-control/runs` - run monitoring, details, and actions
- `/hangfire/job-control` - redirect to the recurring-jobs page

The view and manage policies are required arguments. Reads and UIs use the view policy; all mutations use the manage policy.

## Configuration

```csharp
var options = new JobControlOptions
{
    ActorProvider = context => context.User.Identity?.Name ?? "unknown",
    AuditMaxEntries = 10_000,
    AuditDefaultReadLimit = 200,
    RunsDefaultPageSize = 50,
};
```

All toolkit state is kept in Hangfire storage: custom recurring-job hash fields, a capped audit list, and a per-job cancellation marker. Hosts need no toolkit schema or migration.

Cancellation of a Processing job is cooperative. Job bodies must flow a `CancellationToken` into awaited work or check `IJobCancellationToken`; otherwise they may complete even after the job has moved to Deleted. The audit trail records that case as `completed-anyway`.

### Built-in Dashboard hosting requirement

OpsToolkit's mutation guarantees — expected-state checks, cancellation acknowledgments, per-job
locking, required reasons, and audit records — apply only to mutations that go through OpsToolkit.
Hangfire's built-in Dashboard understands the persisted state name (`Deleted`) but none of those
records, so its native requeue/delete buttons can, for example, requeue a job whose cancelled body
has not yet acknowledged that it stopped (OPS-003). Governed mode therefore requires mapping the
built-in Dashboard read-only; it remains the read plane for queues, servers, states, and history:

```csharp
app.MapHangfireDashboard("/hangfire", new DashboardOptions
{
    Authorization = new[] { new HostDashboardAuthorizationFilter() },
    IsReadOnlyFunc = _ => true,
});
```

| Mode | Native Dashboard | Guarantee |
|---|---|---|
| Governed mode (recommended) | `IsReadOnlyFunc = _ => true` | OpsToolkit is the only operator mutation surface; acknowledgment-gated requeue and audit guarantees apply |
| Compatibility mode | Writable | Native delete/requeue/recurring-job actions bypass OpsToolkit policy: they can requeue an unacknowledged cancellation, skip intervention audits and required reasons, skip the expected-state and per-job locking rules, and leave OpsToolkit workflow projections disagreeing with Hangfire's state |

Read-only Dashboard mode closes the operator-UI bypass; it is not a security boundary. Application
code, another service, or a storage administrator calling Hangfire's state APIs directly can still
mutate governed jobs — every writer sharing the Hangfire storage must follow the same
cancellation/requeue protocol if the governed guarantee is required.

## Hosting limitation

The current package can only be referenced and configured by an ASP.NET Core web host that also
runs the Hangfire server. A split deployment with the dashboard/API web host and Hangfire server
worker in separate processes is not supported by this release. That topology requires the HTTP and
server components to be separated in a future package release.

## Feature documentation

- [Recurring Jobs control](HangfireRecurringJobs.md)
- [Job Runs dashboard and cancellation](HangfireJobRuns.md)
- [Long-running job liveness: heartbeat, progress, stall detection, and governed retry](HangfireLiveness.md)
- [Liveness design: the two-layer model, the retry state machine, and rejected alternatives](HangfireLivenessDesign.md)
- [Operator audit trail](HangfireAuditTrail.md)

## Run the demo

```bash
cd tests/OpsToolkit.Hangfire.Host
docker compose -f docker-compose.postgres.yaml up -d
dotnet run
```

Open the built-in Hangfire dashboard or either toolkit page at the address printed by the host.

## License

MIT - see [LICENSE](LICENSE).
