# OpsToolkit.Hangfire

Production controls for Hangfire installations that need durable operator state, explicit
authorization, and an audit trail—without adding a database schema, frontend build, or
storage-provider dependency.

`OpsToolkit.Hangfire.JobControl` provides:

- durable disable and enable for recurring jobs;
- governed trigger and delete actions;
- a Job Runs dashboard for queued, processing, scheduled, succeeded, failed, and deleted jobs;
- reasoned, race-protected cancel, requeue, and delete actions;
- cancellation acknowledgement for processing jobs;
- a count-retained operator audit trail; and
- two embedded, zero-build operator UIs.

## Why use it?

Hangfire's dashboard is excellent for observing background work, but production teams often need a
stronger operational boundary. JobControl adds that boundary while staying close to Hangfire's own
storage model:

- **Operator accountability:** audit entries capture actor, action, job, reason, timestamp, and
  outcome.
- **Authorization by construction:** separate ASP.NET Core policies protect viewing and management.
- **Zero-schema adoption:** toolkit state uses Hangfire's existing storage primitives—no migrations
  or additional tables.
- **Bundled UI:** two embedded HTML resources, with no npm toolchain or static-file setup.
- **Storage-provider friendly:** the package depends on `Hangfire.Core`, not a particular SQL or
  storage provider.

## Install

```bash
dotnet add package OpsToolkit.Hangfire.JobControl
```

[View `OpsToolkit.Hangfire.JobControl` on NuGet.org](https://www.nuget.org/packages/OpsToolkit.Hangfire.JobControl/).

## Quick start

Use one options instance for both the server and HTTP planes so their audit retention agrees:

```csharp
var jobControl = new JobControlOptions
{
    ActorProvider = context =>
        context.User.FindFirst("email")?.Value ?? "unknown",
};

builder.Services.AddHangfire(configuration => configuration
    .UsePostgreSqlStorage(/* your existing storage configuration */)
    .UseJobControl(jobControl));

app.MapJobControl(
    viewPolicy: "CanViewJobs",
    managePolicy: "CanManageJobs",
    apiBase: "/hangfire/api",
    options: jobControl);
```

`UseJobControl()` installs the server-side filters that enforce recurring-job disable and
acknowledge processing-job cancellation. `MapJobControl()` maps both APIs and both UIs.

`apiBase` is one shared root. JobControl maps recurring-job endpoints beneath
`{apiBase}/recurring` and run endpoints beneath `{apiBase}/runs`. Consumers that require unrelated
paths can map the lower-level `MapJobControlApi()` and `MapJobRunsApi()` methods directly.

> **0.3.0 routing change:** in 0.2.x, `apiBase` represented the complete recurring API path and
> `runsApiBase` configured runs separately. In 0.3.0, `apiBase` is their shared root and
> `runsApiBase` is removed from `MapJobControl()`. `DefaultApiBase` now names the shared root; use
> `DefaultRecurringApiBase` when calling a lower-level recurring mapper.

The default pages are:

- `/hangfire/job-control/recurring` — recurring-job controls and history
- `/hangfire/job-control/runs` — run monitoring, details, and actions
- `/hangfire/job-control` — redirects to the recurring-jobs page

The view and manage policies are required arguments. Reads and UIs use the view policy; all
mutations use the manage policy.

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

All toolkit state is kept in Hangfire storage: custom recurring-job hash fields, a capped audit
list, and per-job cancellation markers. Hosts need no toolkit schema or migration.

Cancellation of a processing job is cooperative. Job bodies must flow a `CancellationToken` into
awaited work or check `IJobCancellationToken`; otherwise they may complete after the job moves to
Deleted. The audit trail records that case as `completed-anyway`.

## Feature documentation

- [Recurring Jobs control](HangfireRecurringJobs.md)
- [Job Runs dashboard and cancellation](HangfireJobRuns.md)
- [Operator audit trail](HangfireAuditTrail.md)

## Try the demo

The included host uses PostgreSQL for realistic storage and HTTP integration testing:

```bash
cd tests/OpsToolkit.Hangfire.Host
docker compose -f docker-compose.postgres.yaml up -d
dotnet run
```

Open `/hangfire` for the standard dashboard or either toolkit page listed above.

## Build and test

With the demo PostgreSQL container running:

```bash
dotnet restore OpsToolkit.Hangfire.sln
dotnet build OpsToolkit.Hangfire.sln --configuration Release --no-restore
dotnet test OpsToolkit.Hangfire.sln --configuration Release --no-build
```

## Community

Bug reports, compatibility findings, documentation improvements, and focused feature proposals are
welcome through GitHub Issues. If you run Hangfire in a production or regulated environment,
sharing the operational problem you are solving is especially valuable—it helps keep the toolkit
practical and provider-neutral.

OpsToolkit.Hangfire is an independent community project and is not affiliated with or endorsed by
Hangfire OÜ.

## License

MIT. See [LICENSE](LICENSE).
