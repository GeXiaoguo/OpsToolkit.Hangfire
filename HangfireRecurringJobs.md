# Recurring Jobs control

`OpsToolkit.Hangfire.JobControl` adds governed operator controls for Hangfire recurring jobs. It stores control state in Hangfire's own storage and provides both an HTTP API and an embedded, zero-build UI.

## Capabilities

- List recurring jobs with schedule, execution, and disabled-state details.
- Disable a job with a required reason; enable, trigger, or delete it.
- Keep disabled state across `RecurringJob.AddOrUpdate` calls and application restarts.
- Override a job's schedule with a required reason, durably across deploys; reset to the code default (requires the registrar, below).
- Override a job's parameter values the same way, against a schema derived from the method signature (requires the registrar).
- Invoke a job on demand with edited parameter values — a force-run that executes even while the job is disabled, optionally persisting the values as the override (requires the registrar).
- Apply separate host-supplied authorization policies to viewing and mutation.
- Record every operator mutation in the audit trail.

Disabling is enforced when an occurrence begins. It does not stop an occurrence already running, and enabling a job does not backfill skipped occurrences. Use the Job Runs cancellation action to stop a currently processing job.

## Storage and execution

The disabled flag and its actor, time, and reason are custom fields on Hangfire's `recurring-job:{id}` hash. Hangfire preserves unknown fields during `AddOrUpdate`, so no application schema or migration is required. Removing the recurring job removes the hash and its control state.

`UseJobControl()` registers `DisabledRecurringJobFilter` globally. Before a recurring occurrence executes, the filter reads its `RecurringJobId`; if that definition is disabled, the occurrence is moved to Deleted without executing the job body. One-off jobs are unaffected.

A deployment that removes `UseJobControl()` stops enforcement even though the stored flags remain. Hosts should treat server-plane registration as required configuration.

## Operator overrides (schedule and parameters)

Overrides split each recurring job into a code-declared **type definition** (what to run, the default cron and argument values, the time zone) and operator-owned **runtime data** (the current cron and/or parameter values). What Hangfire schedules is always `effective = override ?? code default`, re-asserted at every startup — which is what a plain `AddOrUpdate` host cannot offer: there, every deploy silently resets an operator's change.

Adoption requires the host to declare its jobs through a `RecurringJobRegistrar` instead of calling `RecurringJob.AddOrUpdate` directly:

```csharp
var registrar = new RecurringJobRegistrar();
var jobControl = new JobControlOptions { Registrar = registrar };

builder.Services.AddHangfire(config => config /* ... */ .UseJobControl(jobControl));
// ...
app.MapJobControl(viewPolicy: "...", managePolicy: "...", options: jobControl);

registrar
    .Register<ReportingJobs>("nightly-report", x => x.Run(), Cron.Daily())
    .Register<PaymentJobs>("payment-sweep", x => x.Sweep(), "*/30 * * * *");
registrar.Apply(JobStorage.Current, jobControl);   // reconcile + project; never throws for a bad row
```

Hosts that don't adopt the registrar keep every other capability; the override and invoke endpoints answer 501 with an explanation and the UI hides the controls.

### Parameter overrides and manual invoke

A job's parameter schema is derived from its method signature, with the expression-baked registration arguments as the code defaults: `Register<ReportingJobs>("nightly-report", x => x.Run(30, false), ...)` declares `30` and `false` as the defaults for those two parameters. Parameters typed `CancellationToken`/`IJobCancellationToken` are server-injected at perform time and excluded from operator editing.

An override is a JSON object of parameter name → value and may be **partial**: a missing name means the code default, per parameter — so a stored override stays valid when a deploy adds a new parameter with a new default. Values are validated by binding them against the deployed method signature (the parameter twin of cron's project-first rule) before anything is persisted; an unknown name or unconvertible value is rejected with a per-parameter error. Primitive types (numerics, `bool`, `string`, enums by name, `DateTime`/`DateTimeOffset`/`TimeSpan`/`Guid`, nullables) get typed coercion; any other parameter type binds as raw JSON.

Invoke creates a one-off run using the job's effective values overlaid with whatever the operator edited for that run. It is deliberately an ad-hoc job carrying the `JobControl.ManualInvokeOf` parameter instead of Hangfire's `RecurringJobId`, so `DisabledRecurringJobFilter` does **not** skip it — invoking a disabled job is the intended force-run escape hatch (the built-in "trigger" stays skipped). With `persist: true` (reason required), the values used are also stored as the job's parameter override, so scheduled fires adopt them.

### Storage and reconciliation

Overrides live in a toolkit-owned hash per job (`jobcontrol:runtime:{jobId}`) plus an index set (`jobcontrol:runtime`) — Hangfire's own storage, core connection APIs, no table, no migration. Deliberately **not** on the `recurring-job:{id}` hash: that hash dies with the job, while an override must survive removal and rollback. One row carries both override kinds; resetting one leaves the other in place, and the row is removed only when nothing remains on it.

`Apply` reconciles every stored row against the declared definitions on every startup (and on demand via the reconcile endpoint):

- A row whose job no longer exists in code is **soft-invalidated** (`job-removed`) — marked, never deleted. A rollback that re-declares the job re-validates the row and the override resumes without operator action.
- A row whose cron Hangfire's own parser rejects is soft-invalidated (`bad-cron`); one whose parameter values no longer bind to the deployed method signature (renamed/removed parameter, incompatible type) is soft-invalidated (`bad-args`). Validation is project-first everywhere — the override endpoints validate through the same projection/binding the scheduler and performer use, before persisting, so "accepted" can never drift from "runnable".
- A dormant row falls back to the code defaults **as a unit** — schedule and parameter values both. A valid cron override never half-applies while its args are broken, or vice versa; "override dormant" always means the code-declared behavior is what runs.
- A failing projection never fails startup; errors are captured in the returned summary.
- Recurring jobs in storage that no `Register` call declared are reported in the summary. Passing `removeUndeclared: true` to `Apply` removes them — opt-in only, because it deletes jobs registered outside the registrar (plain `AddOrUpdate`, dashboard-created pilots). Each removal is audited.

### Rollout caveat (mixed-version deploys)

While old and new versions run simultaneously (canary, rolling deploy), a pod still on plain-`AddOrUpdate` registration re-asserts code crons over any override at restart, and nothing re-projects until a registrar-version startup, an override edit, or `POST .../reconcile`. This self-resolves once the rollout that introduces the registrar completes; enter overrides after it does, or hit reconcile post-promotion.

## HTTP surface

With the default paths, `MapJobControl()` exposes:

`MapJobControl(apiBase: "/hangfire/api")` treats `apiBase` as a shared root and derives this
feature's `/recurring` branch. The lower-level `MapJobControlApi()` method still accepts the full
recurring API path for hosts that compose endpoints manually.

| Method | Path | Purpose |
|---|---|---|
| GET | `/hangfire/api/recurring` | List recurring jobs (includes code default + override state) |
| POST | `/hangfire/api/recurring/{jobId}/disable` | Disable; body requires `reason` |
| POST | `/hangfire/api/recurring/{jobId}/enable` | Enable; optional reason |
| POST | `/hangfire/api/recurring/{jobId}/trigger` | Trigger an occurrence |
| POST | `/hangfire/api/recurring/{jobId}/delete` | Delete the definition |
| POST | `/hangfire/api/recurring/{jobId}/cron` | Override the schedule; body requires `cron` and `reason` |
| POST | `/hangfire/api/recurring/{jobId}/cron/reset` | Remove the schedule override, restore the code default |
| GET | `/hangfire/api/recurring/{jobId}/parameters` | Parameter schema with code-default, override, and effective values |
| POST | `/hangfire/api/recurring/{jobId}/args` | Override parameter values; body requires `args` and `reason` |
| POST | `/hangfire/api/recurring/{jobId}/args/reset` | Remove the parameter override, restore the code defaults |
| POST | `/hangfire/api/recurring/{jobId}/invoke` | One-off run with optional `args`; `persist: true` (with `reason`) also stores them; runs even when disabled |
| POST | `/hangfire/api/recurring/reconcile` | Re-run reconcile + project on demand (never removes undeclared jobs) |
| GET | `/hangfire/api/recurring/audit` | Read audit entries, optionally filtered by `jobId` |
| GET | `/hangfire/job-control/recurring` | Embedded operator UI |

The override, parameters, and invoke routes respond `501` when `JobControlOptions.Registrar` is not configured, and `404` for a job the registrar doesn't declare (an override needs the code definition to rebuild the job — the stored payload is never trusted). Override mutations are audited as `cron-override`/`args-override` (with old→new detail), `cron-reset`/`args-reset` (with the restored defaults), `invoke` (with the values used and the created job id), and `reconcile` (with a change summary).

The view policy protects reads and the UI; the manage policy protects mutations. Both policies are required mapping arguments, so the feature cannot accidentally be mapped anonymously.

## Operational notes

- A disabled job's ordinary Hangfire "trigger now" occurrence is also skipped; the invoke endpoint is the deliberate exception and always runs.
- Deleted-state retention is controlled by the Hangfire storage provider; the toolkit audit is the durable operator record.
- Delete captures a definition snapshot in its audit entry because the recurring-job hash no longer exists afterward.
