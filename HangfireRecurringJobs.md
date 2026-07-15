# Recurring Jobs control

`OpsToolkit.Hangfire.JobControl` adds governed operator controls for Hangfire recurring jobs. It stores control state in Hangfire's own storage and provides both an HTTP API and an embedded, zero-build UI.

## Capabilities

- List recurring jobs with schedule, execution, and disabled-state details.
- Disable a job with a required reason; enable, trigger, or delete it.
- Keep disabled state across `RecurringJob.AddOrUpdate` calls and application restarts.
- Apply separate host-supplied authorization policies to viewing and mutation.
- Record every operator mutation in the audit trail.

Disabling is enforced when an occurrence begins. It does not stop an occurrence already running, and enabling a job does not backfill skipped occurrences. Use the Job Runs cancellation action to stop a currently processing job.

## Storage and execution

The disabled flag and its actor, time, and reason are custom fields on Hangfire's `recurring-job:{id}` hash. Hangfire preserves unknown fields during `AddOrUpdate`, so no application schema or migration is required. Removing the recurring job removes the hash and its control state.

`UseJobControl()` registers `DisabledRecurringJobFilter` globally. Before a recurring occurrence executes, the filter reads its `RecurringJobId`; if that definition is disabled, the occurrence is moved to Deleted without executing the job body. One-off jobs are unaffected.

A deployment that removes `UseJobControl()` stops enforcement even though the stored flags remain. Hosts should treat server-plane registration as required configuration.

## HTTP surface

With the default paths, `MapJobControl()` exposes:

`MapJobControl(apiBase: "/hangfire/api")` treats `apiBase` as a shared root and derives this
feature's `/recurring` branch. The lower-level `MapJobControlApi()` method still accepts the full
recurring API path for hosts that compose endpoints manually.

| Method | Path | Purpose |
|---|---|---|
| GET | `/hangfire/api/recurring` | List recurring jobs |
| POST | `/hangfire/api/recurring/{jobId}/disable` | Disable; body requires `reason` |
| POST | `/hangfire/api/recurring/{jobId}/enable` | Enable; optional reason |
| POST | `/hangfire/api/recurring/{jobId}/trigger` | Trigger an occurrence |
| POST | `/hangfire/api/recurring/{jobId}/delete` | Delete the definition |
| GET | `/hangfire/api/recurring/audit` | Read audit entries, optionally filtered by `jobId` |
| GET | `/hangfire/job-control/recurring` | Embedded operator UI |

The view policy protects reads and the UI; the manage policy protects mutations. Both policies are required mapping arguments, so the feature cannot accidentally be mapped anonymously.

## Operational notes

- A disabled job's ordinary Hangfire "trigger now" occurrence is also skipped.
- Deleted-state retention is controlled by the Hangfire storage provider; the toolkit audit is the durable operator record.
- Delete captures a definition snapshot in its audit entry because the recurring-job hash no longer exists afterward.
