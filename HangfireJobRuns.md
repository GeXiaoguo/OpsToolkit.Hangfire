# Job Runs dashboard and cancellation

The Job Runs feature provides an authorized JSON facade over Hangfire's monitoring API, an embedded operator UI, and governed cancel, requeue, and delete actions.

## Read surface

The dashboard covers statistics, queues, servers, and paged Enqueued, Processing, Scheduled, Succeeded, Failed, and Deleted lists. Job details include invocation data, parameters, and full state history. Page size defaults to `RunsDefaultPageSize` and is capped at 500.

Default paths are under `/hangfire/api/runs`; the embedded UI is `/hangfire/job-control/runs`.
`MapJobControl(apiBase: "/hangfire/api")` derives the `/runs` branch from that shared root. The
lower-level `MapJobRunsApi()` method continues to accept a complete, independent runs API path.

## Actions and race protection

All mutations require the manage policy. Clients send the state they observed as `expectedState`; a concurrent state change returns `409` rather than applying an action to stale data. Unknown jobs return `404`.

| Action | Allowed states | Notes |
|---|---|---|
| Cancel | Enqueued, Scheduled, Processing | Reason required; transitions the job to Deleted |
| Requeue | Enqueued, Scheduled, Succeeded, Failed, Deleted | Processing is refused to prevent concurrent double execution |
| Delete | Succeeded, Failed | Removes a terminal job and snapshots identifying details into audit |

Requeue clears any cancellation marker so a later execution cannot produce a false acknowledgment.

## Processing-job cancellation protocol

Cancellation uses Hangfire's existing state-change cancellation machinery:

1. The API records actor and reason, changes the Processing job to Deleted using the expected-state guard, writes a `JobControl.CancelRequested` job parameter, and audits `cancel`.
2. Hangfire's server cancellation watcher observes that the job is no longer Processing and signals the job's cancellation token.
3. `CancellationOutcomeFilter.OnPerformed` reads the marker and audits `cancel-ack` as `aborted`, `completed-anyway`, or `faulted`.

Queued and scheduled jobs have no running body, so their cancellation completes at the state change and needs no acknowledgment.

Cancellation is cooperative. Job methods should accept a `CancellationToken` and pass it to awaited operations, or accept `IJobCancellationToken` and call `ThrowIfCancellationRequested()` at safe points. Raw `CancellationToken` delivery is bounded by the host's `BackgroundJobServerOptions.CancellationCheckInterval` (five seconds by default in Hangfire). A body that observes neither mechanism can finish its side effects even though the stored job remains Deleted; the `completed-anyway` acknowledgment makes this visible.

The cancellation marker is a job parameter and expires with the Hangfire job. Audit history has its own count-based retention.
