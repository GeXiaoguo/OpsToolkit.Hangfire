# Long-running job liveness: heartbeat, progress, stall detection, and governed retry

The liveness feature lets a long-running job prove it is *making progress*, not merely that its worker
process is alive — flags the executions whose progress stops, and (strictly by per-job opt-in) runs the
**governed retry-on-stall workflow**: cancel the confirmed-hung body through the shared cancellation
protocol, wait for the cancelled execution's own acknowledgment, and only then re-run it, within a
budget. By default a detected stall is flagged and surfaced, never acted on automatically.

This is the operator guide. The design rationale — the two-layer timeout model, the retry state
machine's rules, and the alternatives considered and rejected — is
[HangfireLivenessDesign.md](HangfireLivenessDesign.md).

## The contract

```csharp
public class ReportingJobs
{
    [Heartbeat(timeoutSeconds: 300)]
    public async Task RebuildLoanBook(PerformContext context, CancellationToken token)
    {
        for (var batch = 1; batch <= total; batch++)
        {
            await ProcessBatchAsync(batch, token);
            context.Beat(percent: batch * 100.0 / total, message: $"batch {batch}/{total}");
        }
    }
}
```

- **Opt-in and local.** Only a method (or declaring type) carrying `[Heartbeat]` participates. Every
  other job is untouched — no storage written, nothing will ever scan it, bit-for-bit today's behavior.
  Calling `Beat()` without the attribute is a no-op; it does not start a contract.
- **Beats come from the job body at real progress points — never from a timer.** A timer-based beater
  would prove the process is alive while the job hangs, which is exactly the blindspot this contract
  exists to close. Silence is the signal.
- **`Beat()` is cheap and safe to call often.** Writes are throttled to one per
  `LivenessOptions.MinBeatInterval` (default 5s); calls in between are coalesced in memory. It never
  throws — a storage blip while persisting a beat is logged and cannot fail the job.
- **Progress rides along.** `percent` (clamped to 0..100) and `message` (truncated to 512 chars) are
  optional; passing null keeps the previously reported values, so a liveness-only `Beat()` never erases
  progress.
- `TimeoutSeconds` is how long the execution may stay silent before the stall detector flags it. It
  must be at least 60 (twice the detector's default scan interval); values of zero or below throw at
  attribute construction.
- `OnStall` opts one job into the retry workflow (`StallAction.Retry`); `MaxRetries` (≥ 0; default 3)
  bounds it — zero means "cancel the confirmed-hung body, never re-run" — and `RetryDelaySeconds`
  (≥ 0; default 60) delays each re-run via Hangfire's own scheduler, so restarts can't re-time it.
  Negative values are refused at contract start (unmonitored + `contract-invalid`, never a thrown
  exception). Like the timeout, the policy is snapshotted at enrollment: the detector applies what the
  executing version declared, even across a rolling deploy.

## Stall detection

The detector is a Hangfire background process. Registering it is the host's single required line —
registered ⇒ running, flag-only, no separate enable switch:

```csharp
builder.Services.AddJobControlStallDetector(jobControlOptions);   // Hangfire.AspNetCore hosts

new BackgroundJobServer(options, storage, new[] { new StallDetector(jobControlOptions) });  // self-hosted
```

That line exists because of a Hangfire seam, not a product choice: `UseJobControl()` registers global
*job filters*, and Hangfire offers no way for a configuration extension to attach a background process
to the server. It is safe with zero configuration — only contracted executions are ever scanned (an
empty index read per interval otherwise).

Every `ScanInterval` (default 30s) the detector:

1. **Renews its health lease** — so "no detector running" is *visible* as degraded status on the
   stalled endpoint, never mistaken for "no stalls".
2. **Self-heals the index**, scoped by execution identity: a tuple whose job is no longer Processing
   (worker crash where the contract never retired, a cancelled zombie's leftover), or whose execution
   was superseded by storage-native invisibility recovery, is removed — never flagged.
3. **Confirms stalls on its own monotonic clock**: it records when *it* first observed the current
   beat sequence number and flags only when that number has stayed unchanged for the contract's
   timeout of its own observation. Cross-server clock skew affects display only; a wall-clock jump
   cannot mass-flag; a storage read failure resets the confirmation baseline (a blip can never
   mass-flag); a detector restart delays — never suppresses — confirmation.
4. **Flags under a per-job distributed lock**, re-reading state inside it, so two detectors racing
   produce exactly one committed flag and one `stall-detected` audit entry.
5. **Un-flags a recovered execution**: a beat that advances past the flagged sequence number retires
   the flag and audits `stall-recovered`. A slow-but-fine job (one long silent DB query) that got
   flagged is recoverable — which is why flag is the default and anything automatic stays opt-in.

**Acknowledge, don't clear.** `POST {apiBase}/runs/{id}/acknowledge-stall` (manage policy, reason
required) records who is looking at a stalled execution — the flag itself stays, because the beat
really is overdue and clearing would silently re-flag. Alerting should page on the *unacknowledged*
count only. The acknowledgment is scoped to the flagged execution, so it can never hide a later
attempt's stall, and it retires automatically with the flag on recovery or terminal state.

## Retry on stall (opt-in, governed)

`OnStall = StallAction.Retry` arms, per job, a durable state machine the detector drives (its inputs
all live in storage, so it survives detector and server restarts):

1. **Two configuration prerequisites, or the policy downgrades to flag-only** (an error log, never a
   crash): the host must declare `LivenessOptions.StorageLeaseWindow`, and the storage's invisibility
   must be sliding (a positively detected non-sliding configuration downgrades every retry).
2. **Cancellation is gated — a possibly-dead owner is never cancelled.** Both must hold: the owning
   server's heartbeat is fresh (within 60s), and the stall has outlived `StorageLeaseWindow`. A dead
   owner's job is recovered by the *storage's own* invisibility re-fetch (that path stays untouched and
   unbounded — cancelling into it would break it); a stall that outlives the window under fresh
   heartbeats is the hung-but-alive case cancellation exists for. Owner absent or stale → flag stands,
   nothing else happens.
3. **The cancel is linearized on the state itself.** The request marker is prepared first, then the
   job moves `Processing → Deleted` via a state whose own data carries the request/execution identity —
   the commit record. The operator cancel endpoint uses the same protocol (one protocol, one audit
   shape; stall-cancels appear under actor `system:liveness`).
4. **Requeue happens only on the cancelled execution's own acknowledgment** — recorded by the filter
   the moment the body observably stopped (`aborted`/`faulted`; a `completed-anyway` body ends the
   workflow with nothing to re-run), matched against the exact request and execution identity, so a
   zombie attempt can never settle a cancel aimed at its replacement. The re-run goes through
   `Scheduled` when `RetryDelaySeconds` > 0; the budget is counted on the job (`stall-retry` audit,
   attempt *n* of *MaxRetries*; `stall-retry-exhausted` when spent, and the run stays Deleted).
5. **No acknowledgment within `AckGracePeriod` — or the owner lost after the cancel committed — means
   BLOCKED**: the job stays Deleted, surfaced on `/runs/stalled` (`retryPhase: "blocked"`) and audited
   `stall-retry-blocked`, with a human-only exit. **No queue row is ever added without an ack** — a
   second row can race the storage's invisibility recovery of the orphaned lease into a double-run.
   The ordinary requeue endpoint refuses with 409 in **both** unacknowledged phases —
   `cancel-requested` (the grace window is a wait for evidence, not evidence) and `blocked` — and a
   late acknowledgment lifts the guard by itself (OPS-003). The break-glass exit from either phase is
   `POST {apiBase}/runs/{id}/force-requeue` (manage policy, reason required, audited
   `force-requeue-unacknowledged-stall` with the source phase): explicit risk acceptance, not
   confirmation that the cancel landed — documented duplicate-execution hazard; recycle the owning
   worker process first. If a `completed-anyway` acknowledgment slips in before the force commits, the
   force is refused and the changed outcome surfaced; an `aborted`/`faulted` one downgrades it to an
   ordinary acknowledged requeue, recorded as such.

Jobs adopting `Retry` must have **idempotent effects or resumable checkpoints**: stall-retry is a
governed duplication path on top of Hangfire's at-least-once baseline. And a retried job's truly hung
predecessor still occupies its worker thread until process recycle — retry frees the *job*, not the
slot.

## What the operator sees

- **Job Runs → Processing**: contracted rows show the progress bar, message, and last beat; an
  *overdue* hint appears the moment a beat is late (wall-clock display aid), and a red **Stalled**
  badge appears when the detector confirms it (the authoritative verdict). A stalled row gains an
  **Acknowledge** action; a *Stalled only* filter narrows the tab; a warning banner appears when the
  detector itself is degraded.
- **`GET {apiBase}/runs/stalled`**: the flagged executions (beat snapshot + flag + acknowledgment),
  detector health (`healthy`/`degraded` with per-server last-scan leases), the active-contract count,
  and acknowledged/unacknowledged counts split for alerting. Retry-workflow executions additionally
  carry `retryPhase` (`cancel-requested`/`blocked`/`exhausted` items are Deleted jobs the workflow
  still governs, kept surfaced here deliberately), `retryAttempt`, and `maxRetries`.
- **Job Runs → Deleted**: a stall-cancelled row shows who cancelled (`system:liveness`) like any
  governed cancel, plus an **Awaiting ack** / **Stall blocked** / **Retries exhausted** pill; while
  the cancel is unacknowledged (awaiting-ack or blocked) the row's only action is the break-glass
  **Force requeue** (separate confirmation, required reason).
- **`GET {apiBase}/runs/processing`**: each row's `beat` object carries `lastBeatAt`, `percent`,
  `message`, `timeoutSeconds`, `overdue`, `stalled`, `acknowledged`.
- **Built-in Hangfire dashboard**: a single *Stalled jobs* count tile on the home page (highlighted
  when non-zero) — the Runs UI stays the rich surface.
- **Audit**: `stall-detected`, `stall-recovered`, `acknowledge-stall`,
  `stall-native-refetch-observed` (a new execution enrolled while an older one was still flagged —
  storage-native recovery observed; the old flag is retired by identity), and the retry workflow's
  `cancel`/`cancel-ack` (shared with operator cancels, distinguished by actor), `stall-retry`,
  `stall-retry-blocked`, `stall-retry-exhausted`, `force-requeue-unacknowledged-stall` — all under
  actor `system:liveness` (configurable) in the same audit trail as every other JobControl action.

## Failure policy: fail open, loudly

Liveness is monitoring, and it must never convert its own problems into job failures:

- An **invalid contract** (e.g. a timeout below the floor) runs the job unmonitored and writes a
  `contract-invalid` audit entry (actor `system:liveness`; audited once per method per process, logged
  as an error on every run).
- A **storage failure during enrollment** runs the job unmonitored and writes `contract-init-failed`.
  An execution is either fully enrolled or honestly unmonitored — never half-monitored.
- A **failing detector pass** advances nothing — no baseline moves, no flag is written, and the health
  lease is not renewed, so a persistently failing detector reads as *degraded*, not as "no stalls".

## Options

Liveness configuration nests in the existing options record and is always present with defaults:

```csharp
var jobControl = new JobControlOptions
{
    Liveness = new LivenessOptions          // optional — these are the defaults
    {
        ScanInterval       = TimeSpan.FromSeconds(30),
        MinBeatInterval    = TimeSpan.FromSeconds(5),
        AckGracePeriod     = TimeSpan.FromSeconds(60),  // ≥ CancellationCheckInterval + one ScanInterval
        StorageLeaseWindow = null,                      // REQUIRED for StallAction.Retry — set ≥ the
                                                        // storage invisibility timeout; unset ⇒ retry
                                                        // downgrades to flag-only
        ActorName          = "system:liveness",
    },
};
```

Per-job values (`TimeoutSeconds`, `OnStall`, `MaxRetries`, `RetryDelaySeconds`) live on the attribute;
there is deliberately no global default timeout — opt-in stays explicit and local.

## Storage

All state lives in Hangfire's own storage (no table, no migration), execution-scoped so overlapping
attempts of one background-job id — possible under storage-native invisibility recovery — can never
overwrite each other:

| Key | Kind | Lifetime |
|---|---|---|
| `JobControl.Beat:{executionId}` | job parameter — contract snapshot + latest beat (`seq`, `beatAt`, `percent`, `message`) | expires with the job; kept at contract end so terminal views can show final progress |
| `JobControl.Liveness.Current` | job parameter — the current execution's id | written only at contract start (a zombie never re-runs `OnPerforming`) |
| `JobControl.Stalled:{executionId}` | job parameter — stall flag + acknowledgment | expires with the job |
| `jobcontrol:liveness:active` | set of `{jobId}\|{executionId}` tuples | added at contract start, removed (exact tuple) at contract end; self-healed by the detector |
| `jobcontrol:liveness:stalled` | set of tuples | added on flag; removed on recovery, terminal state, or self-heal — except the retry workflow's surfaced Deleted phases (`cancel-requested`/`blocked`/`exhausted`), retained while the committed cancel still governs the job |
| `jobcontrol:liveness:retry-pending` | set of tuples | drives every post-cancel workflow phase; removed on requeue, blocking, exhaustion, supersession, or terminal cleanup |
| `JobControl.CancelRequested` | job parameter — the governed-cancel request marker (shared with operator cancels; carries the request/execution identity since 0.5) | prepared before the cancel transition; cleared on requeue or a lost transition |
| `JobControl.CancelAck:{executionId}:{requestId}` | job parameter — the cancelled execution's acknowledgment (`aborted`/`faulted`/`completed-anyway`) | expires with the job |
| `JobControl.StallAttempt:{executionId}` | job parameter — retry-workflow phase record (`cancel-requested`/`retried`/`blocked`/`exhausted`/…, attempt number, policy snapshot) | expires with the job; terminal phases persist as the workflow's durable record |
| `JobControl.StallRetryCount` | job parameter — stall retries spent on this job (each retry is a fresh execution id, so the budget rides on the job) | expires with the job |
| `jobcontrol:liveness:detector:{serverId}` (+ `jobcontrol:liveness:detectors` index) | hash — per-server health lease, last successful scan | storage-expired several intervals out; freshness judged from `lastScanAt` at read time |

## Long-running jobs need more than a heartbeat

Two host-side facts to plan around, covered in depth by the liveness plan:

- **Storage invisibility must be sliding.** Hangfire.PostgreSql defaults to a fixed 30-minute
  `InvisibilityTimeout` with `UseSlidingInvisibilityTimeout = false`: any job running longer is handed
  to a second worker — duplicated, regardless of beats. Long-running jobs require
  `UseSlidingInvisibilityTimeout = true` (SQL Server: `SlidingInvisibilityTimeout`). The detector runs
  a best-effort configuration check at startup, warns when it positively detects a non-sliding
  configuration, and downgrades `StallAction.Retry` to flag-only on such storage.
- **Deploys abort long jobs** (`ShutdownTimeout` defaults to 15s), and Hangfire is at-least-once —
  long-running job bodies should be idempotent or checkpoint-resumable.
- **A stalled worker slot is never reclaimed by flagging.** Cancel + requeue (the shipped Runs
  actions) free the *job*; a truly hung body occupies its worker thread until process recycle. The
  best mitigation is a dedicated queue + separate server process for long-running jobs, so a hang
  can't starve short jobs and recycling is cheap.
