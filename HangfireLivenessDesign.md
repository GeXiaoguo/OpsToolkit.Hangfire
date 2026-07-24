# Liveness design: heartbeat, stall detection, and governed retry

This document records *why* the liveness feature is shaped the way it is — the model it is built on,
the invariants each piece enforces, and the alternatives that were considered and rejected. The
operator-facing guide (what to configure, what you see, how to respond) is
[HangfireLiveness.md](HangfireLiveness.md).

## Requirements

| # | Requirement | Where satisfied |
|---|---|---|
| R1 | A long-running (or forever-running) job must **not time out** as long as it keeps emitting heartbeats | Two layers — the storage prerequisite plus a detector that only flags contracted executions whose beats stop |
| R2 | **Retry** must work for a long-running job whose heartbeat goes missing | Split by cause: a **crashed worker** is retried by storage-native invisibility recovery, which liveness deliberately never disturbs; a **hung-but-alive body** gets the governed, acknowledgment-gated cancel → requeue workflow |
| R3 | Heartbeat is **opt-in**; a job that declares no heartbeat behaves exactly as today | `[Heartbeat]` is the contract; no attribute → no storage written, never scanned, never flagged |

## Why a JobControl feature, not a separate package

Liveness is one capability with the shipped cancellation machinery, not an independent module:

1. **Heartbeat, stall detection, and cancellation only make sense together.** The operator story —
   *stall detected → review → cancel → acknowledge → retry* — should not be split across two
   packages, two audit feeds, and two option records.
2. **Retry-on-stall would otherwise re-implement the cancel protocol** — its own marker, its own
   acknowledgment classification, its own requeue guards — duplicating code that lives one directory
   away. In-package, stall-cancel and operator cancel share one protocol.
3. **A "decoupled" design would be coupling by well-known names**: sharing the audit list's serialized
   format and parameter conventions across packages without sharing code is worse than a compile-time
   dependency — nothing enforces it.
4. **Precedent and cost**: schedule and parameter overrides were also folded into JobControl. The de
   facto boundary is "the governed operator control plane," which liveness belongs to.

**Extraction hedge:** liveness code lives in its own files, depending only on narrow internal seams
(the audit store, the shared cancel protocol, the shared requeue helper, the options record), so a
future split stays cheap if a real second consumer appears.

## The two-layer timeout model (R1 is not achievable by the toolkit alone)

The only mechanism that *actually* times out a long-running job is the **storage invisibility
timeout**, and it operates below anything a job filter can reach. Hangfire.PostgreSql defaults to a
fixed 30-minute `InvisibilityTimeout` with `UseSlidingInvisibilityTimeout = false`: the dequeue query
treats any job fetched longer ago than the timeout as abandoned and hands it to another worker while
the original body is still running — not a timeout but a **duplication**. SQL Server's
`SlidingInvisibilityTimeout` renews the fetch lease on a background timer — but that proves the
*worker process* is alive, not that the *job* is progressing.

R1 therefore decomposes into two layers, each necessary and neither sufficient:

| Layer | Signal | Answers | Owner |
|---|---|---|---|
| Storage sliding invisibility | automatic timer renewal | "is the worker process alive?" — keeps the job *owned*, prevents re-fetch/duplication | **Host configuration prerequisite** |
| JobControl heartbeat | explicit `Beat()` from the job body | "is the job making progress?" — detects hung-but-alive | this feature |

The host prerequisite is required, not optional: Postgres hosts set
`UseSlidingInvisibilityTimeout = true` (incompatible with `IsLightweightServer` — the renewing storage
process doesn't run there); SQL Server hosts set `SlidingInvisibilityTimeout`. The detector runs a
best-effort configuration check: a *positively* detected non-sliding configuration downgrades
`StallAction.Retry` to flag-only with a repeated error log; an inconclusive detection warns once.

> **Rejected: renew the invisibility deadline from `Beat()` instead of requiring sliding
> invisibility.** (1) *No sanctioned seam*: the invisibility deadline lives on the storage's queue
> record, owned by the worker's fetched-job handle — neither `PerformContext` nor `IStorageConnection`
> exposes it. Renewal would mean raw provider-specific SQL, and has no equivalent at all in SQL
> Server's transaction-based fetch mode, where invisibility is a held transaction, not a column.
> (2) *It replaces governed retry with ungoverned duplication*: beats stop → the storage silently
> re-fetches the job while the hung-but-alive body is still running — a double-run with no
> cancel-first, no acknowledgment, no retry budget, no audit entry. (3) *It breaks the flag-only
> default*: beats are body-driven and throttled, so a legitimately silent phase (a slow-but-fine long
> DB query) would hard-trigger a storage re-fetch — duplicating a healthy job — where today it earns a
> recoverable flag. Sliding invisibility already *is* the "heartbeat that renews invisibility,"
> correctly scoped to the question that layer owns; the job-progress signal stays advisory and feeds
> the governed response instead.

## The opt-in contract: an attribute, resolved at execution time

`[Heartbeat]` is a plain attribute (not a `JobFilterAttribute`) on the job method or its declaring
type, read via reflection from the executing job and cached per method. No attribute → no contract:
no storage is written, nothing is ever scanned, behavior is bit-for-bit today's (R3).

**Why an attribute and not registrar-declared configuration:** the filter resolves it at execution
time with zero storage lookups; it works for **every** host, including plain-`AddOrUpdate` hosts that
never adopt the registrar; and it automatically covers ad-hoc jobs, including manual-invoke runs,
since the attribute travels with the method.

**Validation never throws into the body.** Constructor arguments validate eagerly (a non-positive
timeout throws at attribute materialization — deterministic, at development time); the floor
`TimeoutSeconds ≥ 60` (twice the detector's default scan interval — below that, silence and a stall
are indistinguishable) plus `MaxRetries ≥ 0` and `RetryDelaySeconds ≥ 0` are enforced at contract
start, where failing means *run unmonitored + audit `contract-invalid`* — never a job failure.

**The contract is a snapshot.** Everything the detector needs — timeout, stall policy, retry budget,
delay — is written into the beat record at enrollment. The detector evaluates the values the
*executing version* enrolled with, never its own reflection over a possibly different deployed
assembly; this also makes detector scans reflection-free and rolling deploys safe.

## Beats

- **Beats come from the job body at meaningful progress points — never from a timer.** A timer-based
  auto-beater reproduces the process-alive flaw the two-layer model exists to close. A hung body
  *can't* call `Beat()`; that silence is the signal.
- **Write-throttled**: calls within `MinBeatInterval` (default 5s) of the last persisted beat are
  coalesced in memory. Job authors may beat per loop iteration without thinking about write volume.
- **Never throws into the body**: a storage failure inside `Beat()` is logged and swallowed — a
  transient blip must not fail a six-hour job.
- **Execution-scoped payload**: the record lives under `JobControl.Beat:{executionId}`, and the active
  index stores `{jobId}|{executionId}` tuples — so an overlapping zombie attempt (possible under
  storage-native invisibility recovery, with no unsafe requeue anywhere in this package) can only ever
  write its own records and retire its own tuple. The one deliberately last-writer-wins cell is the
  current-execution pointer, written only at contract start: a zombie never re-runs `OnPerforming`, so
  the latest contract start is by definition the current attempt.
- **`seq` is the stall signal**: a per-execution monotonic beat counter. "Unchanged" means an
  unchanged `seq`, so progress text or future payload fields can never perturb stall semantics.
- Calling `Beat()` from a job without the attribute is a no-op — it does not implicitly start a
  contract. *(Rejected: "first beat starts the contract" — it would make stall semantics depend on
  whether a code path happened to run.)*

## Detection

**The filter/detector split.** `LivenessFilter` (a server filter registered by `UseJobControl()`)
owns enrollment and retirement; `StallDetector` (an `IBackgroundProcess`) owns confirmation and
everything after. The detector's registration line — `AddJobControlStallDetector()` — exists because
of a Hangfire seam, not a product choice: `UseJobControl()` registers global *job filters*, and
Hangfire offers no way for a configuration extension to attach a background process to the server.
Registered ⇒ running; there is no separate enable flag, because an idle pass costs two empty set
reads.

**Fail-open with a loud signal.** A throwing liveness filter would fail — and, under
`[AutomaticRetry]`, retry-storm — a production job over a monitoring problem. So the filter never
throws: an invalid contract runs unmonitored and audits `contract-invalid`; an enrollment storage
failure runs unmonitored and audits `contract-init-failed`. An execution is either fully enrolled or
honestly unmonitored — never half-monitored.

**Stall confirmation runs on the detector's own monotonic clock.** The detector records when *it*
first observed the current `seq` and flags only when that `seq` has remained unchanged for the
contract's timeout of its own observation. Cross-server clock skew therefore affects display only; a
storage read failure resets the baseline (a blip can never mass-flag); a detector restart delays —
never suppresses — confirmation by at most one timeout plus one scan interval.

**Self-heal is scoped by execution identity.** A tuple whose job is no longer Processing *under that
execution* is removed (covering a worker crash where retirement never ran, a cancelled zombie's
leftover, and an execution superseded by storage-native re-fetch) — with one deliberate exception: a
stall-cancelled execution stays surfaced on the stalled index while its post-cancel workflow
(cancel-requested / blocked / exhausted) still governs the job's state.

**Detector health is a first-class signal.** Each server's detector renews a health lease only after
a *successful* pass, and the stalled endpoint reports `degraded` when no fresh lease exists — so "no
detector running" (or a persistently failing one) is visible, never mistaken for "no stalls".

**Concurrency.** Core job-parameter writes have no compare-and-set, so "guarded by marker presence"
alone would be a check-then-write race between detectors on different servers. Every stall **phase
transition** (flag, cancel-request, ack-evaluation, requeue, exhaust, supersede-cleanup) runs under a
narrowly-scoped per-job distributed lock with state re-read inside; lock acquisition uses a short
timeout, and failure skips the job for that scan — another detector owns it. Lock-free by design:
`Beat()` and contract retirement, whose writes are execution-scoped.

**A resumed beat recovers a flagged execution** (`stall-recovered`): a slow-but-fine job that got
flagged is recoverable — which is why flag is the default and anything automatic is opt-in. "Stalled"
is a marker plus surfacing, **never a custom `IState`** (that would fight the dashboard and the
monitoring API).

## Retry on stall: an acknowledgment-gated, durable state machine

Five rules govern the workflow; each closes a specific hazard.

**Rule 1 — never cancel a possibly-dead owner.** Hangfire server records are leases: the watchdog
removes them only after `ServerTimeout` (default five minutes), so *presence proves nothing recent* —
and cancelling a dead owner's job actively **breaks Hangfire's own crash recovery**: with sliding
invisibility, process death stops renewal, the row becomes visible, and another worker re-runs the job
Processing → Processing — *unless* the state is no longer Processing, in which case the recovered row
is forgotten. So: owner absent from the server list → flag only, defer to native recovery — the
re-fetched attempt enrolls a fresh contract and the old stall retires by identity. Crashed-worker
retry is thereby storage-native (R2's first half), unbounded by `MaxRetries` — documented baseline
behavior. Cancellation is issued only when **both** gates hold — hints that shrink, but cannot close,
the check-then-act window: *(a)* the owner's last server heartbeat is fresh, and *(b)* the stall has
outlived the host-declared `StorageLeaseWindow` (≥ the storage invisibility timeout; **required** to
enable retry, unset ⇒ downgrade to flag). If the owner had died, native recovery would already have
replaced the execution within that window — so a stall outliving it under fresh heartbeats implies
hung-but-alive: the only case cancellation is *for*.

**Rule 2 — the state transition is the cancellation's commit record.** An after-the-fact marker
write loses a fast abort: the body can reach its acknowledgment point before the marker exists. For a
retry *gate* that is fatal, so the protocol linearizes on the state itself: **(1)** the request marker
(`JobControl.CancelRequested`, carrying a request id and, when the job is enrolled, the execution id)
is written *before* the transition — prepared, proving nothing by itself; **(2)** the state changes
via `CancelledState`, whose `Name` is the plain string `"Deleted"` (built-in handlers, statistics, and
dashboard rendering match by name and are untouched) and whose own state data carries the request and
execution identity — **committed atomically with the transition**; **(3)** an acknowledgment is valid
only when the finishing execution's in-process identity matches both the prepared marker and the
committed state data; **(4)** a lost transition retires the prepared marker — no acknowledgment is
ever valid for a cancel that never won. The **operator cancel endpoint uses the same protocol** — one
cancellation protocol — which retired the pre-existing near-miss where a fast abort was recorded as
`abort-observed` instead of acknowledged. Markers written by a pre-0.5 binary (no request id, marker
written after a plain `DeletedState`) still acknowledge through a legacy path during a rolling
deploy. (Transactional job parameters would be the alternative commit channel, but they are
feature-gated per storage and must not be assumed; state data, a core capability, is the commit
record.)

**Rule 3 — the workflow is durable and driven by `retry-pending`, not the active index.** Self-heal
removes a Deleted job from the active index on the next scan, so the moment a cancel commits, the
workflow's driver becomes a `jobcontrol:liveness:retry-pending` tuple plus a per-execution
`JobControl.StallAttempt` phase record (phase, request id, attempt number, and the policy snapshot —
all immutable identifiers). Pending entries survive detector and server restarts and are removed only
on requeue, blocking, exhaustion, supersession, or confirmed terminal cleanup.

**Rule 4 — retry is acknowledgment-only; the unacknowledged terminal is a surfaced block, and the
ordinary requeue respects it.** A matching acknowledgment (`aborted`/`faulted`) → requeue through the
shared requeue helper (one implementation, so the endpoint and the detector cannot drift), counting
the attempt against `MaxRetries` (the count rides on the *job* — each retry runs as a fresh execution
id) and honoring `RetryDelaySeconds` via Hangfire's own `ScheduledState` (a restart can neither
double- nor re-time the delay). A `completed-anyway` acknowledgment ends the workflow — the body
finished its work; re-running it would duplicate a completed run. No matching acknowledgment within
`AckGracePeriod` — or the owner going absent/stale after the cancel committed — leaves the job
**Deleted**, audited and surfaced as `stall-retry-blocked`; **no queue row is ever added**, because
enqueueing does not touch the orphaned fetched lease, and a second row can race invisibility recovery
into a double-run. The ordinary requeue endpoint returns 409 for a job carrying an unacknowledged
committed stall-cancel in **either** pre-ack phase — `cancel-requested` or `blocked` (OPS-003: the
grace window is a wait for evidence, not evidence, so it is guarded identically) — keyed off the
workflow record and its committed identity, so non-liveness jobs are unaffected and a late
acknowledgment lifts the guard by itself. The break-glass exit from either phase is a distinct
**force-requeue** action: separate confirmation, required reason, audited as
`force-requeue-unacknowledged-stall` with the source phase, documented duplicate-side-effect and
state-clobber hazards, recommended process recycle first, identity-scoped retirement of the
superseded records (only the overridden request's marker is cleared). Detector, ordinary requeue, and
force-requeue all serialize their read/decision/transition/cleanup under the same per-job lock, and
force-requeue re-reads the acknowledgment inside it: `completed-anyway` arriving first makes the
force stale (refused, outcome surfaced); `aborted`/`faulted` downgrades it to an ordinary
acknowledged requeue.

**Rule 5 — single winner with `AutomaticRetry`.** The worker attempts its terminal transition only
from expected state Processing and deliberately ignores the result otherwise. If the stall-cancel
won, the `Failed` candidate never applies and `AutomaticRetry` never elects; if the body's failure won
first, the stall-cancel loses its expected-state guard and the detector treats the job as
**Superseded** — identity-scoped cleanup of its own pending data only, never a stall retry stacked on
top of an exception retry. `MaxRetries` and Hangfire's `RetryCount` are separate budgets on mutually
exclusive paths.

### The state machine

| Phase | Hangfire state | Liveness index | Transition rule |
|---|---|---|---|
| Active | `Processing` | `active` | Contract snapshot + start beat written at enrollment |
| Suspect | `Processing` | `active` | Same `seq` observed unchanged, detector-clock window running |
| Flagged | `Processing` | `active` + `stalled` | Marker/index/audit under the per-job lock |
| Cancel requested | `Deleted` (identity-bearing) | `retry-pending` + `stalled` | Rule 1 gates passed; Rule 2 protocol committed |
| Acknowledged | `Deleted` | `retry-pending` | Ack matches request + execution identity exactly |
| Retried | `Scheduled`/`Enqueued` | none (old tuples retired by identity) | Shared requeue helper; attempt counted on the job |
| Blocked | `Deleted` | `stalled` (surfaced) | No matching ack in grace, or owner absent/stale post-cancel — human-only exit |
| Exhausted | `Deleted` | `stalled` (surfaced) | Attempt number exceeds `MaxRetries`; final audit, no automatic requeue |
| Recovered | `Processing` | `active` | `seq` advanced before cancel committed — flag retired |
| Superseded | any other winner | none | Another Hangfire transition won; identity-scoped cleanup only |

`MaxRetries = 0` is a meaningful policy: cancel the confirmed-hung body, never requeue — "kill on
stall".

> **Rejected alternatives.** **(a) `RequeueWithoutAck`** — removed entirely, not gated: Hangfire
> completion transitions carry no execution fencing (only the state *name* Processing is checked), so
> a deliberately-overlapped zombie finishing can terminate the new attempt's state machine outright —
> a defect below anything marker correlation can repair. The execution-identity protocol is still
> mandatory (native overlap under a storage partition remains possible), but deliberate overlap is
> not supportable without provider-level fencing. **(b) Server-list absence as a requeue
> authorization** — absence is stale by up to `ServerTimeout` and proves nothing (Rule 1). **(c) A
> non-enqueue "restoration" path for dead-owner cancels** (re-arming the old lease by moving
> `Deleted` back toward `Processing`) — whether the orphaned queue row still exists is unobservable
> through core interfaces; restoration can strand a permanent Processing ghost.

## Honest limits

- **A stalled worker slot is never reclaimed.** Cancel + requeue frees the *job*; a truly hung body
  occupies its worker thread until process recycle (a Hangfire architecture limit — no filter-based
  feature can preempt). Mitigations, in order of value: a dedicated queue + separate server process
  for long-running jobs (hangs can't starve short jobs; recycling the long-job process is cheap),
  retry budgets, and alerting on the stalled count.
- **At-least-once, amplified.** Stall-retry adds a governed duplication path on top of Hangfire's
  baseline at-least-once semantics — and a storage partition can already natively overlap a zombie
  with its own re-fetched replacement, with no liveness feature involved. Jobs adopting retry must
  have **idempotent effects or resumable checkpoints** — a job-author contract the docs state, not
  something the feature can supply.
- **Forever-running jobs** are a distinct sub-case: every deploy aborts them (`ShutdownTimeout`
  defaults to 15s), so checkpoint-resume is mandatory, not advisory; recurring forever-jobs need
  concurrency protection or occurrences pile up behind the running one; and they legitimately sit in
  Processing for weeks — only their *beats*, never their runtime, may drive flagging.
- **Interplay with `[DisableConcurrentExecution]`**: a hung zombie still holds the distributed lock,
  so a retried run blocks or faults until the lock times out (Postgres default: 10 minutes). This
  will look like "the retry didn't work".

## Storage and internal seams

All state lives in Hangfire's own storage (no host schema), execution-scoped throughout. Job
parameters expire with the job, so none of this needs a cleanup process; the beat record is kept at
contract end so terminal views can show final progress.

| Key | Kind | Lifetime |
|---|---|---|
| `JobControl.Beat:{executionId}` | job parameter — contract snapshot + latest beat | expires with the job; kept at contract end |
| `JobControl.Liveness.Current` | job parameter — current execution pointer | written only at contract start |
| `JobControl.Stalled:{executionId}` | job parameter — stall flag + acknowledgment | expires with the job |
| `JobControl.CancelRequested` | job parameter — prepared cancel request (shared with operator cancels; request/execution identity since 0.5) | prepared before the transition; cleared on requeue or a lost transition |
| `JobControl.CancelAck:{executionId}:{requestId}` | job parameter — matched acknowledgment | expires with the job |
| `JobControl.StallAttempt:{executionId}` | job parameter — workflow phase + attempt number + policy snapshot | expires with the job; terminal phases persist as the workflow's record |
| `JobControl.StallRetryCount` | job parameter — job-scoped retry budget spent (each retry is a fresh execution id) | expires with the job |
| `jobcontrol:liveness:active` | set of `{jobId}\|{executionId}` tuples | exact-tuple removal at contract end; self-healed |
| `jobcontrol:liveness:stalled` | set of tuples | removed on recovery/terminal/self-heal, except the surfaced Deleted phases |
| `jobcontrol:liveness:retry-pending` | set of tuples | drives all post-cancel phases (Rule 3) |
| `jobcontrol:liveness:detector:{serverId}` (+ index set) | hash — per-server health lease | expiry-self-healed; freshness judged from the recorded last scan |

Internal seams the liveness files may touch: the audit store, the shared cancel protocol
(`CancelledState` + the request/acknowledgment stores, which the operator cancel path also uses), the
shared requeue helper, and the options record. Nothing else — that is the extraction hedge from the
packaging decision.

## Resolved design questions

1. **Non-sliding storage: warn or refuse retry?** Refuse — shaped as a **downgrade, never a crash**:
   positive detection ⇒ retry ignored, flag-only, error logged every scan cycle; inconclusive ⇒ warn
   once. Failing startup would trade a misconfiguration for a crash-loop, and the check is
   best-effort reflection — too weak a signal to hard-fail a host on.
2. **Why is detector registration opt-in at all?** A Hangfire seam, not a product choice: background
   processes must be handed to the server by the host. The single line is unavoidable; the design
   shrinks it to `AddJobControlStallDetector()` — registered ⇒ running, no separate enable flag.
3. **Native dashboard display?** The Runs UI for rich data, plus the one clean native seam that
   exists: the `UseDashboardMetric` stalled-count tile. Anything richer means replacing built-in
   pages wholesale — version-fragile, rejected.
4. **Why is there no global default timeout?** A global default would silently enroll nothing today,
   but would change the meaning of adding the attribute later — opt-in stays explicit and local (R3).
