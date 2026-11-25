# Durable allocation, inbox, SDK adapter and registry

This package implements the controller's local state, pinned SDK queue adapter,
automatic repository/scale-set registry, session manager, and JIT
registration/delivery broker. The daemon command composes these pieces but its
production platform factory remains deliberately unconfigured. It starts no
worker and changes no workflow selector. The approved API-probe
command remains separate. Local verification is not evidence of live scheduling,
isolation or GitHub broker semantics.

## Implemented invariants

- A private SQLite WAL journal uses FULL synchronization and a process allocation
  lock. Platform, App installation, total capacity and legacy reservations are
  durable identity; restart cannot silently change them. The journal is at
  version 15, and every upgrade step between a journal's version and this
  binary's must be declared in `journalUpgrades`: 12 -> 13 (the `assigned-stale`
  jobs.state value), 13 -> 14 (`repositories.archived` and
  `repositories.disabled`) and 14 -> 15 (the `refused_envelopes` table) all run
  in place, keeping live leases. A version-14 binary refuses a version-15
  journal, so rolling back needs a manual `PRAGMA user_version=14`, which is
  safe because version 14 never reads the new table. Any other
  version is refused at open with the recovery it needs — a newer one names the
  other binary, an older one says to move the state directory aside — because
  there is no data migration across the job-key re-key. Linux/macOS
  qualification ceilings remain six/two respectively.
- Complete discovery snapshots update repository eligibility atomically. Removed,
  archived and disabled repositories receive no new reservation. Known revocation
  between reservation and acquisition also prevents that API call. Existing work
  and cleanup obligations remain in the journal.
- Eligibility is private AND active, and the allowlist only narrows it further.
  An EMPTY `RepositoryAllowlist` means every private, active repository the App
  installation can see, so onboarding a repo takes no controller configuration
  change; a non-empty one pins the pool to exactly those repositories. Public,
  archived and disabled repositories are refused whatever the list says
  (ADR-0003), a foreign owner fails the whole discovery snapshot, and the
  scale-set client re-applies the private-only rule at the one point an
  Administration-write credential is built.
- The unbounded-onboarding risk the empty list used to carry is answered by two
  bounds rather than by refusal. Creation is eager — there is no webhook and the
  broker cannot offer a job to a scale set that does not exist yet — so
  `Reconcile` admits at most `createBudget` (5) NEW repositories to the creation
  path per cycle, and `Config.MaxRepositories` (default 80, ceiling 120, refused
  at Open outside that range) bounds GROWTH -- how many repositories may hold a
  scale set, and so how many queue sessions one cycle polls. The default sits
  above the account's private-repository count on purpose: a cap below the
  eligible set refuses repositories forever with no symptom but a queued job. A
  repository already managed or already bound to a scale set is never held back
  by either bound: it may be mid-flight with an uncertain creation outcome. The
  per-cycle line reports `creating=`, `create_deferred=`, `over_cap=` with the
  first refused repository's ID, and `orphaned=`, while any is non-zero.
- A repository that LOSES eligibility and still owns a scale set has its row
  marked `remove-intent` and is REPORTED -- `orphaned=<n>
  orphaned_first=<owner/repo>` on the cycle line -- and nothing else. The
  controller cannot delete a remote scale set at all: `scaleSetAPI` has no
  delete method. That is the decision, not an omission. The call is
  irreversible, its evidence would be one discovery snapshot in which absence
  and a refused read are the same signal, and performing it for a repository
  that just went public means wielding Administration:write on a public
  repository. The client is dropped the moment eligibility is lost, so nothing
  retains that credential either. The leftover is cosmetic; the operator clears
  it (`hosts/ms-a2/github-runner-pool/README.md`).
- `repositories` persists `archived` and `disabled` (journal 14; `journalUpgrades`
  is the explicit map of source version to step, and 12 -> 13 belongs to
  homelab#895 -- a no-op stamp on this branch). They are not
  read by eligibility -- discovery already applied it -- but by
  `githubapp.ScaleSetClient`, which re-applies the private-and-active rule to
  the repository the JOURNAL reconstructs. Before they were columns, both
  arrived false and that arm of the choke point could not fire.
- `Registry.Run` refreshes the explicitly selected or all-repositories App
  installation immediately and every minute. A separate management loop
  reconciles up to four repositories concurrently, using cached
  repository-scoped SDK clients. Slow readback does
  not delay discovery. Failed discovery retains the last complete snapshot for
  diagnostics and fences new work until a successful refresh; silence expires
  admission after three minutes. Cleanup of existing leases remains possible.
- Each newly eligible repository receives a persisted random scale-set name and
  the explicit stable platform label. Create intent precedes POST; response loss
  is recovered by finding that exact name and verifying ID readback. An absent
  uncertain create never triggers another POST. Foreign label/name collisions,
  changed IDs, dropped/extra labels and enabled runner self-updates prevent
  activation. GitHub may add the known resource name as a system label. There is
  no label fallback and no adoption of a binding without an ownership record.
- Scale-set readback failures fence new allocation/acquisition while retaining
  workers and their identities. Successful readback of the same ID restores the
  fence. The registry has no update/delete API: it cannot repair a collision by
  overwriting another controller or delete resources after losing an App grant.
  `Repositories` and `DiscoveryStatus` expose discovered identities, eligibility,
  observation times and safe diagnostics for the orchestrator and monitoring.
- `ReserveNext` chooses the next repository with an offered request, then its
  oldest offer, and reserves a physical slot in the same transaction. The fairness
  cursor survives restart. One repository can consume the whole available cap;
  other waiting repositories receive the next freed slot in rotation.
- `PlanAdvertisements` emits one complete polling sweep for every active,
  verified session, including repositories with zero workers. It preserves every
  charged lease first, then hands the remaining admission to repositories with
  unmet demand, one slot per repository per pass in durable round-robin order. A
  sweep with admission still free after that offers one exploratory slot to the
  next repository in rotation, because GitHub assigns a job only to a scale set
  that advertised capacity. This prevents repeated polls from promising the same
  free slot while allowing a lone busy repository to grow to the full cap over
  successive sweeps.
- **Demand is journaled job rows raised by a fresh statistic, and the statistic
  covers `TotalAssignedJobs` PLUS `TotalAvailableJobs`.** A repository this pool
  has never advertised capacity for has no journaled rows and no assigned jobs,
  because GitHub's broker assigns work only to a scale set that advertised — so
  its queued jobs exist solely as `TotalAvailableJobs` in the poll envelope.
  Reading assigned jobs alone therefore scored it idle, and its only route to a
  slot was the one rotating exploratory slot: three repositories each held a job
  queued for 45+ minutes across 48 subscriptions while the pool logged
  `admission=2 free=2 charged=0` (2026-09-23). **An available job is GitHub's,
  not ours.** It raises what a sweep ADVERTISES and is never charged a slot;
  occupancy still comes from the journal's leases alone, `committed` still
  cannot exceed `usable`, and the fairness pass still hands out one slot per
  repository per pass, so several repositories with available work share the
  admission instead of the first one taking all of it.
- **Demand and admission are different things, and only one of them is an
  invariant.** Assigned jobs are GitHub's view: its broker assigns work to every
  allowlisted repository at once, so a shared pool is routinely asked for more
  than it admits. That overflow is planned around, never refused — the plan
  serves what fits and reports the rest as `Deferred`, which the cycle line
  prints as ` deferred=<n>` when it is non-zero. Treating it as an invariant
  violation failed the whole cycle before polling and provisioning ran, so two
  repositories each holding one assigned job against an admission of one left
  both jobs queued for 30+ minutes with the pool idle (2026-09-22). Journal
  occupancy above admission stays an error: that means admission shrank
  underneath running workers, which no plan can describe.
- A job is identified by a **job key**: the decimal runner request ID when GitHub
  sends one, and `job:` plus the string job ID when it does not. Both of GitHub's
  delivery models work: `JobAvailable` then `AcquireJobs` (GHES), and a lone
  `JobAssigned` with `runnerRequestId` 0 that the broker has already bound to this
  scale set (github.com). An assigned job is reserved ahead of every queued one,
  because GitHub times an unclaimed assignment out; that outranks the round-robin
  repository cursor, which is bounded because `PlanAdvertisements` never promises
  more than the free admission, so jumping the queue cannot overcommit a slot.
  Job identity is resolved against the journal, not from one event: a job may
  change shape between events, and a per-event key would split it across two rows
  and strand its lease. Two durable rows matching one event refuse the envelope.
- `AcquireReserved` commits its intent before calling the pinned client's
  `AcquireJobs` interface with exactly one request ID. Any lost response, invalid
  returned ID, duplicate result or canceled request retains the slot as uncertain.
  Reinvoking that operation never retries the POST. The client must have mutation
  transport retries disabled, as the existing App-client factory does. A
  broker-assigned job has no request to acquire, so that call is skipped and the
  evidence is recorded under the `broker-assignment` source -- the controller's
  own inference, deliberately distinct from GitHub's acquire response.
- Every phase occupies its slot: reserved, acquisition intent/unknown, acquired,
  provisioning, running, cleanup pending and quarantined. A missing worker never
  frees a slot. Cleanup requires independent positive evidence for worker/storage
  absence, runner-registration absence and settled acquisition. Timeouts or
  aggregate job counts alone are not sufficient evidence.
- Queue start/completion evidence records the request lease and the exact owned
  runner lease separately, because GitHub may execute a request on another idle
  worker in the same set. It advances cleanup for the observed worker only;
  an unknown request with no own worker intent remains fenced for live/operator
  reconciliation even after its request event is observed.
- Each process starts with allocation/provisioning fenced until a complete owned
  worker inventory is reconciled. Untracked, duplicated or previously released
  workers keep the fence closed. The backend must produce the inventory; worker
  self-report is not sufficient. An uncertain recorded operation remains charged
  after reconciliation even when no worker is found.
- `WorkerBackend` defines one shared Kubernetes/Tart boundary. Its inventory must
  cover VM processes and writable storage, bind every observed worker to the
  lease's preselected resource name, and report how many additional workers fit
  current CPU, memory, disk and overhead. Incomplete, duplicate, renamed, unknown
  or pre-provisioning resources close admission. Trusted free headroom constrains
  both advertisements and reservations while the qualification ceiling and
  legacy reservation remain hard upper bounds. Create is a single-attempt
  operation whose uncertain result must be reconciled by inventory. Delete is
  exact and idempotent, and is re-sent after a backoff while a complete inventory
  still lists the worker (homelab#1094).
- Journal version 7 records each worker's preselected resource identity, create
  intent, ten-minute startup deadline, delete intent and two-minute cleanup
  deadline. `BeginWorkerCreate` requires a current eligible managed set plus the
  JIT broker's ready transfer and keeps the one-use capability/origin out of
  SQLite. Direct or inventory create confirmation must match the exact identity.
  Delete intent precedes the adapter call. After an unknown result the same exact
  delete is re-sent once `redeleteBackoff` has passed since the last attempt,
  which is stamped before each call (homelab#1094). Only a complete trusted observation of both process and writable-storage
  absence records the backend half of cleanup; release still requires independent
  runner-registration absence and acquisition settlement.
- Journal version 8 records provisioning failures and the platform quarantine
  deadline. Startup and cleanup timeouts become durable charged quarantine state;
  three provisioning failures in fifteen minutes suppress new reservations for
  fifteen minutes while trusted inventory and cleanup remain available. A timeout
  is never treated as worker, storage, registration, or acquisition absence.
- `PoolLifecycle` connects reserve/acquire, JIT preparation, the durable worker
  intents and `WorkerBackend` without introducing a platform-specific path. Each
  create/delete call occurs once after its intent. A complete later inventory
  confirms a lost create response or process-plus-storage absence after a lost
  delete response. Inventory failure and any post-list reconciliation failure
  close admission. Registration cleanup starts only after durable physical
  absence, and the lease releases only when acquisition is independently settled.
- `Daemon.Cycle` supplies the bounded production ordering: complete discovery,
  owned-set reconciliation, trusted backend inventory, deadline/quarantine
  maintenance, exact session sync, one complete fair poll, then at most the
  platform qualification limit of provision attempts. Maintenance gives each
  newly quarantined resource one durable delete attempt and removes its exact
  runner registration only after process-and-storage absence. A JIT failure
  before the backend call is quarantined and cleaned as a registration-only
  obligation because the durable ordering proves no worker create was invoked.
  Removed repository sessions remain durable and unpolled while healthy
  repositories continue a complete sweep. The controller retains each lease
  until acquisition settlement. A global discovery, inventory, maintenance,
  session or poll failure stops later side effects. A repository-specific
  managed-set failure remains visible but does not stop other repositories
  already verified active.
- `ParseEnvelope` preserves event ordering across types, rejects unknown types
  and retains request/assignment identities. It omits `AcquireJobURL`, which the
  SDK does not use to acquire jobs, so token-bearing URLs do not enter the journal.
  `Receive` commits the sanitized ordered envelope before DELETE acknowledgment;
  failed persistence never calls DELETE, and failed DELETE keeps the inbox pending.
  The one exception is in `QueueSession.Poll`: a message refused the same
  deterministic way three times (invalid envelope, foreign repository, changed
  redelivery, or a job-identity conflict) is acknowledged unapplied and recorded
  in `refused_envelopes` with its identity, a fixed reason word, its event
  count, its event type names and timestamps, never its payload. The whole
  envelope is dropped, sibling events included. A changed redelivery's
  earlier `messages` row is acknowledged before the DELETE, so a crash after the
  DELETE cannot pin the cursor. Transient failures (SQL, a missing scale-set
  row, runner settlement, encoding) never count. Two residuals: a body with no
  valid `messageId` cannot be counted and still loops, and rows for dead
  sessions are never pruned.
- Message identity is repository + scale-set incarnation + session + envelope ID.
  Request identity is scoped by repository and scale-set incarnation. Workflow
  JobID is not globally terminal: canceled assignments may be reoffered. Exact
  envelope redelivery is idempotent; conflicting contents fail without ACK.
  Cursors do not skip an earlier unacknowledged envelope and are not transferred
  to a replacement session with unproved semantics.
- `QueueSession.Poll` captures the original response through the SDK's supported
  HTTP hook, commits its ordered sanitized envelope before SDK decoding, then
  acknowledges. A typed SDK result without its matching wire receipt fails.
  Original read/close failures and oversized responses remain failures. The
  database uses the caller's context: closing a timed HTTP body can cancel the
  transport's request context even after a successful response.
- Session creation and close intents commit before their API calls. One open or
  uncertain session per repository/set prevents duplicate creation across restart.
  Initial demand statistics commit with activation; refresh observations survive
  empty HTTP 202 polls too. Session UUID, owner, set and queue routing must remain
  unchanged during token refresh. Validation precedes SDK decoding, preventing its
  built-in 401 retry from using a foreign session. Credentials remain transient.
- Journal version 5 added only the SHA-256 fingerprint of the session queue URL;
  version 6 adds the independent advertisement cursor.
  After a complete physical inventory, `ResumeQueueSession` uses the in-repo
  fork's additive constructor to PATCH that exact UUID, validates owner/set/UUID/
  routing before publishing the native handle, and replays pending ACKs in the
  original session cursor. Missing metadata, truncated/lost responses, foreign
  routing, and later uncertain refreshes permanently fence the handle without a
  replacement POST. The queue URL and refreshed token remain memory-only.
- The public constructor builds its SDK client from the journal's repository and
  App installation. Acquisition accepts only a lease belonging to that session.
  Explicit zero transport retries and ordinary redirect errors prevent uncertain
  POST replay. The one exception is deliberate and bounded: a create-intent idle
  for `createIntentRetryAfter` (1 h) gets one fresh create under its own owner,
  restamped first. Only a 201 ends it; a 409 or a lost answer keeps the intent
  and waits another full window, so a held session costs one POST per hour
  (homelab#1235).
  Validated 401 refresh is retained. Polling and acquisition can run
  concurrently; only one poll advances a session's cursor at a time.
- `SessionManager.Sync` requires complete physical inventory before any session
  API call. It resumes every durable active identity, creates only a missing
  session for a currently eligible and API-verified managed set, and refuses the
  complete sweep if any create/close intent, removed set, stale process handle or
  partial handle set remains. `PollSweep` feeds all exact handles to the durable
  advertisement planner and polls every repository, including zero-capacity
  turns, concurrently so one long poll cannot delay another repository or the
  minute discovery cadence. `AcquireNext` reserves the physical slot first, then
  acquires through the selected repository's exact session. Network calls do not
  hold the manager mutex, so a long poll does not serialize acquisition.
- Session close requires no live leases, pending acknowledgments or observed
  acquired/assigned/running jobs. Close intent invalidates old offers and fences
  allocation atomically. An uncertain close is not replaced, and is replayed
  only as one fresh DELETE of the journaled UUID per `closeIntentRetryAfter`
  (1 h); a 204 or that DELETE's own 404 records it closed (homelab#1167). These
  local guards do not establish GitHub's session recreation/draining guarantees.

The `jobs` table is an acquisition-offer queue, **not** a complete GitHub lifecycle
mirror or a desired-worker counter. Envelopes may contain only 50 events from a
larger backlog. Aggregate statistics are persisted independently of offer count.

**`TotalAssignedJobs` and `TotalAvailableJobs` are floor-raisers, never the
demand itself, and they can freeze.** `scale_sets.statistics` is written on
session CREATE, on a refresh that carries statistics, and on a poll envelope —
and github.com's refresh carries none, so after a resume both numbers are stuck
at whatever the previous process last saw. Planning strictly by them advertised
capacity 0 to a repository with real queued jobs for 100 minutes (2026-09-22).
Demand therefore starts from journaled job rows with no live lease, and the two
statistics raise that floor together only when `classifyStatistics` calls the
observation **current** — which needs **both** bounds:

| Verdict | Meaning | Planned from |
|---|---|---|
| `current` | after this process started AND within `staleAssignmentWindow` | yes |
| `observed-before-this-process-started` | inherited from the previous process | no |
| `older-than-the-staleness-window` | this process's own, but too old | no |
| `observation-instant-missing-or-unparsable` | no usable instant | no |

**The upper bound is not decoration.** A poll that finds nothing returns HTTP 202
with no envelope, so `RecordEnvelope` never runs and nothing is rewritten. With
only a lower bound an idle repository stops re-observing and a count that has
since gone to zero is believed for the life of the process — an `available=1`
never re-observed keeps claiming a turn on alternate sweeps forever. The window
is the same five minutes an unconfirmed broker assignment gets, for the same
reason: it is this pool's bound on how long an unproven claim may hold a
resource. A repository with live work re-observes well inside it, because
`RecordEnvelope` rewrites `statistics` and `observed` for every accepted
envelope — including one carrying no events.

A refused statistic is reported with an INFO naming both counts and the reason,
but only when it would actually have raised the floor; a refused zero changes
nothing and logging it every sweep would bury the case that matters. Whatever
reads these numbers must still be combined with existing
reserved/booting/running/cleanup capacity.

**Reported demand that no envelope names gets one probe worker (homelab#1216).** Statistics and the REST read only raise what a poll advertises, so before this change a worker could start only after an envelope. On 2026-10-01 two queued jobs waited 2h16m with every poll answering 202. `Store.ProbeUnseenDemand` runs in each cycle just before provisioning, over the sets whose poll answered an empty 202. A set's unseen demand is a current `TotalAssignedJobs` less everything held for it (offered rows, starting and running leases), or the fresh REST queued count less offered rows and starting leases, whichever is larger. When that stays above zero for `unseenDemandAfter` (3 minutes) with no envelope from the set, it journals one probe job row (`unseen:<uuid>`, no request ID, no job ID). The ordinary reservation then registers a JIT runner on that scale set and starts a worker, within admission and the breaker. Its acquisition is recorded under the `unseen-demand` source, never as a broker assignment. Bounds: one probe per set at a time, at most one per `unseenProbeRetry` (10 minutes), none within 3 minutes of process start, and none after `unseenProbeLimit` (3) probes with no envelope from the set in between (one WARN per process; the count is read from the journal, so it survives restarts and resets only on the set's next envelope, homelab#1219). An unreserved or stale probe row is retired to `unavailable` at `Open`. A probe and its lease never count as demand served when the unseen count is read, so the clock does not restart under its own probe. The REST read counts only jobs whose labels are exactly the pool label, the rule the queue janitor uses: a job asking for `[slipway-linux, gpu]` can never run here. A job blocked on `needs:` is not counted either, because GitHub creates it only when its predecessor finishes. If GitHub hands the runner a job, the JobStarted and JobCompleted envelopes advance the probe's lease by runner identity, as they do for a job GitHub moved to another worker. If not, the runner exits and is retired. Whether GitHub offers a job it never announced to a runner registered this way is not yet proven live.

**A broker-assigned job is not trusted across a restart.** `Open` demotes every
`assigned` row with `runnerRequestId` 0 to `assigned-stale`, which `ReserveNext`
will not select, because `AcquireReserved` certifies such a job from the
controller's own inference rather than a GitHub answer. No per-job confirmation
API exists: the only signal is a fresh envelope naming the same job — a broker
reassignment retry, or its completion — and both outrank `assigned-stale`.

A worker can execute a different
acquired request from the one that initially motivated its creation. Reconcile
completion by the observed runner identity, not by assuming a JIT registration
was bound to a job key. Several broker-assigned jobs in flight at once all carry
`runnerRequestId` 0, so only the job key distinguishes them.

Finished workers are reaped on two paths, and only one runs lifecycle work at a
time (homelab#1190). `Daemon.Cycle` holds the daemon's lifecycle lock for its
whole run except `PollSweep`, the broker long-poll of about 50 s. `Daemon.Reap`
runs every `-reap-interval` (default 10 s) and only tries the lifecycle lock, so
it works only while the cycle waits on the poll or between cycles, and never
queues behind the cycle. Each pass's maintenance has a 90 s deadline, and a pass refills at most 4 slots. The deadline never cancels a started refill, since a cancelled Create writes a provisioning-failure row toward the pool-wide breaker; a refill runs under the daemon's context bounded by the worker start timeout, and none starts with under 30 s of the pass left. Reap runs the trusted inventory and the worker half of
maintenance, then provisions from demand already journaled, but only while
`refillAllowed` says the cycle has passed every boundary that stops
provisioning. Its GitHub calls are the registration cleanup, at most once per
`registrationGap` (1 minute) across both paths and never during a discovery
outage, plus the acquire and JIT registration for each slot it refills. Before this, a finished
worker's slot stayed held until the next cycle's `Maintain`, a median 80 s.

Both paths provision through `Daemon.provision`, which runs up to
`-create-concurrency` worker creations at once (default 1, the serial loop;
the MS-A2 runs 4). Each create spends about 4 s on the GitHub acquire, the JIT
registration and a Kata boot, so a serial nine-job burst left its last job
waiting 53 s behind the first eight. The first create of every pass runs
alone, so a broken backend still costs one provisioning failure per pass, not
four. After it, at most 2 run at once whatever the flag says (`maxFanOut`, one
under the breaker threshold of 3), and no more than the backend's free slots,
so a fault that starts mid-pass cannot trip the breaker by itself. A create cut
short by shutdown is quarantined and cleaned up like a failed one but writes no
failure row, so a restart during a burst does not hand the next pod a tripped
breaker. The fan-out adds no second writer: creates run inside the lifecycle
lock, beside each other only. Slot admission stays atomic in the Store
(`ReserveNextExcept` holds the Store mutex), and the Kubernetes backend counts
creates its own inventory read may have missed against headroom, so two
creates cannot both take the last slot (`Backend.unlistedCreates`). macOS
refuses any value but 1, because the tart backend has no such count.

## Changing capacity or legacy slots

The `platform` row holds four persisted fields. Two are **identity** and two are
**sizing**, and they behave differently on a restart whose configuration differs:

- `name` and `installation` are immutable. A mismatch refuses, naming the field
  and both values, because the journal belongs to another pool instance.
- `capacity` and `legacy_slots` are reconfigurable, but only onto a **quiescent**
  journal: no lease outside the terminal `released` phase, no `worker_resources`
  row outside the terminal `absent` phase, and therefore no slot index held. A
  lowering never truncates a held slot.

There is no reconcile flag and no reconcile mode. Adoption happens in `Open` and
nowhere else, inside the same migration transaction, so anything short of a clean
adoption leaves the row exactly as it was.

### Adopt now, or defer and restart

**A busy journal never refuses to open.** Refusing would deadlock the pool: the change arrives as a restart, so a job running at that moment leaves a non-released lease, the process would exit, the supervisor would restart it, and with no controller alive nothing drives that lease to `released` — `FinishCleanup` is its only writer. The journal could never become idle again.

So there are two paths.

**Idle at start — adopt.** The row is updated and one INFO line names it:

```text
Reconciled platform sizing on an idle journal. Changes: capacity 1 -> 2
  platform=linux journal_capacity=1 configured_capacity=2
  journal_legacy_slots=0 configured_legacy_slots=0
```

**Busy at start — defer.** The process opens and runs on the **journal's** sizing, not the flags'. Both directions are safe: a pending raise keeps the old, smaller ceiling; a pending lowering keeps the old, larger one, which the live leases were already allocated against. One WARN names requested versus effective sizing and what is still active, by count and phase:

```text
Deferred a platform sizing change because the pool is not idle.
  Requested: capacity 1 -> 2. Effective sizing is the journal's until the pool
  drains. Active: 1 lease(s) in phase reserved; slot(s) held: 0
```

While a change is pending, every per-cycle line carries it, appended after the existing fields so the order an operator or a log query already reads is untouched:

```text
runner-pool cycle: ready=true admission=1 … messages=0 last_envelope_age=4m12s pending_capacity=2 pending_legacy_slots=0
```

Each cycle re-checks quiescence, after `Maintain` (which is what drives finished leases to `released`) and before any session or provisioning work. Once the pool is idle the controller latches itself out of new work — `ReserveNext` returns `ErrCapacity` — and exits cleanly:

```text
runner-pool cycle: ready=true admission=1 … messages=0 last_envelope_age=4m12s pending_capacity=2 pending_legacy_slots=0 draining=true
runner-pool sizing change pending and journal idle; restarting to adopt pending_capacity=2 pending_legacy_slots=0
```

`draining=true` marks that last cycle because it stops before polling, so it reads `polled=0 advertised=none` — byte for byte the signature of the silent outage the cycle line exists to reveal.

The supervisor restarts it, and that start takes the idle path above. The quiescence read and the latch happen under the same mutex `ReserveNext` takes, so no slot can be handed out in between.

**The drain predicate is stricter than adoption's.** It also waits for the offer queue to empty: a broker-assigned job already journaled but not yet reserved survives the restart, but nothing claims it for the restart window (launchd's `ThrottleInterval` is 60s), and GitHub retries an unclaimed assignment only about three times. So a busy queue postpones the restart, and the WARN's activity list says so (`N offered job(s) not yet reserved`). Adoption at `Open` does not wait on the queue — an unclaimed job is not state sized by the old numbers.

**A journal whose own sizing exceeds the platform ceiling refuses to open.** On a deferral the journal's pair becomes the effective sizing, so it is held to the same ceiling the flags are. A journal row is not trusted input — it was written by whatever binary and flags ran last, and the inert manifest bundle still carries `-capacity=6`. The recovery is in the message: move the state directory aside.

An unchanged restart writes nothing and logs nothing.

The platform ceilings in `cmd/runner-pool/runtime.go` (Linux 2, macOS 2) are unchanged and still validate first, so a reconfigure can never exceed them. The backend is built from the store's **effective** sizing (`PlatformSlots`), not the flags, so a deferred change cannot leave the controller and the Kubernetes `AdmissionBudget` disagreeing.

**It takes a restart, which means a deploy — and possibly a second, automatic one if the pool was busy:**

- Linux: change `-capacity` in `hosts/ms-a2/github-runner-pool/deployment.yaml`,
  merged through a PR so Flux reconciles it. `verify-manifests.py` pins the
  expected value per bundle mode, so that assertion moves in the same PR.
- macOS: change `-legacy-slots` in
  `hosts/m1max/runner-pool/lol.sjo.runner-pool.plist` and re-run `install.sh` on
  the host.

**`-legacy-slots 1 → 0` alone does NOT give a second concurrent macOS VM.** It only raises the *controller's* pool slots from 1 to 2. The tart backend also caps how many pool VMs it owns, with the darwin daemon flag `-max-owned-vms` (default `1`, ADR-0018): `Create` refuses with `owned-vm-cap` once that many other VMs are in its ledger, and `FreeSlots` is clamped to the cap's headroom once any VM is owned. Admission is `min(pool slots, backend free)`, so a second VM needs `-max-owned-vms 2` and `-legacy-slots 0` together. `-max-owned-vms 2` runs at `1`, with a warning, while the effective `-legacy-slots` leaves fewer than two pool slots (a refusal would restart-loop while the store defers the sizing), and neither may be set until `ci-guest` and its 04:30 reprovision are stopped (plan `docs/plans/2026-09-30-m1max-second-slot.md`, Steps 4 then 5).

## Verification

From `services/runner-pool`:

```sh
GOCACHE=/private/tmp/slipway-go-cache make check test build
```

Tests use real temporary SQLite databases and the race detector. They exercise
50 simultaneous reservation attempts against six slots, a lone repository using
the full cap, two repositories sharing one slot across restart, lost acquisition
responses, actual SQLite write rejection before API side effects, failed ACK and
redelivery, conflicting messages, ordered cancel/reoffer, credential omission,
revocation, unknown inventory, legacy occupancy, and incomplete cleanup. These
are local fault tests, not the ten-job live concurrency acceptance.

Actual SDK tests use local TLS servers and generated App keys. They exercise
create/close intent ordering, response loss and restart fencing, wire order,
SQLite write failures, unknown events, malformed/truncated/oversized responses,
missing-hook refusal, lost ACK redelivery, 401 refresh for poll/ACK/acquisition,
changed session identities, missing/mismatched acquisition counts, redirect and
503 refusal, independent long polling/acquisition, capacity/scope checks, initial
and refreshed demand after idle restart, exact-session resume, pending-ACK replay,
lost/truncated PATCH responses, a failing native-refresh negative control, and
absence of credentials/routing URLs in SQLite. Advertisement tests rotate one
free slot across complete multi-repository sweeps, poll zero-allocation sessions,
preserve charged/assigned work, and reject omitted sessions or obligations beyond
the cap. Additive journal migrations preserve leases and sessions, retain the
restart inventory fence, and require fresh discovery after upgrading.

Registry fixtures cover adding an App grant without repository configuration,
public/private repositories, archives, complete removal versus failed discovery,
lost create responses with and without a remote resource, post-create write
failure, label/identity drift, foreign collisions, revocation/rename races, cached
client replacement after rename, installation mismatch, tick/cancellation behavior,
and discovery while management is blocked. An actual SDK/TLS fixture proves the
explicit-label request construction; it does not prove GitHub workflow routing.
Actual App-client SDK tests reject malformed list counts before they can become
false absence or an out-of-range index panic in upstream lookup.

## Next integration work

1. Implement the concrete Kubernetes API/CRI/storage transport and worker image,
   then register that reviewed backend with the composed daemon. Creation lost
   before its UUID was committed, uncertain close, and unknown acquisition remain
   fenced rather than guessed. Live GitHub PATCH/session ownership and process
   fencing remain explicit pilot gates.
2. Verify the random ownership name/stable-label combination
   through live App API readback and an unchanged workflow selector. Validate
   current statistics, the implemented capacity rotation (including zero
   allocation), and request identity across canceled/reassigned attempts in a
   separately approved live queue canary. Aggregate statistics cannot identify
   which particular uncertain acquisition succeeded. Concurrent poll/refresh
   statistics are observations in local receipt order, not broker causal proofs.
3. Implement the Tart adapter against `WorkerBackend`, including GUI-session,
   legacy-slot, image-bake and reboot recovery. Both platform transports must
   supply physical headroom and absence rather than controller-database state.
4. Add operator recovery for permanently uncertain mutations and alert/diagnostic
   sinks. The lifecycle already retires a bootstrapped worker that has emitted no
   job-start event for five minutes, destroys a completed worker immediately, and
   releases either slot only after durable backend absence, registration absence
   and acquisition settlement. Only the trusted reconciler may supply cleanup
   evidence.

See the [queue protocol research](../../research/queue-recovery.md) for source
evidence and live gates. No storage or network deployment authority is added by
this package.

## JIT broker and worker launcher

`NewJITBroker` builds repository-bound clients from the journal and selected App
installation. `Prepare` accepts an acquired lease, atomically charges provisioning
and persists a random bootstrap-token digest, confirms the unique runner name was
absent, then journals intent before one native JIT POST. Only the known runner ID,
phases and safe diagnostics are durable. A lost/invalid response retains its intent;
an empty lookup cannot settle a potentially late creation. Restart never downloads
or generates another payload for that lease.

The worker handler requires HTTPS and a per-lease bearer capability. A SQLite
compare-and-set consumes it before bytes are written; concurrent/replayed requests,
wrong scope, canceled requests and failed writes cannot deliver again. A successful
write records transport progress, not runner execution. `Expire` forgets unused
payloads after the startup deadline; the lifecycle tick must call it and reconcile
cleanup. `CleanupRegistration` revokes delivery and requires trusted physical
worker absence before removing only an exact observed runner identity. Lost DELETE
responses use readback, never replay. It does not release the lease or settle its
acquired job. Cleanup failure remains visible and charged.

`cmd/runner-pool-worker` uses the [bootstrap package](../bootstrap) inside a fresh
VM. It verifies TLS certificates, refuses redirects/retries/proxy routing, fetches
a bounded opaque payload, and executes the native listener using its supported
JIT environment input. It rejects root and preexisting `.env`/registration files,
keeps JIT out of argv, removes bootstrap environment and filters inherited variables.
Only encoded payloads up to 64KiB and total exec inputs up to 96KiB are accepted.
The native runner writes its own credential files inside the VM; destruction and
image sanitation remain necessary isolation controls.

Journal version 4 added transfer metadata; later additive migrations preserve it.
Tests exercise the actual SDK POST/DELETE path, durable write failures,
late/lost creation, runner identity collisions, restart, partial delivery, concurrent
replay, expiration, wrong TLS/scope, journal credential omission, a maximum-sized
native exec and the actual worker main entrypoint with a fixture listener.

The cross-platform endpoint, TLS, injection, server-bound and shutdown requirements
are fixed in the [worker bootstrap contract](../bootstrap/CONTRACT.md), and its
dedicated bounded TLS listener is implemented in the bootstrap package. Production
daemon listener configuration and narrowly scoped network exceptions are not wired.
Worker images/backend injection and trusted physical observations remain platform
work. GitHub JIT credential replay before first use and after deletion/job
completion must still be tested live; one-use bootstrap is a separate control.
See [JIT protocol evidence](../../research/jit-lifecycle.md).
