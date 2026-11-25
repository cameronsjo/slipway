# Operations and recovery

These are the traps met while running the pool and moving real repositories onto it. Several are GitHub Actions behavior that only matters once your own CI depends on a small pool.

## Read the cycle line first

The controller logs one line per cycle:

```text
runner-pool cycle: ready=true admission=2 free=1 reason=slots charged=1 polled=48 advertised=<repo>/<set>=<n>,... messages=0 polls=202:48,200:0,401:0,other:0,refused:0,quarantined:0 last_envelope_age=31s deferred=4
```

| Field | Meaning |
|---|---|
| `charged` | Leases not yet released. It should match running workers. A non-zero `charged` with no worker pods means a stuck lease ([Capacity and scheduling](capacity-and-scheduling.md)). |
| `advertised` | What each scale set offered GitHub this sweep. A repository with queued jobs at `=0` sweep after sweep is starving. |
| `polls=202:N` | Empty long-polls. That is normal when nothing is advertised to repositories with work. |
| `polls=…,refused:N` | Envelopes the journal itself declined to commit (a repository mismatch, a conflicting redelivery). A refused envelope is not acknowledged, so GitHub still owes it, and it never counts as a heard poll. The same message refused the same way three times is quarantined instead (next row); a transient storage failure never counts toward that. Non-zero here for more than one cycle is worth reading the log line for which repository. |
| `polls=…,quarantined:N` | Envelopes refused the same way three times and then acknowledged without being applied, so one bad message cannot block its session forever. Every event in the envelope is lost with it ([A quarantine drops the whole envelope](#a-quarantine-drops-the-whole-envelope)). Each one logs a WARN (`Quarantined a queue envelope refused N times`) with repository, scale set, message ID, a reason word, the event count and a count per event type, and leaves a row in the journal's `refused_envelopes` table. Logs-only today: a Gatus or Discord signal is homelab#1009's scope. A body with no valid `messageId` cannot be counted and still loops, and rows for dead sessions are never pruned. |
| `last_envelope_age` | Time since any queue message. `none` right after a restart. |
| `deferred` | Slots of demand the planner could not serve. |

Diagnose in this order:

1. Check `charged` against the pods that actually exist.
2. Check `advertised` for the repository you are waiting on.
3. Check `last_envelope_age`.

Only then suspect GitHub.

## A quarantine drops the whole envelope

One queue envelope carries several events, and the journal refuses it as a unit. When it is quarantined, every sibling event goes with it, including the ones that were fine. The controller does not quarantine single events.

What survives is the shape. The WARN line carries `events` and a count per type (`job_available`, `job_assigned`, `job_started`, `job_completed`). The `refused_envelopes` row keeps `events` (the count) and `event_types` (a comma list of type names). A body that failed to parse records `0` and an empty list, because none of its events can be trusted. No job ID, repository name or other value is kept.

To see what was lost:

```sql
SELECT repo_id, set_id, message_id, reason, refusals, events, event_types, acked
FROM refused_envelopes WHERE refusals >= 3;
```

A row with `acked` set is a confirmed quarantine. A row at three or more refusals with `acked=''` is one of two things: the DELETE failed and the message is still queued, or the DELETE succeeded and only the stamp afterwards failed. The journal cannot tell these apart, so treat such a row as possibly lost.

A lost `JobAvailable` is the open question. It is not measured whether GitHub offers that job again through REST demand and the scale-set statistics, or whether the job waits until someone re-runs it. That is GitHub-side behavior. Until it is measured, treat a quarantine with `job_available` above zero as a job that may need a manual re-run.

## Roll back past journal version 15

Version 15 adds the `refused_envelopes` table. A version-14 image refuses to open a version-15 journal, so rolling the controller back needs one manual step on the journal first. With the controller stopped:

```bash
sqlite3 <state-dir>/pool.sqlite 'PRAGMA user_version=14;'
```

This is safe because version 14 never reads `refused_envelopes`; it leaves the table and its rows alone. Rolling forward again needs no step: the 14 -> 15 upgrade is a no-op, and the existing table and its rows are kept.

## Restart the controller only when sessions are deaf

- **Expected:** a stuck queue is fixed by cancelling and re-running the jobs.
- **Actually:** when no envelope arrives on any subscription while jobs are queued, the controller's queue sessions have died. Cancelling and re-running creates fresh jobs that are ignored the same way. Deleting the controller pod fixed it in under a minute. The controller now exits on its own after 30 silent minutes.
- **Cost:** hours of re-running jobs that were never the problem.
- **Code:** `cmd/runner-pool/runtime.go:39-44`.
- **Status:** `fixed`. A restart does not help when the silence comes from advertising zero to the repositories with work. Check `advertised` before restarting.

## Old queued runs can sit ahead of yours

- **Expected:** a fresh push to a pull request gets the next free slot.
- **Actually:** a repository's queued jobs are served roughly oldest first. Runs from stale branches, or from pull requests already folded into another, hold the queue.
- **Cost:** the pull request you care about waits behind work nobody needs.
- **Code:** none.
- **Status:** `by design` (GitHub). Cancel superseded runs before expecting yours to move.

## Pushing to a pull request while the pool is full wastes slots

- **Expected:** pushing a fix commit just queues new checks.
- **Actually:** workflows with a concurrency group cancel the pull request's in-flight run. On a saturated pool, that run may already hold one of the few assignments. The assignment is thrown away, and the new run goes to the back.
- **Cost:** on a two-slot pool, a few pushes to open pull requests can burn most of an hour's throughput.
- **Code:** none.
- **Status:** `by design` (GitHub). Batch fixes, and hold pushes to other pull requests while the pool is saturated.

## Some runs never get a runner

- **Expected:** a queued run eventually runs.
- **Actually:** we saw runs stay queued for hours while newer runs in the same repository got assignments, even after a controller restart. Cancelling them and closing and reopening the pull request produced fresh runs that did move.
- **Cost:** a pull request that looks merely slow for an afternoon.
- **Code:** none.
- **Status:** `open`, *unverified* cause. The fresh-run workaround is reliable.

## A cancelled run and its re-run share a commit

- **Expected:** the pull request's check rollup shows the latest result for each check.
- **Actually:** a cancelled run and its re-run have the same head SHA, and the rollup lists both under the same check name.
- **Cost:** a merge script that fails on any `cancelled` entry stops for good the first time you cancel and re-run.
- **Code:** none in the pool. Judge each check name by its newest attempt.
- **Status:** `by design` (GitHub).

## Retargeting a pull request does not trigger CI

- **Expected:** changing a pull request's base branch re-runs its checks.
- **Actually:** it does not. Closing and reopening the pull request does.
- **Cost:** a stacked pull request retargeted to `main` sits with no checks at all.
- **Code:** none.
- **Status:** `by design` (GitHub). A pull request with merge conflicts also gets no `pull_request` run and no message saying why. Merge the base in first.

## A pull request can depend on the bug it fixes

- **Expected:** every fix goes through CI.
- **Actually:** when the pool itself is broken, the pull request that fixes it cannot get a runner. We needed an admin merge several times.
- **Cost:** without a documented exception, the fix waits forever.
- **Code:** none.
- **Status:** `by design`. Our rule is an admin merge only when pool CI is blocked by the bug the pull request fixes, with the evidence recorded, after the same review the change would otherwise get. A hand-run hosted-runner copy of the required checks now exists as the better answer. It runs the same job names on the pull request's head commit, but has not yet been proven to satisfy a required check on a live pull request.

## Dependabot and a self-hosted pool

- **Expected:** Dependabot runs wherever the repository's other jobs run.
- **Actually:** Dependabot on self-hosted runners needs a runner group or scale set named `dependabot`, Docker-in-Docker, and a repository setting. We did not confirm that personal accounts get it at all. Otherwise its runs use GitHub-hosted minutes.
- **Cost:** on a personal account with a hosted-minutes cap, Dependabot runs are cancelled for billing.
- **Code:** none. We use self-hosted Renovate instead, behind a separate, strictly scoped GitHub App.
- **Status:** `open` (unverified for personal accounts).

## A billing stop looks like a broken workflow

- **Expected:** a failed job has logs.
- **Actually:** when hosted minutes or spending limits run out, a hosted job ends in a few seconds with zero steps. The only explanation is an annotation about account payments.
- **Cost:** a job that is still on a hosted runner, often because it carries a secret, fails silently after a migration.
- **Code:** none.
- **Status:** `by design` (GitHub). Check a failed job's `steps` count and annotations before debugging the workflow.

## The dockerd sidecar outlives the runner

- **Expected:** once the runner container exits, the worker Pod finishes.
- **Actually:** each worker Pod also runs a `dockerd` sidecar, and it keeps running after the runner exits. The Pod shows `1/2 NotReady` and stays in phase `Running`. The controller deletes finished worker Pods itself, but its teardown cannot yet prove the worker absent, so it logs `Worker teardown lacks exact absence; retaining its reservation for the next cycle` and holds the slot one more cycle.
- **Cost:** part of the idle time between jobs on every slot. With the controller scaled to zero, nothing deletes these Pods, and they look like live jobs.
- **Code:** none yet.
- **Status:** `open`. With the controller at zero, delete a worker Pod only once its `runner` container has terminated. Never delete a Pod whose `runner` is still running, or a `Pending` one, which is a VM booting for a job already assigned.

## Drain the pool at a moment, not at zero

- **Expected:** before host maintenance, wait for the cycle line to read `charged=0`, then stop the controller.
- **Actually:** behind a queue, `charged=0` never arrives: every freed slot is refilled within a cycle. One drain waited 34 minutes before giving up.
- **Cost:** a maintenance window spent waiting, or a queue cancelled to make room.
- **Code:** none in the pool.
- **Status:** `by design`. Scale the controller to zero at a moment when no worker is still booting: every worker Pod is `Running` and at least two minutes old. Workers already running finish their jobs. Then delete finished Pods as described in the entry above.

## A ConfigMap-mounted script is a symlink

- **Expected:** a Node script started as `node /scripts/x.mjs` sees its own path in both `process.argv[1]` and `import.meta.url`.
- **Actually:** a ConfigMap volume mounts each key as a symlink (`/scripts/x.mjs -> ..data/x.mjs`). Node resolves symlinks for the main module, so `import.meta.url` is the resolved file while `argv[1]` is the link. The common entry guard `import.meta.url === pathToFileURL(process.argv[1]).href` is never true.
- **Cost:** `main()` never runs. The process exits 0 with an empty log, and the job reads as a success that did nothing.
- **Code:** none in the pool; this applies to any Node workload run beside it.
- **Status:** `by design` (Kubernetes and Node). Compare `realpathSync` of both sides. Test the guard by running the script through a symlink and requiring a line only `main()` prints.

## A pod deadline does not leave time to clean up

- **Expected:** when a pod's `activeDeadlineSeconds` expires, the container gets SIGTERM and its termination grace period to finish cleanup, such as revoking a token.
- **Actually:** a pod killed at its deadline ended `Failed/DeadlineExceeded` with no log line from the wrapper's cleanup, even with a 90-second grace period and an init process that forwards SIGTERM. The token it should have revoked lived until it expired.
- **Cost:** a credential outlives its job by up to its full lifetime.
- **Code:** none in the pool.
- **Status:** `by design`. Enforce your own time budget inside the wrapper, well under the pod deadline: stop the child with SIGTERM, then SIGKILL after a short wait, then clean up.

## A Job-level deadline deletes the pod and its logs

- **Expected:** after a Job hits its deadline, `kubectl logs` shows what happened.
- **Actually:** when a Job's own `activeDeadlineSeconds` expires, Kubernetes terminates and removes its pods. Their logs go with them.
- **Cost:** a deadline test on a Job copy cannot be read afterward.
- **Code:** none in the pool.
- **Status:** `by design` (Kubernetes). Test deadline behavior with the pod-level `activeDeadlineSeconds`, or stream the logs while the pod runs.

## Large job images do not fit the worker's Docker storage

- **Expected:** a job can `docker pull` any image a hosted runner can.
- **Actually:** until homelab#983, each worker's Docker data root was a 4 GiB memory-backed volume (`docker_bytes` in the controller config). An image of about 2.6 GB compressed and 7 GB unpacked downloaded every layer within a minute, then stalled partway through extraction until the job timed out.
- **Cost:** a job that hangs for its full timeout with no error that names storage.
- **Code:** homelab#983 moves the data root to `--data-root=/var/lib/sjo-docker`, a path on dockerd's own 10 GiB devmapper root filesystem, and drops the `docker` volume. The path is not `/var/lib/docker` because the dockerd image declares `VOLUME /var/lib/docker`, and with nothing mounted there the runtime puts that volume in guest RAM. The argv is `dockerdArgs()` in `internal/backend/kubernetes/backend.go`; `admission.yaml` pins the same vector. A job can now unpack images up to about 9 GiB, and hits `ENOSPC` at the device's 10 GiB; no `sizeLimit` or ephemeral-storage limit is enforced on this host.
- **Status:** `open` until the homelab#983 cutover is deployed and its canary passes (plan `docs/plans/2026-09-25-runner-pool-dockerd-disk-root.md`, T4). Until then, when a job needs only a few files from a large image, fetch the image's manifest and layers by digest, check each digest, and unpack only what you need.

## Node's test runner changes its summary format

- **Expected:** `node --test` prints the same summary lines everywhere.
- **Actually:** some Node versions print TAP when their output is piped (`# skipped 0`), and others print the spec reporter (`ℹ skipped 0`).
- **Cost:** a CI check that greps one form passes locally and fails on the runner, or the reverse.
- **Code:** none in the pool.
- **Status:** `by design` (Node). Match both forms exactly, for example `^(ℹ|#) skipped 0$`, and nothing looser. Or pass `--test-reporter` explicitly.

## Path-gate required checks with a job-level `if:`

- **Expected:** a `paths:` filter on a workflow is the way to skip required checks a change cannot affect.
- **Actually:** a workflow skipped by `paths:` leaves its required checks pending, which blocks the merge. A job skipped by an `if:` condition reports success, which passes a required check. So gate each required job with a job-level `if:` that reads a change-detection job's output, written to fail closed: `${{ !cancelled() && needs.changes.outputs.<area> != 'false' }}`.
  - `!= 'false'`: if the change-detection job fails, its outputs are empty, and the gated job runs.
  - `!cancelled()`: without a status function GitHub adds an implicit `success()`, so a failed change-detection job would skip every dependent job, and a skip passes.
- **Cost:** on a small pool, every change queues every job. Getting the gate wrong either blocks every docs-only change or lets a real change merge unchecked.
- **Code:** none in the pool; this is how consumers write workflows.
- **Status:** `by design` (GitHub). Proven live: a docs-only pull request ran 4 jobs, its three gated required checks reported `skipped`, and the merge went through. Pin the exact `if:` form with a lint so it cannot drift.
