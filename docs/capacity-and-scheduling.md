# Capacity and scheduling

The pool has a small, fixed number of worker slots and many repositories. Every minute the controller decides how much capacity to advertise on each repository's scale set. Most of the outages we hit were this decision going wrong while the pool itself was healthy.

## How the planner works

`planCapacity` (`internal/controller/advertisement.go:373-458`) is a pure function. Its inputs are the scale sets, the slots each repository already holds, per-repository demand, the headroom left, and a durable cursor.

- **Headroom** is admission minus every lease not yet `released` (`internal/controller/advertisement.go:203-237`).
- **Demand** is journaled jobs, raised to at least one slot by a fresh REST reading or a fresh available-job statistic (`internal/controller/advertisement.go:159-185`).
- The **first pass** walks repositories starting after the cursor. Each one with unmet demand gets a slot.
- The **exploratory slot** comes next. If headroom is left, at most one repository with no demand and no slot gets one slot.
- **Later passes** give demand only.
- The **cursor** moves to whoever got the last slot, and it is stored in the journal, so it survives restarts.

## A never-advertised repository is invisible

- **Expected:** a repository with queued jobs shows up as demand.
- **Actually:** GitHub assigns only to a scale set that advertised. A repository the pool never advertised for has no journaled rows and no assigned jobs. Its work appears only as `TotalAvailableJobs` in a poll envelope. Because an empty poll returns no envelope, that count is seen once and then ages out. After a restart it is classed as inherited and discarded.
- **Cost:** the repository's only route in is the rotating exploratory slot. With about 50 repositories that is a discovery delay of about 45 minutes, while the pool reads `free=2 charged=0` and looks idle.
- **Code:** the freshness window now guards only the assigned count; an available count buys one slot at any age (`internal/controller/advertisement.go:111-158`).
- **Status:** `fixed`. A repository allowlist file that looks like an onboarding gate is not one; the controller discovers repositories from the App installation.

## The exploratory slot can starve real demand

- **Expected:** when slots are scarce, repositories with queued jobs are served first.
- **Actually:** an earlier planner let the one exploratory slot compete with real demand in the first pass. That was a deliberate fix for an earlier starvation, where a busy repository took every slot and an idle neighbor never got the one advertisement GitHub needs. With only one slot of headroom, the pass walked the cursor across all repositories. Nearly all of them were idle, so the slot went to an idle repository most sweeps.
- **Cost:** measured with about 50 repositories and one usable slot: a repository with 36 queued jobs got one slot about every 48 minutes. The controller's liveness check then saw 30 minutes of silence and restarted it, which changed nothing, because the cursor is durable.
- **Code:** repositories with demand are served first, and the exploratory slot uses only the headroom left after them (`internal/controller/advertisement.go:381-429`). The REST demand reading now finds most queued work, so the probe is needed only where that reading is missing or stale.
- **Status:** `fixed`.

## A quarantined lease never gives its slot back

- **Expected:** once a worker is gone, its slot is released.
- **Actually:** two paths put a lease into `quarantined`:
  - a failure after the lease is acquired but before the worker is created, such as a failed JIT preflight (`internal/controller/lifecycle.go:271-298`);
  - a cleanup deadline passing while cycles fail (`internal/controller/lifecycle.go:691`).

  The release path used to select only `cleanup-pending` leases, and no statement moved a lease out of `quarantined`. The lease kept counting against headroom, and restarts did not help, because the state is in the journal.
- **Cost:** one of two slots stayed charged for hours with no worker, job or storage behind it. That halved the pool and triggered the starvation above.
- **Code:** the release queries now select `quarantined` too (`internal/controller/lifecycle.go:754`, `internal/controller/lifecycle.go:775-777`). A lease still releases only under the same three proofs: no worker, or a worker proven absent; the JIT registration removed; acquisition evidence present.
- **Status:** `fixed`.

## A lease can fail the whole cycle

- **Expected:** one bad lease affects one worker.
- **Actually:** deleting a quarantined worker requires proof that its process and storage are both gone. The first reading after a delete is taken before the Pod object disappears, so both flags read false. Earlier code returned that as an error, and the error aborted the entire cycle, so every other cycle did no work.
- **Cost:** about half the pool's throughput, silently.
- **Code:** a worker still tearing down is logged and kept for the next cycle instead of failing it (`internal/controller/orchestrator.go:177`). A missing worker identity still fails the cycle. The next cycle's inventory reconcile proves the absence and releases the worker normally.
- **Status:** `fixed`.

## Capacity changes wait for an idle pool

- **Expected:** changing `-capacity` takes effect on the next start.
- **Actually:** the journal records capacity at first open. Early versions refused to start when the flag differed, which crash-looped the controller on the first capacity raise. The current code defers the change and keeps the old sizing until no lease is active (`internal/controller/store.go:529-541`, `internal/controller/store.go:654-710`).
- **Cost:** a lease that never releases means the pool is never idle, so a raise never applies. Check the controller log for the deferred-sizing warning after any change. A successful change logs `Reconciled platform sizing on an idle journal.`
- **Status:** `fixed`. Quarantined leases now release, which removes the most common reason the pool never went idle.

## Sizing: where the slot count comes from

- **Expected:** worker count is bounded by CPU or memory.
- **Actually:** on our host the first limit was storage. Each Linux worker gets three writable 10 GiB block devices (pause, runner and dockerd) from a dedicated LVM thin pool, plus overhead, 30.5 GiB per worker (`PerWorkerStorageBytes` in `internal/backend/kubernetes/backend.go`; homelab#983 raised it from two devices after measuring three). After the witness's 8 GiB reserve and 2 GiB runtime overhead, a 64 GiB thin pool fits at most one worker and a 128 GiB pool at most three. Four need the larger pool homelab#983 grows before its cutover. Memory is the next limit: each Kata worker is a fixed 2 vCPU / 8 GiB VM plus runtime overhead, and admission subtracts what other workloads reserve (their limits, not their usage).
- **Cost:** a ceiling picked during qualification becomes the production limit unless someone re-derives it. LVM cannot shrink a thin pool, so growing it is one-way.
- **Code:** ceilings in `cmd/runner-pool/runtime.go:24-33`; the admission budget in `cmd/runner-pool/linux_backend.go:130-137`.
- **Status:** `by design`. The worker network is also sized to the slot count, so raising it touches the host guard too; see [Isolation and network](isolation-and-network.md).

## Each job costs minutes of slot time, however short it is

- **Expected:** a 10-second lint job costs about 10 seconds of pool time.
- **Actually:** between one job ending and the next worker existing, a slot sat idle for about 3.6 minutes. The time goes to a fresh microVM boot, the completion message being read only at the next poll, cleanup starting one cycle later, and teardown holding the slot for one more cycle (see [Operations and recovery](operations-and-recovery.md#the-dockerd-sidecar-outlives-the-runner)).
- **Cost:** on a pool with a few slots, a workflow split into many tiny jobs spends most of its slot time on turnover. One repository folded 17 separate lint and test jobs, each 5 to 45 seconds long, into one job.
- **Code:** none in the pool; this is how consumers write workflows.
- **Status:** `open` for the turnover gap. Minimize the number of jobs per run, not the length of each job: put many short checks in one job as steps, and run each step under `if: ${{ !cancelled() }}` so one failure does not hide the rest.
