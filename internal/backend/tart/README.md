# Tart worker backend (macOS)

The macOS half of `controller.WorkerBackend`. One Tart VM per job on the M1 Max, cloned from a digest-pinned base image, fenced by the pf boundary that is already installed on that host, and destroyed afterwards.

Design decisions and their reasoning live in [`docs/plans/2026-09-20-runner-pool-macos-tart-pilot.md`](../../../../../docs/plans/2026-09-20-runner-pool-macos-tart-pilot.md). The bootstrap differences from the Linux adapter are in [`internal/bootstrap/CONTRACT.md`](../../bootstrap/CONTRACT.md) § Tart backend.

## What it does

`Create` runs a fixed order and never returns success on a partial result:

1. Write the ledger entry (lease UUID only), before anything exists to lose.
2. Memory gate, then the owned-VM cap (`max_owned_vms`).
3. The pf gate — four read-backs, below.
4. `tart clone <base-image-ref> <name>`.
5. `tart set <name> --random-mac --random-serial --cpu N --memory M`. `tart clone` copies the base image's MAC and ECID verbatim; without this a pool VM carries `ci-guest`'s MAC onto the same bridge. The clone's `config.json` MAC is then read back and must differ from every other local VM's; its own config, and a ledger-owned sibling's config when present, must be readable; a vanished, unreadable or symlinked unowned entry is skipped, and a symlinked `config.json` is never followed.
6. `tart run <name> --no-graphics --net-softnet` as an owned child with `TART_NO_AUTO_PRUNE=1`, so a clone can never evict the base image the 04:30 `ci-guest` reprovision clones from.
7. Wait for the guest agent to answer `tart exec <name> /usr/bin/true`, bounded by the lease's startup deadline.
8. `tart ip <name>` must parse as IPv4 inside the configured subnet and must not be the gateway.
9. Stream a sha256-verified runner tarball and the guest launcher in over `tart exec -i`.
10. Fetch the JIT host-side from the loopback bootstrap listener, and write it to the launcher's stdin.
11. Wait only for the launcher's one-line handshake, bounded by `launch_handshake_timeout`.

Anything that fails after step 4 leaves the ledger entry in place, so `Inventory` keeps reporting the VM and the controller drives `Destroy`. Anything that fails after step 7 **also stops the VM immediately** rather than leaving it running until a later cycle notices.

## Delivery does not wait for the job

`tart exec -i <vm> <launcher>` does not return until the JOB ends, because the launcher execs the native runner in place. Waiting on it would stall the whole sequential daemon cycle for the length of the job — no inventory, no cleanup, no deadline handling — and any job over the ten-minute startup deadline would be quarantined while running correctly. It would also accumulate the job's entire stdout in controller memory.

**A controller restart ends the in-flight job.** The `tart run` child gets `/dev/null` for stdout and stderr — a real descriptor it inherits, so it survives this process — but the delivery child needs a pipe for the handshake, and when the controller exits that pipe's read end closes and the orphaned `tart exec` dies on its next write. The VM is then found by inventory on the next start and destroyed, and the job shows as failed in GitHub. Restarting the controller mid-job is not free.

So the delivery child is started and never waited on. Its stdout goes nowhere except one bounded handshake line: the guest launcher prints `SJO_LAUNCH_OK` after every validation has passed and immediately before the exec that replaces it. No handshake within `launch_handshake_timeout` is a refusal, and the VM is stopped. The adapter owns that child alongside the `tart run` child and reaps both on `Destroy`.

## The gate

Read-only, and it mutates nothing, so it cannot race Apple's own ruleset reload and cannot touch the legacy guest. Five assertions over the three argument-exact NOPASSWD entries in `hosts/m1max/pf/sudoers.d-tart-pf-assert` plus one plain file read:

| Assertion | Read-back |
|---|---|
| pf is enabled | `sudo -n /sbin/pfctl -s info` |
| the anchor carries at least 8 `block drop` rules | `sudo -n /sbin/pfctl -a tart-ci -s rules` |
| the anchor file hashes to the reviewed value | `open(pf_anchor_file)` — world-readable, no sudo |
| the loaded rule counts equal the pinned file's | the anchor-rules capture |
| the anchor is referenced from the main ruleset | `sudo -n /sbin/pfctl -s rules` |
| the anchor is ordered ahead of `com.apple.internet-sharing` | the same capture |

Any command failure, any timeout, and any empty or unreadable output refuses. Unmeasured is not a pass. Every read is bounded by `gate_read_timeout`, because a wedged `pfctl` with no timeout blocks the re-assert loop forever with no log line — the fail-open shape the loop exists to prevent.

Three things that each looked sufficient and were not:

- **Counting rules is not pinning content.** A different ruleset with the same number of rules passes a count. `pf_anchor_sha256` pins the bytes, and it is the same `EXPECT_HASH` value `hosts/m1max/pf/tart-pf-assert.sh` already enforces — a test parses that value out of the script and asserts the example config carries it, so the two gates cannot drift.
- **The floor is 8, not 3.** The reviewed anchor carries 4 IPv4 + 1 antispoof + 3 IPv6 (homelab#417). A floor of 3 is cleared by an anchor holding only its three IPv6 rules, every IPv4 block gone. `Config.validate` refuses anything below `MinAnchorBlockRules`.
- **The hash pins the file; pf evaluates the loaded ruleset.** Those are not the same thing: a ruleset loaded from somewhere else can carry every reviewed block plus a leading `pass in quick on bridge100 all` that wins before any of them, and the hash sees nothing. The loaded `block` and `pass` counts must equal the pinned file's. Counts, not text, so pfctl's print formatting is not pinned — and the file side expands braced lists, because pf loads `proto { tcp, udp }` as two rules and the reviewed anchor uses that syntax three times. The reviewed anchor loads as **8 block and 6 pass** rules.
- **An anchor reference is a whole quoted token at the start of the line.** Three readings have been wrong here. A prefix match on `anchor "` + name accepted `anchor "tart-ci-old" all`. Allowing the keyword anywhere in the line accepted `load anchor "tart-ci" from "…"`, which loads the file and evaluates nothing. Treating a child path as ours accepted `anchor "tart-ci/evil"`, where pf evaluates the sub-anchor and the reviewed rules still never run. The line must **start** with `anchor "`, and the token must equal the name or the name plus exactly `/*` — the wildcard form because the real host prints Apple's as `anchor "com.apple.internet-sharing/*" all`.

The anchor name is configuration, but the sudoers file grants `-a tart-ci` only, so naming a different anchor is refused by sudo and reads here as an unreadable boundary — the intended fail-closed shape, and the stand-in negative control for the pilot.

**The pool never invokes `tart-pf-assert.sh`, `tart-ci-run` or `tart-ci-reprovision.sh`.** Their failure paths act on `ci-guest` by name. A test fails if any recorded argv contains those strings, and construction refuses a configuration naming one as a program.

Every gate command is also bounded by `WaitDelay`, not just by the context. `exec.CommandContext` kills the process when the context ends, but `Output()` waits for the stdout **pipe** to reach EOF, and a grandchild that inherited it keeps it open — measured at 60s against a 1s deadline. Without `WaitDelay` the read timeout is a suggestion.

While an owned VM exists the same assertions re-run every 60 seconds. On failure the adapter stops **only** the VMs named in its own ledger, and stops each one once **it has actually stopped** — a `tart stop` that fails is retried and re-logged on every tick, because recording the attempt rather than the outcome is how a wedged VM ends up running unfenced with nothing ever trying again. A partially failed `Destroy` clears that memory too. It derives the owned set from the ledger on every tick, not from in-memory child handles, so it resumes after a controller restart.

## The ledger

`<ledger_dir>/<resource-id>.json`, one field: `{"lease_id": "<uuid>"}`. Written before `tart clone`, removed only when `Destroy` has proven both process and storage absence. No capability, no JIT, no registration state.

Every `tart` invocation carries `TART_HOME=<tart_home>`, plus a derived `HOME` and `PATH` and `TART_NO_AUTO_PRUNE=1`. With an empty environment tart reads its *default* home, so nothing would tie the VMs the adapter clones and deletes to the `tart_home` the inventory reads `config.json` out of — two views of "which VMs exist" that can disagree.

`Inventory` joins three sources — the ledger, `tart list --format json`, and the directories under `<tart_home>/vms/`. If any one of them fails, `Complete` is false: a partial join cannot tell "absent" from "not read". A `slipway-macos-*` VM with no ledger entry is reported with the VM name as its lease identity, which the controller cannot resolve to a lease, so it fences. A **symlinked** VM directory is reported the same way whatever the ledger says: `os.ReadDir` reports it as a symlink rather than a directory, so filtering on `IsDir` alone made a VM planted behind a link invisible to the whole join.

## The owned-VM cap and free slots

`FreeSlots` is the physical number: `max(0, physical_vm_capacity - running darwin VMs)`. "darwin" comes from each VM's own `<tart_home>/vms/<name>/config.json` `"os"` field, because `tart list` carries no OS field — a running Linux VM charges no macOS slot. It is forced to zero when the memory gate fails. Once this controller owns any VM, it is also clamped to `max_owned_vms - owned`. With nothing owned it stays the physical number, as before ADR-0018. Independently of all of that, `Create` refuses a VM when `max_owned_vms` others are already in the ledger or memory is below the floor. Both refusals happen before the clone and carry `controller.ErrCapacity`, so the controller treats them as a capacity wait, not a provisioning failure that counts toward the breaker; an unreadable `vm_stat` is still a plain failure. After `tart set` it refuses a clone whose MAC another local VM carries.

### Reading a capacity refusal

A capacity wait writes no `provisioning_failures` row and does not move the breaker, so the failure rows and the breaker state no longer show a memory-gate or cap refusal. The only record is one log line from the adapter, in the pool's stderr log (`~/Library/Logs/sjo-runner-pool/runner-pool.err.log` on the M1 Max). Its message is `tart worker creation deferred before clone: no capacity this cycle` (the constant `CapacityDeferredMessage` in `backend.go`), with `check=memory-gate` or `check=owned-vm-cap` and `counts_toward_breaker=false`. A memory-gate deferral logs at `ERROR`, as the plain refusal did, because memory below the floor is a host-paging warning. A cap deferral logs at `WARN`. To count memory-gate deferrals in a window:

```bash
grep -F 'deferred before clone: no capacity this cycle' ~/Library/Logs/sjo-runner-pool/runner-pool.err.log | grep -c 'check=memory-gate'
```

An unreadable `vm_stat` is not a deferral. It still logs `tart worker creation refused before clone` with `check=memory-gate` and still writes a failure row.

`max_owned_vms` is the darwin daemon flag `-max-owned-vms`, default `1`, the cap that was hard-coded before ADR-0018. It is a flag, not a platform-file key, because `install.sh` derives the file's exact key set from the decoder's json tags; and it sits in the same plist as `-legacy-slots`, which it must fit inside: a value above 1 runs at 1, with a warning, until the effective `-capacity` minus `-legacy-slots` leaves that many pool slots (`EffectiveMaxOwnedVMs` in `config.go`). A value outside `1..physical_vm_capacity` is refused at startup. One is what keeps the nightly `tart-ci-reprovision` safe while `ci-guest` runs: with at most one pool VM, its re-clone always finds a free slot. Set `2` only after `ci-guest` and its reprovision are stopped ([ADR-0018](../../../../../docs/decisions/0018-macos-pool-owns-both-vm-slots.md)).

## Configuration

`config/macos-controller.example.json` is the schema, loaded by `cmd/runner-pool/macos_backend_darwin.go` with a strict decoder that refuses unknown fields and trailing data. The bootstrap origin and bind address come from the daemon flags, not the file, and so does the owned-VM cap (`-max-owned-vms`, darwin-only, default `1`).

## Tests

```bash
cd services/runner-pool
GOTOOLCHAIN=go1.26.4 go test -race -count=1 ./internal/backend/tart/... ./cmd/...
```

Every external command runs through the `Runner` interface, so the suite drives a fake that records the argv, environment and stdin of every invocation. Filesystem state is a real temp directory pointed at by `tart_home` and `ledger_dir`. The bootstrap fetch runs against a real loopback TLS listener with a generated CA.

The secrets test's weakness is worth stating: it records what the adapter *constructs*, which is call construction only. The plan's M4 repeats the sweep against the real host during the pilot job.
