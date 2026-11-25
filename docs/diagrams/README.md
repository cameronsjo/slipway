# Runner pool diagrams

Three diagrams covering the shared GitHub runner pool on MS-A2. Each ships as an
editorial HTML source (`diagram-design` skill format) plus an exported `.svg`
next to it. PNG export is not available in this environment — see the note at
the bottom of this file.

## `isolation-layers.html`

Nested-containment view of the five shells a CI job crosses, outside in: the
MS-A2 bare-metal host, a k3s pod, a Kata VM via QEMU, the runner container
running as UID 1001, and a root-only Docker daemon reachable only inside that
VM. Drawn from `services/runner-pool/README.md` and the "Worker boundary and
qualification defaults" section of the approved
[shared runner pools plan](../../../../docs/plans/2026-09-12-shared-github-runner-pools.md).
That plan is implemented source with several canaries passed (Kata mechanics,
storage) — the nesting itself reflects shipped design, though production
activation is still pending.

## `network-boundary.html`

What a worker VM can and cannot reach, and which layer enforces each control:
public HTTPS and cluster DNS allowed outright, one exact-tuple bootstrap JIT
route allowed narrowly, and everything else — host, LAN, tailnet, link-local
and metadata addresses, sibling workers and other pods, other controller
endpoints, non-443 ports, IPv6, and ARP/MAC spoofing — denied at a named layer
(the independent nftables guard, NetworkPolicy as a weaker second layer, Kata
plus admission, or the CNI's port isolation and MAC-spoof checks). Drawn from
the draft plan on branch `plan/runner-pool-network-qualification`
(`docs/plans/2026-09-13-runner-pool-network-qualification.md`, homelab#722).
**Every control on this diagram is marked unproven**: that plan is a draft
awaiting panel review, no host action has been authorized, and the source
harness in `services/runner-pool/canary/network` reports all live results as
`UNMEASURED`.

## `job-lifecycle.html`

One CI job's path through the pool, left to right: GitHub queues the job,
the controller discovers the repository and reserves a slot, mints a one-use
JIT runner config, creates the worker pod (recording a durable create intent
first), the Kata VM boots and bootstraps over the narrow HTTPS route with a
per-lease token, the job runs with its own Docker daemon, and the runner exits
with its pod deleted. Two safety checks are called out separately: quarantine
after three provisioning failures within fifteen minutes, and the host witness
that must confirm the VM, its snapshots, and its devices are physically gone
before the slot is released — pod disappearance alone never releases it.
Drawn from the "Worker boundary and qualification defaults" and "JIT delivery
source checkpoint" sections of the
[shared runner pools plan](../../../../docs/plans/2026-09-12-shared-github-runner-pools.md)
and `internal/bootstrap/CONTRACT.md`. The JIT broker, bootstrap listener, and
lifecycle bookkeeping are implemented source; no controller is deployed and no
live job has run through this path yet.

## PNG export

`diagram-design:doctor` reports Playwright is not installed in this
environment (`WARN`, `pip install playwright && playwright install chromium`
not run — no auto-install per the skill's contract). Only `.svg` exports are
included here. Re-run `diagram-design:export-diagram` on each `.html` source
after installing Playwright to produce the `.png` files.
