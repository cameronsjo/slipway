# Kubernetes/Kata worker adapter

`backend/kubernetes` is the Linux implementation of the shared
`controller.WorkerBackend` boundary. It builds one fresh, pinned-image Pod per
lease with the dedicated production Kata `RuntimeClass`, fixed owner/lease
labels, no service-account token, no host namespaces or mounts, and bounded
memory-backed Docker/workspace/socket paths. Only the Docker sidecar is
privileged; the Kata runtime must provide `privileged_without_host_devices=true`
and must reject hypervisor annotation overrides.

The package exposes a small `API` interface over a concrete `client-go`
transport. `NewInCluster` uses the controller Pod's service-account
credentials; worker Pods set `automountServiceAccountToken=false`. The
transport paginates the exact owner/platform selector, merges it with an
injected trusted CRI/storage witness, creates a Pod once, waits until the
runner is attachable under the startup deadline, and deletes only with a
foreground UID precondition. It does not perform CRI or storage inspection
itself.

The host witness uses a separate Unix socket and Linux `SO_PEERCRED` in both
directions: the controller accepts replies only from the configured host UID,
and the witness reads requests only from the configured controller UID. Each
connection carries one bounded, versioned request for the immutable namespace
and owner. Process, storage, and capacity replies remain separate and an error,
partial reply, changed scope, stale socket, or unavailable peer credential
closes admission. The controller receives observations only; it receives no
containerd socket, host mount, or host credential. The host collectors behind
this protocol use fixed absolute `crictl` and `ctr` paths and a clean command
environment. The process collector accepts only the dedicated runtime handler,
records the CRI sandbox, Pod UID, worker name, and lease in a credential-free
host ledger, and retains matching QEMU/shim/virtiofsd orphans after the Pod
disappears. The storage collector maps only the dedicated snapshotter's
container metadata to that ledger and retains recorded writable snapshots after
container deletion. Missing CRI/containerd inventory, changed identities,
ordinary-container fallback, and unowned dedicated-snapshotter containers all
produce incomplete scopes.

Capacity combines two non-overlapping views. The Kubernetes source reads the
selected node's allocatable CPU/RAM and counts each non-pool container's CPU
**request** and the larger of its memory request or limit, including init
containers, Pod-level resources, and RuntimeClass overhead. The asymmetry is
deliberate: CPU is compressible, so an over-limit container is throttled rather
than killed and a limit charges capacity nobody holds; memory is not, so
overcommitting it kills containers. Counting CPU limits fenced this pool
permanently -- 13 idle legacy CronJob runners carry 4500m limits each against a
32000m node -- and `conservativeResources` carries the measured numbers. The host source measures `MemAvailable`,
the dedicated LVM thin pool's data and metadata use, and actual cache/log tree
sizes. Cache and log admission each receive at most half of current filesystem
headroom, which remains safe if both paths share a filesystem. Observed thin-pool
use above active-worker reservations is charged as other use. Missing fields,
overlapping authority, unsafe metadata pressure, or any budget overrun closes
admission.

Every headroom report names its **binding dimension** — `cpu`, `memory`,
`storage`, `cache`, `logs`, or `slots` when the configured ceiling binds before
any resource does. A bare `FreeSlots: 0` cannot tell a full thin pool from a
node whose memory is committed to idle CronJob limits, and on the real node it
sent an operator to the wrong one.

**An oversubscribed host fences admission; it does not fence the inventory.**
The invariant is two-sided and both sides matter. Never admit on negative
headroom: every dimension clamps to zero and `FreeSlots` is zero, so no new
worker is created. Never fence the list of workers the controller owns because
of it: the report stays `Complete`, so `Maintain` can retire the worker whose
retirement restores the headroom. Refusing the whole inventory fenced the
controller out of its own recovery on 2026-09-21, with one worker running and a
legacy CronJob rollover pushing free memory negative.

Fail-closed is retained, unchanged, for every case where the inventory is
**unknown** rather than full: an incomplete snapshot, a negative input value,
usage exceeding capacity, or a rootfs size that disagrees with the budget. An
oversubscribed host has been read; an unreadable one has not. Absence is still
never claimed without both proofs — a worker is omitted only when its Pod, its
process and its storage are all absent.

A shortage discovered inside `Create` wraps `controller.ErrCapacity`, and the
lifecycle treats that as a wait: no `provisioning_failures` row, no breaker, no
quarantine-for-failure. That is safe because the headroom check runs before the
create-attempt marker and before the Pod POST, so nothing physical exists — the
ordering is the proof, and a test asserts no Pod was requested.

A host witness **storage refusal** during that same pre-create inventory read
wraps `controller.ErrWitnessRefusedBeforeCreate` on the same ordering argument
(homelab#1213), unless a POST already went out for that resource. The lifecycle rolls the lease back without a failure row, but
still returns the error, so the cycle logs it and the next cycle retries. Only a
storage refusal is tagged: process and capacity refusals, an unreachable
witness, a failed Pod listing, and any error after the POST keep the counted
path.

The witness's storage collector re-reads the CRI Pod list once before refusing
writable storage whose sandbox the identity ledger does not hold. A worker
created between the Pod read and the snapshot read is attributed on the second
pass; storage whose owner never appears is still refused.

The already-attempted refusal is checked **above** the headroom check, for the
same reason in reverse: a resource whose Pod POST has already gone out must
never come back as `ErrCapacity`, because the lifecycle reads that as "nothing
was created, release the reservation". Ordering makes it true rather than
relying on the caller not to ask twice.

Capability delivery uses the Kubernetes `pods/attach` stream with an
interactive `HELLO?`, `HELLO name uid`, `CAP token`, `ACK name uid` handshake.
The controller speaks first, and that ordering is load-bearing rather than
stylistic: an attach does not replay output the container emitted before the
attach existed, so a worker that announced itself at startup raced the stream
and the controller read EOF instead of an identity. The worker now prints
nothing until it has read `HELLO?`, so the first byte in either direction is
produced after both ends are attached. Each handshake line is bounded at 256
bytes and each read and write is bounded in time, so a guest that never speaks
is reported as silent rather than holding the stream to the create deadline.

A failure *before* the capability is written also reports the stream's own
failure by class: `create stage=attach: ... (stream=forbidden)`. The vocabulary
is closed — `none`, `forbidden`, `not-found`, `container-not-running`,
`http-NNN`, `timeout`, `tls`, `refused`, `closed`, `other` — and no part of the
underlying error's text is ever returned, so a new failure mode can only be
under-described, never leaked. Nothing after the capability write is classified:
there the bytes in flight are the secret. The error also carries the runner
container's own state (`runner=terminated reason=Error exit=2`), which is what
separates "the guest never spoke" from "there was no guest to speak"; the reason
comes from an allowlist and anything else reads `other`.

The bounded write also gives up the moment the stream goroutine returns. Without
that, an attach refused in milliseconds still burned the full thirty seconds,
because an `io.Pipe` write blocks until someone reads it — which made a
permission error look exactly like a silent guest. The
capability is absent from the Pod, Secret, environment, arguments, image, and
journal; the stream is canceled after the bounded ACK. A stream that closes
before the ACK, or remains uncertain after it, is an error and is never
retried. Before opening that stream, creation requires the independent witness
to report the exact Pod UID with both its Kata process/sandbox and writable
storage present. The service account needs `get/list/create/delete` on worker
Pods, `get` and `create` on the `pods/attach` subresource (a WebSocket attach is an HTTP GET, and Kubernetes 1.35+ also requires `create` for the upgrade), cluster-wide `list` on Pods for
node workload accounting, and `get` on the selected Node.

The source slice is not a deployment or image publication. Container
writable rootfs is now charged as two fixed 20GiB devices per worker, separately
from memory-backed workspace/Docker volumes. The host-reported rootfs size must
match the controller budget, and every storage observation verifies separate
digests for the effective containerd config, Kata config, and Kata artifact
manifest. The exact CRI/containerd/LVM metadata produced by the approved
dedicated runtime must still pass a live failing-control qualification. No
cluster resource, runtime, image, or daemon is changed here.
The production API implementation has four obligations:

- `List` must be a complete listing of the managed/platform label scope plus
  the CRI sandbox/process and writable-storage witness. It must include orphaned
  Kata processes or storage after a Pod disappears and must return
  `Complete=false` when any scope cannot be read.
- `Headroom` must account for current workloads, Kata/containerd overhead, and
  enforceable storage capacity. The adapter charges runner requests, Docker
  requests, and configured overhead before returning free slots.
- `Create` receives the capability only as a transient
  `memory-only-authenticated-stream` request. The token must never be serialized
  into the Pod or Secret API objects, command line, environment value, journal,
  or image. The call performs one Kubernetes mutation; an uncertain response is
  reconciled by `List`.
- `Delete` must bind one delete to the reconciled Pod UID and exact ownership
  labels, then independently prove both process/sandbox and writable-storage
  absence. A partial witness is returned as partial evidence and keeps the
  controller reservation charged; the adapter will not issue a second delete.

The adapter rejects the existing `kata-qemu` and `sjo-kata-qemu-probe` classes.
The production class name, runner image, Docker image, worker binary, CA path,
bootstrap origin, controller owner UID, rootfs size, and MS-A2 node are immutable
configuration. The Linux daemon factory now composes the in-cluster clients,
host witness, conservative capacity source, trusted inventory, Pod transport,
and backend from one strict non-secret JSON file. The separate root witness
executable composes the privileged collectors from its own strict file. The
checked-in examples are deliberately inert. This source slice does not install
a runtime, create a `RuntimeClass`, publish an image, or apply manifests. Those
values must be matched against the approved fingerprinted MS-A2 runtime and a
real worker image before deployment.

Tests exercise the fail-closed boundary: incomplete and foreign inventory,
duplicate ownership, negative headroom, pending Pods, exact UID-bound deletion,
repeated mutation refusal, pinned images, dedicated runtime selection, bounded
resources, memory-only volumes, and absence of the bootstrap capability from
the generated Pod spec.
