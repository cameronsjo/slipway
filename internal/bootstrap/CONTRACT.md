# Worker bootstrap contract

This contract is shared by the Kubernetes and Tart backends. A platform adapter
must satisfy it before it can be connected to the production daemon. The live
platform pilots still have to prove the network and credential boundaries.

## Controller endpoint

Each platform instance has one dedicated bootstrap HTTPS listener. Its
configuration has no default and must explicitly provide:

- a bind address;
- one advertised HTTPS origin with a DNS hostname and port, and no user info,
  path, query, or fragment;
- certificate and key paths readable only by the controller account;
- a required PEM trust bundle containing only public certificates; and
- the server resource limits listed below.

The advertised hostname is the TLS server name. The listener serves the
`JITBroker` directly, without a general controller mux, health route, debug route,
static files, redirects, or proxy. The only accepted request shape is:

```text
GET /v1/leases/<canonical-lease-uuid>/jit HTTP/1.1
Authorization: Bearer <one-use-per-lease-capability>
```

The request must use TLS, contain exactly one Authorization value, and have no
body, transfer encoding, query, fragment, or alternate escaped path. Every other
request receives the same credential-free refusal. The server must not log URL
queries, headers, bodies, capabilities, or JIT bytes.

The listener permits at most 16 simultaneous connections and disables HTTP
keep-alives. It uses a 5-second header deadline, 10-second read deadline,
15-second write deadline, 15-second idle deadline, and 8 KiB maximum header size.
The JIT response is at most 64 KiB and has an exact Content-Length. A platform
must fail startup if it cannot apply every bound.

`server.go` implements this listener and accepts the `JITBroker` as its sole
handler. Platform daemon wiring must use it rather than constructing another
HTTP server.

## Worker injection

The platform adapter is configured with the same advertised origin and public
PEM trust bundle as the lifecycle component. It injects these immutable public
values into each fresh worker together with only these per-job values:

- the canonical lease UUID;
- its 256-bit one-use bootstrap capability; and
- the absolute native runner root.

The capability is injected as `SLIPWAY_BOOTSTRAP_TOKEN` for the initial worker
process. The other values are fixed launcher arguments. They do not enter a
worker manifest annotation, log, command line, reusable image, controller
journal, or backend inventory. Kubernetes must use an in-memory per-Pod secret
projection with no service-account token. Tart must inject into the fresh VM's
initial process without writing it into the base image or host-visible VM
metadata. The public CA file may be persisted in the immutable image. The worker
must refuse to start without this explicit trust bundle; it must never fall back
to the host system trust store.

Where the capability is delivered over an interactive stream rather than a
projection, the controller speaks first and the worker stays silent until it is
prompted. A stream attached after the worker started carries none of the output
the worker already produced, so a worker that announces itself unprompted can
only be heard by luck. Every line of such a handshake is bounded in length and
in time, and the capability is written only after the worker's identity has been
read and matched.

`runner-pool-worker` removes the capability from its environment before network
access, validates the untouched runner root, makes one direct HTTPS request with
the supplied trust bundle and no proxy, redirect, or retry, and passes only the
opaque JIT value to the native runner environment. App keys, installation tokens,
reusable registration tokens, Kubernetes credentials, and host-helper credentials
never cross this boundary.

## Lifetime and shutdown

The capability and its in-memory JIT payload expire ten minutes after preparation.
A revoked or expired capability receives the same credential-free refusal as an
unknown one: no refusal body, header, or diagnostic repeats the capability, the
JIT bytes, or any other credential. Consumption is committed before response
bytes are written. A partial response,
lost response, cancellation, expiry, controller restart, or concurrent/repeated
request consumes or forgets the capability and requires exact registration and
worker cleanup; it never causes another JIT POST or delivery attempt.

The daemon expires transfers on every lifecycle reconciliation. On shutdown it
first stops accepting bootstrap connections, allows at most two minutes for an
active response and lifecycle cleanup, then closes connections and exits. Process
exit destroys every remaining in-memory capability and payload. Their durable
records stay revoked or unresolved and keep the reservation charged after restart.

## Required platform witness

Each Linux and macOS pilot must show all of the following from the actual worker
network, with the intended rule identified for each refusal:

1. the assigned lease and capability fetch exactly one JIT response;
2. a wrong lease, wrong capability, replay, sibling worker, non-TLS request,
   wrong TLS name, and untrusted CA fail;
3. every other path and every other controller, host, Kubernetes API, LAN,
   tailnet, metadata, and sibling endpoint remains unreachable;
4. the App key, installation and registration tokens, JIT bytes, and capability
   are absent from manifests, backend inventory, journal files, logs, and the
   next worker; and
5. expiry, cancellation, restart, and interrupted delivery leave the slot charged
   until physical worker/storage, GitHub registration, and acquisition cleanup
   are independently reconciled.

This witness proves the bootstrap boundary only. It does not prove that the
GitHub JIT credential itself cannot be copied or replayed after delivery.

## Tart backend

The macOS adapter (`internal/backend/tart`) satisfies this contract with one
structural difference, and the difference re-scopes three of the five witness
items above. The reason is the network, not a relaxation: the `tart-ci` pf
anchor blocks every guest-to-host packet except DHCP and DNS, so the HTTPS
bootstrap route is unreachable from a Mac guest. Opening a pass for it would
mean editing a hash-pinned root-owned anchor to buy symmetry with a platform
whose network looks nothing like this one.

**The host fetches, the guest never holds the capability.** The adapter runs in
the same process as the `JITBroker`. It makes the contract's one-use request
itself, dialling `127.0.0.1` at the advertised origin's port with the TLS server
name set to the advertised hostname and `RootCAs` set to only the configured
bundle — no proxy, no redirect, no retry, TLS 1.2 floor. The bootstrap listener
binds a loopback address and the adapter refuses to construct otherwise. The
opaque JIT is then written to the stdin of one `tart exec -i <vm>
<guest-launcher>` call. There is **no `SLIPWAY_BOOTSTRAP_TOKEN` analog in the
guest**: the capability never crosses the boundary in any form, so there is
nothing for a guest process to unset.

**The CA bundle is not baked into the guest.** On Linux the public bundle is
part of the immutable worker image because the worker dials the controller. The
Tart guest makes no bootstrap request, so it needs no trust bundle, and the
stock Cirrus base image is used unmodified. The bundle is read on the host.

**The runner tree and the launcher are streamed in, not baked.** Both are
sha256-verified on the host before any byte reaches the guest. The image's own
`~/actions-runner` is never used.

**The lost property, named.** On Linux the worker's own fetch is its proof of
life: only a running worker inside the fresh VM can spend the capability. Here
the host spends it, so a successful fetch proves nothing about the guest.

The replacement is ordering plus identity. The adapter fetches a JIT only for a
VM it created in the same `Create` call, only after that VM's own `tart ip`
answer is confirmed to be an IPv4 address inside `192.168.2.0/24` and not the
gateway, and only after the guest agent has answered `tart exec`. A failed
stdin write, or a launcher that does not print its one-line handshake within the
configured window, burns the capability and stops the VM: the JIT transfer's
durable record is already past its one-use phase guard, so no second fetch can
succeed for that lease; the lease stays charged; and the VM stays reported by
`Inventory` until physical absence is proven. What is **not** replaced is the Linux property that a fetch implies a
live worker — the macOS pilot must establish job start from GitHub's own
acquisition evidence instead.

Witness items re-scoped for this backend:

- **Item 2** (a wrong lease, wrong capability, replay, sibling worker, non-TLS
  request, wrong TLS name, untrusted CA all fail) is asserted against the
  **host-side** client, since no guest can reach the listener. "Sibling worker"
  becomes "a second host process", and the LAN and tailnet cases become the
  stronger assertion that the listener is not bound anywhere but loopback.
- **Item 3** (every other endpoint unreachable) keeps its full force for the
  guest and gains the bootstrap port: from inside a pool VM, the host gateway,
  the LAN, the tailnet, the `ci-guest` sibling, **and** the bootstrap port must
  all be refused. Sibling isolation here comes from softnet, not from pf; that
  is measured, and it does not carry over to a VM started without
  `--net-softnet`.
- **Item 4** (no credential in manifests, inventory, journal, logs, or the next
  worker) adds the adapter's own surfaces: the argv and environment of every
  external command, the stdin of every command except the one launcher call, the
  ledger under `<state-dir>/tart-ledger/`, and the structured log. The ledger
  holds one field, the lease UUID. The delivery child's stdout is never
  accumulated -- only one bounded handshake line is read from it -- because that
  stream is the whole job's output.

**A controller restart ends the in-flight job.** The delivery child holds a pipe
for its handshake line, so when the controller exits the orphaned `tart exec`
dies on its next write and the guest's runner goes with the VM. Item 5 is
unaffected -- the slot stays charged, the VM is found by the next inventory and
destroyed, and the job reports failed to GitHub -- but the restart is not
transparent to a running job.

Items 1 and 5 are unchanged and unweakened.
