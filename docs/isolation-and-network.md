# Isolation and network

Each Linux job runs in a Kata microVM on a small, dedicated worker network. A host nftables guard fences that network (`canary/network/host/canary-guard.nft`). The guard is the security boundary for everything a job can reach, so every change to it is treated as a security change.

## Only private repositories, only without deployment secrets

- **Expected:** a self-hosted pool can serve any repository.
- **Actually:** a public repository lets anyone open a pull request that runs on your hardware. The pool therefore serves private repositories only. The controller re-reads each repository's visibility from the App's own discovery, not from its caller, and refuses a repository that turned public (`internal/githubapp/app.go:292`).
- **Cost:** one public repository on the pool gives a stranger code execution on your network, however good the VM isolation is.
- **Code:** see above. Public repositories stay on hosted runners or on a separate, more tightly scoped fleet.
- **Status:** `by design`. Jobs that need deployment credentials (signing, publishing, cloud deploys) also stay on hosted runners, so no long-lived secret reaches a pool worker. One exception: an Apple code-signing identity and an App Store Connect API key may reach a macOS pool guest for a TestFlight upload from `main`, on an in-guest RAM disk only, per homelab ADR-0017. It does not apply to the Linux pool or to notarization.

## Egress is public HTTPS only, so port 80 hangs

- **Expected:** a worker can reach the internet on any port.
- **Actually:** the guard accepts new connections to public addresses on TCP 443 only (`canary/network/host/canary-guard.nft:255`). Everything else is dropped silently, not rejected. A fetch on port 80 therefore hangs until its own timeout.
- **Cost:** an `http://` package mirror or a scheme-less `curl example.com/install.sh | sh` looks like a slow mirror for five minutes, then fails. Nothing names the port.
- **Code:** the guard above.
- **Status:** `by design`. Fix the job, not the guard:
  - switch apt sources to `https://`;
  - copy a CA bundle into images that lack one, for example from a distroless stage;
  - give every `curl` an explicit `https://`;
  - sweep a repository with `grep -rnE 'http://|curl [^h-]' .github/ Dockerfile*` before moving it.

## One worker must not borrow another's address

- **Expected:** per-VM IP anti-spoofing is enough.
- **Actually:** the guard has several layers.
  - The CNI bridge plugin's `macspoofchk` pins each port to its assigned MAC (the worker network's CNI conflist in `canary/network/host/`, line 19).
  - A bridge-level rule allows only bound (MAC, IPv4) pairs (`canary/network/host/canary-guard.nft:173`).
  - An inet rule drops any source outside the slot set (`canary/network/host/canary-guard.nft:230`).

  A worker sending with its own MAC and a *sibling slot's* IPv4 passes the first and third layers. Only the (MAC, IPv4) binding rule stops it.
- **Cost:** with more than one slot, a job could inject into or reset a sibling job's flows if that one rule regressed. None of our live proofs tested it until a review found the gap.
- **Code:** the raw-frame stage of the live network proof sends exactly this frame (`canary/network/stages/stage-5l-raw-frames.sh`, case `src-ipv4-sibling-slot`).
- **Status:** `fixed`. The case passed live with four slots.

## Slot count is baked into the network and the guard

- **Expected:** raising worker capacity is a controller setting.
- **Actually:** the worker subnet, the CNI address range, the guard's slot sets and the host's slot binder are all sized to the slot count. For example, a `/29` fits two slots and a `/28` fits four with room for test fixtures. Raising capacity means widening the subnet, rewriting the guard, re-measuring its fingerprint (which the host witness pins), and re-proving the guard live.
- **Cost:** a capacity change is a host-level security change, done during a drained maintenance window.
- **Code:** the `subnet`, `rangeStart` and `rangeEnd` keys of the worker network's CNI conflist in `canary/network/host/` (lines 29-31), and the slot sets in `canary/network/host/canary-guard.nft`.
- **Status:** `by design`.

## The host slot binder can hit systemd's start limit

- **Expected:** a oneshot unit started by both a timer and a path watcher always runs.
- **Actually:** systemd's default start limit is 5 starts in 10 seconds. Pod churn triggers the path watcher on top of the 5-second timer. When the service hits the limit, the path unit fails with `unit-start-limit-hit` and stays failed. Only the timer keeps bindings current after that.
- **Cost:** new workers wait up to 5 seconds for their address binding, and more slots mean more churn.
- **Code:** the service unit ships beside the slot binder in the deployment manifests. Its `[Unit]` section sets `StartLimitIntervalSec=0`; the reconcile is idempotent and takes about a second.
- **Status:** `fixed`. The setting belongs in `[Unit]`: systemd reads `StartLimitIntervalSec` only there.

## A slot binder can win the race against your test's cleanup

- **Expected:** a test that bound an address can remove the binding itself at teardown.
- **Actually:** the host slot binder reconciles bindings against live Pods every few seconds. Once the test's Pod is gone, the binder may remove the element first. The test's own delete then fails with "not found".
- **Cost:** a proof run that passed every guard assertion printed one `FAIL` on cleanup and rolled back a maintenance window.
- **Code:** the proof stages call one unbind helper. It passes when the element is already absent, and fails only when the element is still present after the delete.
- **Status:** `fixed`. In any cleanup that shares state with a reconciler, treat "already absent" as success, and assert the end state instead of the delete's exit code.

## nft prints set elements one per line

- **Expected:** a diff that ignores a dynamic set's elements can skip the `elements = {` line.
- **Actually:** `nft list ruleset` wraps a concatenated set one element per line once it holds more than one. Skipping only the first line leaves the other elements in the diff.
- **Cost:** a guard diff that passed with one job running failed with two, reporting a live slot binding as a changed guard line.
- **Code:** the guard-diff tool removes the whole `elements = { … }` list by brace depth, only inside the named set, in the named table. The set's declaration and every rule that reads it stay in the diff, and a second set with the same name in one table is refused.
- **Status:** `fixed`. Normalize the whole list, scoped to one named set; never strip every `elements` block.

## host-local IPAM keeps reservations one directory down

- **Expected:** the CNI `host-local` plugin keeps one file per reserved address in its `dataDir`.
- **Actually:** it keeps them in `<dataDir>/<network name>/`, beside a `lock` file and a `last_reserved_ip.0` file. A check that lists only the top level of `dataDir` sees the network directory itself and counts it as a held address.
- **Cost:** a pre-flight check refused to widen the network on every host that had ever run a job, and the maintenance window rolled back.
- **Code:** the widen check descends into the network directory, and still counts any file placed directly in `dataDir`. Its test fixture uses the real nested layout.
- **Status:** `fixed`. Build test fixtures from the layout the real tool writes, not from a guess.

## A pod cannot reach its own node's hostPort

- **Expected:** an egress test run from a pod can check every address the policy should block, including the node's own.
- **Actually:** there is no hairpin through the portmap DNAT. A pod cannot reach a hostPort on the node it runs on, whatever the network policy says. A "blocked" result for the node's own addresses is therefore true for the control pod too, and proves nothing.
- **Cost:** a network-policy check read `UNMEASURED`, because its control pod was blocked on the node rows as well.
- **Code:** none in the pool.
- **Status:** `by design`. Measure the same address range with a target on another host, and mark the node's own rows as not applicable only when that covering row passed.

## WebSocket attach needs `get` and `create`

- **Expected:** `get` on `pods/attach` is enough for a client to attach.
- **Actually:** a WebSocket upgrade is an HTTP GET, so RBAC checks `get`. Kubernetes 1.35 and later also require `create` for the upgrade. Either verb alone fails with `unable to upgrade streaming request: pods "x" is forbidden:` and an empty reason.
- **Cost:** `kubectl auth can-i` checks one verb at a time and cannot show this. Testing as cluster-admin passes, because admin has both.
- **Code:** the controller's Role grants both on `pods/attach`, and a manifest check pins that exact rule.
- **Status:** `fixed`.

## Attach does not replay earlier output

- **Expected:** attaching to a container shows what it already printed.
- **Actually:** attach delivers only output produced after the attach. Earlier output exists only in the container log.
- **Cost:** a handshake where the worker greets first is never heard by a controller that attaches a moment later.
- **Code:** the attaching side speaks first (`HELLO?`), and the worker answers with its name and UID (`internal/backend/kubernetes/transport.go:591`, `internal/backend/kubernetes/transport.go:634`).
- **Status:** `fixed`.

## A pod is more than its capabilities

- **Expected:** a host command that works with the pod's capability set works in the pod.
- **Actually:** a non-privileged pod also has a device cgroup allowlist (no `/dev/mapper/control`, no block devices), and here a read-only host bind. The host witness's `lvs` failed in two ways:
  - with a read-only root, `File locking initialisation failed`;
  - under the device allowlist, exit 0 with an empty report and the error only inside the JSON `log` array.
- **Cost:** a check that passed on the host reported healthy while reading nothing in the pod.
- **Code:** the witness stays unprivileged; a root timer on the host writes the thin-pool report to a file the witness reads.
- **Status:** `fixed`. A faithful local stand-in is `systemd-run --wait --pipe -p ReadOnlyPaths=/ -p DevicePolicy=closed -p CapabilityBoundingSet=<caps> <cmd>`.
