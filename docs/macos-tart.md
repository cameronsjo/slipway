# macOS on Tart

The macOS pool runs each job in a fresh Tart VM on Apple silicon. Most of its limits come from the platform, not the controller.

## Apple allows two macOS VMs per host

- **Expected:** a large Mac runs as many macOS VMs as its memory allows.
- **Actually:** Apple's licence and the Virtualization framework allow two concurrent macOS guests per host. Linux guests do not count toward the limit.
- **Cost:** any other macOS VM on the same host takes one of the two slots. If a scheduled job (a nightly re-provision, say) needs a slot while the pool holds both, that job fails.
- **Code:** the macOS ceiling is 2 (`cmd/runner-pool/runtime.go:25`). Run the pool at an admission that leaves room for whatever else needs a macOS VM on that host.
- **Status:** `by design`. `tart list --format json` has no OS field. To tell a macOS VM from a Linux one, read `os` from the VM's `config.json`.

## `tart clone` copies the network identity

- **Expected:** a clone is a new machine.
- **Actually:** `tart clone <image> <vm>` copies `config.json` verbatim, including `macAddress` and `ecid`. Every clone of one image shares a network identity.
- **Cost:** two clones on one bridge share a MAC. Anything keyed on MAC (DHCP leases, firewall state, ARP) can cross between them.
- **Code:** run `tart set <vm> --random-mac --random-serial` between `clone` and `run`, and check that the two `config.json` values differ before starting.
- **Status:** `by design` (Tart).

## Guest-to-guest isolation comes from softnet, not the packet filter

- **Expected:** host packet-filter rules separate VMs on the same bridge.
- **Actually:** pf rules cover traffic leaving the bridge. Sibling isolation between VMs on the same bridge came from Tart's `softnet` mode, measured in both directions with a positive control. No pf rule matched guest-to-guest traffic.
- **Cost:** removing softnet removes sibling isolation, and no pf rule catches it.
- **Code:** the backend starts every guest with `--net-softnet` (`internal/backend/tart/backend.go:151`).
- **Status:** `by design`. Verify sibling isolation live whenever the VM network mode changes.

## A firewall check scoped to one VM name does not cover a second VM

- **Expected:** a host script that asserts the firewall before starting a VM covers any VM started through it.
- **Actually:** a check written for one named VM stops *that* VM on failure. Reused for a second VM, a transient failure stops the wrong VM, and a failed re-check leaves the new VM running unverified.
- **Cost:** it fails open for the new VM and knocks over the old one.
- **Code:** the pool uses its own read-only firewall assertion, not a legacy fleet's.
- **Status:** `by design`. Give each VM class its own gate.

## The pool runs inside a logged-in session

- **Expected:** a LaunchDaemon can run the macOS pool headless.
- **Actually:** on our host the macOS pool runs as a per-user LaunchAgent in a logged-in session, at admission 1, beside other macOS VMs. We did not get a headless LaunchDaemon setup working. *Unverified* whether that is a hard limit or our configuration.
- **Cost:** a logout or a reboot to the login window stops the pool.
- **Code:** guests run with `--no-graphics` (`internal/backend/tart/backend.go:151`); the session requirement is in the host setup, not the controller.
- **Status:** `open` (unverified).

## `plutil -replace` on an array index inserts

- **Expected:** `plutil -replace ProgramArguments.N -string <value> <plist>` replaces the Nth argument of a LaunchAgent.
- **Actually:** on an array element it inserts a new element at that index and keeps the old one. The argument list grows by one.
- **Cost:** the pool crash-looped on its own argument-parse error until the previous plist was restored.
- **Code:** none in the pool; this is host setup.
- **Status:** `by design` (macOS). Remove the element, insert the new one at the same index, then assert the argument count before reloading the agent. Keep a copy of the previous plist to roll back to.
