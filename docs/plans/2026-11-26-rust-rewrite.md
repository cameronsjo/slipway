# Plan: rewrite slipway in Rust

- **Status:** proposed. Nothing in this plan is implemented.
- **Date:** 2026-11-26
- **Scope:** every Go binary in `cmd/` and the packages under `internal/` and `third_party/actions-scaleset/`. The OCI build inputs, Python lock checks and docs change only where a phase says so.
- **Decision needed from Cameron:** the goal (§ Why), the scope option (§ Options), and the four open questions at the end.

## Summary

Rewrite slipway one process at a time, smallest and most isolated first, and the controller last. Every phase ships a Rust binary that replaces exactly one Go binary, behind the same wire contract, the same flags, and the same SQLite journal. Rollback at any point is "deploy the previous image". The Go tree stays in the repository as the reference implementation and the test oracle until the last Go binary is retired.

Two pieces stay in Go on purpose: `internal/admission` (its whole value is matching the API server's `cel-go`), and the Go `actions/scaleset` fork as a reference for pin-update review. § Keep in Go explains both.

Rough size: **10 to 14 weeks** of calendar time with Cameron reviewing one PR per phase step. Most of that is the controller core and its 17k lines of tests.

## Why

This plan does not know Cameron's reason, and the reason changes the plan. The likely ones and what each buys:

| Goal | What Rust actually buys here | Cheaper way to get it |
| --- | --- | --- |
| Type-checked lifecycle | Phases, lease states and evidence kinds become exhaustive `enum`s; an unhandled phase fails to compile. This is the strongest argument: the controller is mostly a state machine whose bugs are "a phase nobody handled". | Go linters (`exhaustive`) get part of it. |
| Data-race freedom by construction | `Send`/`Sync` checks replace `-race` as the backstop for 56 goroutine sites and the store/manager mutexes. | `-race` already runs in CI. |
| Smaller, simpler `scratch` images | Static musl binaries, no Go runtime. The witness and host-tool images shrink. | Already static with `CGO_ENABLED=0`. |
| One language across Cameron's projects | Some of Cameron's other tooling is already Rust. | None. |
| Memory safety | Go is already memory-safe. Not a reason. | n/a |

**What it costs, beyond the work:**

- **Kubernetes is a Go ecosystem.** `client-go` is the reference client. `kube-rs` is good and supports websocket exec, but it is a second implementation, and slipway leans on exact semantics (UID-precondition deletes, paginated selectors, websocket exec error shapes).
- **The scale-set SDK has no Rust port.** `third_party/actions-scaleset` is copied unchanged from upstream so a hash test catches drift. A Rust port loses that property. Pin updates become a behavioral review instead of a file diff.
- **Reproducible binaries start over.** Every `image-lock.json` pins a binary SHA-256. Rust needs its own reproducible build recipe before the first image can be locked.
- **38k lines of tests.** They encode hard-won invariants (lost responses, restart fencing, refused envelopes). Porting them is most of the work and the part that cannot be skimped.

## Options

1. **Full rewrite (this plan).** All five binaries move to Rust; Go remains only for `internal/admission` and the reference SDK fork.
2. **Leaf binaries only.** Rewrite `runner-pool-host-tool`, `runner-pool-worker`, `runner-pool-guest-launch` and `runner-pool-host-witness` (Phases 0 to 2). The controller stays Go. About 3 weeks. Gets the smaller images and proves the toolchain, without touching the journal or the GitHub queue code.
3. **No rewrite; harden the Go.** Add `exhaustive` and stricter vet rules. About 2 days.

**Recommendation:** start with option 2 as the first milestone of option 1. Phase 2 ends at a natural stop: Cameron can decide to continue into the controller with real numbers on how the port went.

## What exists today

Production lines exclude `_test.go`. Counts are from `8c6b4bf`.

| Component | Prod | Test | Role |
| --- | ---: | ---: | --- |
| `cmd/runner-pool` | 1,648 | 1,712 | Controller: `discover`, `probe`, `recover`, `daemon` modes; flag parsing; cycle line |
| `cmd/runner-pool-worker` | 70 | 84 | In-VM JIT fetch and exec (Linux) |
| `cmd/runner-pool-guest-launch` | 182 | 256 | In-VM launcher (macOS Tart) |
| `cmd/runner-pool-host-witness` | 169 | 114 | Host-side witness daemon |
| `cmd/runner-pool-host-tool` | 50 | 26 | Allowlisted `crictl`/`ctr` launcher in the `scratch` witness image |
| `internal/controller` | 10,680 | 16,919 | Journal, inbox, sessions, advertisement, lifecycle, JIT broker, daemon |
| `internal/backend/kubernetes` | 5,527 | 6,117 | Kata Pod backend, host witness protocol and collectors, capacity |
| `internal/backend/tart` | 2,205 | 3,279 | Tart VM backend, pf gate, ledger |
| `internal/githubapp` | 2,376 | 3,333 | App auth, scoped tokens, scale-set client, queue janitor |
| `internal/discovery` | 1,287 | 807 | Repository discovery and diagnostic report |
| `internal/bootstrap` | 465 | 664 | Bootstrap TLS listener and worker client |
| `internal/probe` | 312 | 273 | Bounded API probe store |
| `internal/admission` | 374 | 563 | Offline CEL evaluation of the admission policy (test-only) |
| `third_party/actions-scaleset` | 2,351 | 3,982 | Vendored SDK plus `resume.go` |
| **Total** | **27,696** | **38,129** | |

## Dependency map

| Go | Rust | Notes |
| --- | --- | --- |
| goroutines, `context` | `tokio`, `tokio_util::sync::CancellationToken` | One runtime per binary. The leaf binaries may stay synchronous (`std`), which keeps them tiny. |
| `net/http` client, `go-retryablehttp` | `reqwest` with `rustls` | `redirect::Policy::none()`, `no_proxy()`, `tls_built_in_root_certs(false)` plus the explicit bundle. No retry layer at all on mutations; the few reads that retry get an explicit loop. |
| `net/http` server (bootstrap listener) | `hyper` 1.x + `tokio-rustls` | `keep_alive(false)`, `header_read_timeout`, `max_buf_size`, and a semaphore for the 16-connection cap. Every bound in `CONTRACT.md` gets a test that fails startup when it cannot be applied. |
| `crypto/tls`, `crypto/x509` | `rustls`, `rustls-pemfile`, `webpki` | Never `native-tls`, so the host trust store can never leak in. |
| `golang-jwt/jwt/v5` | `jsonwebtoken` | RS256 App JWTs only. |
| `modernc.org/sqlite` | `rusqlite` with `bundled` | Same WAL + `synchronous=FULL` pragmas. `Connection` is not `Sync`: wrap it in a `Mutex`, matching the Go store's mutex, and run queries on `spawn_blocking`. Needs a pinned C compiler for musl builds. |
| `unix.Flock` | `rustix::fs::flock` | Same `pool.lock` file. The lock is what keeps a Go and a Rust controller off one journal at the same time. |
| `SO_PEERCRED` (`peercred_linux.go`) | `tokio::net::UnixStream::peer_cred` | Linux only, as today. |
| `client-go`, `k8s.io/api` | `kube` + `k8s-openapi` | Needs the `ws` feature for exec. Pin the `k8s-openapi` feature to the cluster minor; confirm 1.36 is supported before Phase 4. |
| `client-go` fake clientset | `tower-test` mock service, or a local fake API server | The Go tests use the fake heavily; this is the largest test-porting cost in Phase 4. |
| `encoding/json`, `sigs.k8s.io/yaml` | `serde_json`, `serde_yaml` | Use `deny_unknown_fields` where Go uses `DisallowUnknownFields`. |
| `google/uuid` | `uuid` | |
| `go-logr`, `klog` | `tracing` | The cycle line is formatted by hand (below), not by a `tracing` formatter. |
| `flag` | `clap` (derive) | Flag names and defaults stay byte-identical, including single-dash long flags (`-state-dir`). `clap` needs `long` aliases or a small custom parser for that; decide in Phase 0. |
| `cel-go` | stays Go | See § Keep in Go. |

## Rules that hold for every phase

1. **One binary per step, behind an unchanged contract.** A step replaces one Go binary. Its flags, exit codes, stdout/stderr, wire protocol and files on disk do not change. Contract changes are separate PRs, after the rewrite.
2. **No journal schema change during the rewrite.** The Rust controller opens journal version 15 and writes version 15. Rollback to the Go controller must work against a journal the Rust one wrote. Any schema change waits until Go is retired.
3. **Go is the oracle.** Each phase adds a differential test that runs the same input through the Go and Rust implementations and compares output. The Go code is deleted only in the final phase.
4. **Operator-visible text is a contract.** The cycle line, the sizing WARN/INFO lines, and every refusal message an operator greps for are matched byte for byte with golden tests. That includes Go's `time.Duration` format (`4m12s`), which needs a small formatter crate of its own.
5. **No new `unsafe`** outside `rustix`/`libc` calls that the Go code already makes through `x/sys/unix`. Each one gets a comment naming the syscall.
6. **Port the tests, not just the code.** A Go test file is retired only when its Rust counterpart covers the same cases. Each PR lists the Go test names it ports.

## Repository layout

```text
Cargo.toml                    # workspace
rust-toolchain.toml           # exact toolchain, like Makefile's go1.26.4 pin
crates/
  slipway-wire/               # shared types: witness protocol, bootstrap request, lease IDs
  slipway-bootstrap/          # listener + worker client (internal/bootstrap)
  slipway-scaleset/           # port of third_party/actions-scaleset incl. resume.go
  slipway-githubapp/          # internal/githubapp
  slipway-journal/            # SQLite store, migrations map, inbox (internal/controller store side)
  slipway-controller/         # lifecycle, sessions, advertisement, daemon
  slipway-backend-kubernetes/
  slipway-backend-tart/
  slipway-discovery/
  slipway-probe/
bins/
  runner-pool/
  runner-pool-worker/
  runner-pool-guest-launch/
  runner-pool-host-witness/
  runner-pool-host-tool/
cmd/ internal/ third_party/   # Go, unchanged until Phase 7
```

Go and Rust build side by side. `make check test build` runs both until Phase 7.

## Phases

Each phase lists its exit test. A phase is done when its PR merges and the exit test has passed on the real host where one is named.

### Phase 0: toolchain and reproducible builds (2 to 3 days)

- Add the workspace, `rust-toolchain.toml`, `Cargo.lock`, `cargo-deny` (licenses and advisories), and a CI job beside the Go one.
- Build `x86_64-unknown-linux-musl` and `aarch64-apple-darwin` statically inside a digest-pinned build container. Flags: `--remap-path-prefix`, `-C strip=symbols`, `codegen-units=1`, `CARGO_INCREMENTAL=0`, `SOURCE_DATE_EPOCH`. Vendor dependencies with `cargo vendor` so a build needs no network.
- Teach `scripts/relock-binary.py` and `image/check-control-plane.py` about Rust binaries. They hash a file, so the change is small.
- **Exit test:** two clean builds on two machines produce identical SHA-256s for a hello-world binary.

### Phase 1: leaf binaries (4 to 5 days)

Order: `runner-pool-host-tool`, `runner-pool-worker`, `runner-pool-guest-launch`. Each is under 200 lines, has no async, no SQLite and no GitHub API.

- `runner-pool-worker` and `guest-launch` share the worker half of `internal/bootstrap` (TLS fetch, env scrub, root checks, `execve` of the native runner). That becomes `slipway-bootstrap::worker`.
- The `SJO_LAUNCH_OK` handshake line and the 64 KiB / 96 KiB bounds are golden-tested.
- **Differential test:** the existing Go bootstrap listener serves a Rust worker, and the reverse, in the same test.
- **Exit test:** a Linux worker image built with the Rust `runner-pool-worker` runs one real job on the MS-A2 pool; a Tart VM with the Rust `guest-launch` runs one real job on the M1 Max.

### Phase 2: host witness (4 to 5 days)

- Port `cmd/runner-pool-host-witness` and the witness side of `internal/backend/kubernetes` (`host_witness.go`, `host_process.go`, `host_storage.go`, `host_capacity.go`, `host_diagnostics.go`, `peercred_linux.go`).
- The `sjo-runner-pool-host-witness/v1` frames move into `slipway-wire` with `serde`. The 1 MiB frame cap and every "incomplete scope" refusal keep their tests.
- **Differential test:** the Go controller's witness client talks to the Rust witness over a real Unix socket with real peer credentials, for every fixture the Go tests use.
- **Exit test:** the Rust witness image runs beside the Go controller on the MS-A2 for 48 hours with no admission change.

**Option 2 stops here.** Re-estimate Phases 3 to 7 from how long Phases 1 and 2 actually took.

### Phase 3: GitHub layer (1.5 to 2 weeks)

- `slipway-scaleset`: port the pinned SDK files the controller uses (client, session client, JWT provider, types, errors) plus `resume.go`. Keep the no-retry, no-redirect client and the response hook that captures the wire body before decoding.
- `slipway-githubapp`: installation-token scoping (one repository, `administration: write`), the private-and-active choke point, queued-jobs REST read, queue janitor.
- **Differential test:** a recorded-exchange harness. Every HTTP exchange the Go SDK tests make against their local TLS server is captured as a fixture, and the Rust client must send byte-equivalent requests (method, path, headers that matter, JSON body) and reach the same decision on each response. The `testdata/live-*.json` captures join the set.
- The Go fork stays in the tree. A pin update now means: update the Go fork as today, re-record the fixtures, and port whatever the diff shows.

### Phase 4: journal and controller core (3 to 4 weeks)

The largest phase. Split into four PRs, each behind the differential harness below:

1. `slipway-journal`: open, the `journalUpgrades` map, platform identity and sizing adoption, the allocation lock. Opens every journal version the Go store opens and refuses the same ones with the same message.
2. Inbox and sessions: `ParseEnvelope`, persist-before-ACK, refused envelopes, session create/close intents, resume.
3. Advertisement and allocation: `PlanAdvertisements`, `ReserveNext`, demand classification, unseen-demand probes.
4. Lifecycle and JIT broker: worker intents, deadlines, quarantine, cleanup evidence, `Prepare`, one-use capability delivery.

Lease phases, evidence sources and statistics verdicts become `enum`s with no wildcard `_` arms in the lifecycle code, which is the main payoff of the rewrite.

**Journal differential harness.** A scenario is a script of inputs: envelopes, API responses (including lost and truncated ones), inventory snapshots, clock steps and restarts. The harness runs each scenario through the Go store and the Rust store against separate temp SQLite files and then diffs every table. The 17k lines of Go tests are mined for scenarios; each Go test either becomes a scenario, a Rust unit test, or is listed in the PR as intentionally not ported with a reason.

**Exit test:** every scenario produces identical tables, and a journal written by Rust opens cleanly in the Go controller and the reverse.

### Phase 5: backends (2 to 3 weeks)

- `slipway-backend-kubernetes`: Pod spec builder, paginated owner/platform inventory, create-once, foreground delete with UID precondition, websocket exec for bootstrap delivery, capacity sources.
- `slipway-backend-tart`: the 11-step `Create`, the pf gate's five read-backs with `gate_read_timeout`, the ledger, the never-waited delivery child, `max_owned_vms`.
- The Pod spec is the security boundary the admission policy checks. The Rust builder's output for every case runs through the Go `internal/admission` evaluator in CI, so the two languages cannot disagree about what the policy accepts.
- **Differential test:** the Rust and Go Pod builders produce identical YAML for every lease fixture.

### Phase 6: controller binary and cutover (1 week of work, 2 weeks wall clock)

- Port `cmd/runner-pool`: the four modes, every flag, the daemon cycle and reaper, `discover` report (against `report.golden.json`), `probe`, `recover`.
- **Shadow step:** run Rust `discover` next to the live Go daemon and diff the reports for a week. `discover` is read-only, so both can run.
- **Cutover, macOS first:** the M1 Max pool is two slots and has the simpler deploy (launchd plist). Stop Go, start Rust on the same state directory, watch the cycle line for 72 hours, then repeat on the MS-A2 through a Flux PR.
- **Rollback:** redeploy the Go image or plist. Rule 2 makes the journal readable by both.

### Phase 7: retire Go (2 days)

- Delete `cmd/` and `internal/` except `internal/admission`. Move the Go fork of `actions-scaleset` under `reference/` with its hash test still running, as the oracle for future pin updates.
- Update `README.md`, `Makefile`, CI, `docs/images-and-reproducibility.md`, and the `Code:` `path:line` pointers in the field guide.
- Journal schema changes are allowed again.

## Keep in Go

- **`internal/admission`.** It exists to evaluate the admission policy the same way the Kubernetes API server does, and the API server runs `cel-go`. A Rust CEL interpreter is a third evaluator that would have to match the second. It is test-only code and never ships in an image. It stays as a small Go module run in CI.
- **The `actions-scaleset` fork, as reference only.** Its value is that a hash test proves it equals upstream. It moves under `reference/` in Phase 7 and runs no production code.

## Risks

| Risk | Effect if missed | Mitigation |
| --- | --- | --- |
| A Rust port silently changes an invariant the Go tests encode | Lost lease, double POST, or a leaked slot in production | Journal differential harness; test-by-test porting ledger in each PR |
| `kube-rs` differs from `client-go` on exec or delete-precondition errors | Uncertain create/delete classified wrong | Phase 5 differential tests against a `kind` cluster, not only mocks |
| Scale-set SDK drift is no longer caught by a file hash | Upstream fixes missed | Go reference fork keeps its hash test; recorded-exchange fixtures re-run on pin updates |
| Reproducible build drifts (C compiler for bundled SQLite) | Image locks cannot be reproduced | Phase 0 two-machine hash test; build container pinned by digest |
| Cycle-line text drifts | Log queries and the outage signature (`polled=0 advertised=none`) go blind | Golden tests on every operator-visible line, including Go duration format |
| Go and Rust controller open one journal at once | Two writers | Same `flock` on `pool.lock`; a cross-language test proves the second one refuses |
| Single-dash flags do not map cleanly to `clap` | Deploy manifests break | Phase 0 decision; manifests in homelab are not edited during the rewrite |

## Open questions for Cameron

1. **Which goal from § Why matters most?** If it is only image size or one-language consistency, option 2 is enough.
2. **Full rewrite or leaf binaries first?** This plan recommends leaf binaries first, with a go/no-go after Phase 2.
3. **Should the controller stay single-binary-per-platform,** or is a Rust rewrite the moment to split Linux and macOS builds into separate binaries? This plan keeps one binary to keep rollback simple.
4. **Is a `kind` cluster in this repo's CI acceptable** for Phase 5? It adds a few minutes to each `ubuntu-latest` run.
