# Plan: rewrite slipway in Rust

- **Status:** proposed. Nothing in this plan is implemented.
- **Date:** 2026-11-26
- **Scope:** every Go file in the repository, including `internal/admission` and `third_party/actions-scaleset/`, plus the Python lock tooling, including the Python embedded in `image/linux/check-lock.sh`. The end state has no Go and no Python. `image/linux/entrypoint.sh` and `scripts/build-oci.sh` stay shell.
- **Decision needed from Cameron:** go/no-go after Phase 2. The earlier open questions are answered in § Decisions.

## Summary

Rewrite slipway one process at a time, smallest and most isolated first, and the controller last. Every phase ships a Rust binary that replaces exactly one Go binary, behind the same wire contract, the same flags, and the same SQLite journal. Rollback at any point is "deploy the previous image". The Go tree stays in the repository as the reference implementation and the test oracle until the last Go binary is retired.

Two pieces need a different Rust path rather than a straight port: `internal/admission` (its value is matching the API server's `cel-go`) and the vendored `actions/scaleset` fork (its value is a hash test proving it equals upstream). § The two hard pieces gives each one a Rust replacement and says what changes.

Rough size: **14 to 17 weeks** of calendar time with Cameron reviewing one PR per phase step. Most of that is the controller core and its 17k lines of tests.

## Why

Cameron's stated goals (2026-11-26): **higher safety**, **interoperability**, a **lower runtime profile**, and Rust as **the language of the future**. If the rewrite goes ahead, it goes **all the way to Rust**, with no Go left in the tree. What each goal buys in this codebase:

| Goal | What it means for slipway |
| --- | --- |
| Higher safety | Lease phases, lease states and evidence kinds become exhaustive `enum`s, so an unhandled phase fails to compile. The controller is mostly a state machine, and its correctness rests on every phase being handled. `Send`/`Sync` rules out **data races** at compile time. They do **not** rule out deadlocks, lock-order inversions, or logical races between tasks; those keep needing the tests they have today. |
| Interoperability | One language with Cameron's other Rust tooling, and shared crates (`slipway-wire`) that other tools can use to speak the witness and bootstrap protocols. |
| Lower profile | Only the two long-lived daemons gain: the controller and `host-witness` lose the Go runtime and garbage collector, and the `scratch` witness image shrinks. `runner-pool-worker` and `guest-launch` `exec` into the runner almost at once, and the worker sits in a multi-hundred-MB runner image, so their size change is noise. |
| Language of the future | One toolchain, one lockfile, one CI path, once Go is gone. |

**What it costs, beyond the work:**

- **Kubernetes is a Go ecosystem.** `client-go` is the reference client. `kube-rs` is good and supports websocket `attach`, but it is a second implementation, and slipway leans on exact semantics (UID-precondition deletes, paginated selectors, and the error, EOF and stdin-close behavior of `pods/attach` over the V5 websocket protocol).
- **The scale-set SDK has no Rust port.** `third_party/actions-scaleset` is copied unchanged from upstream so a hash test catches drift. A Rust port can no longer be byte-equal to upstream. § The two hard pieces keeps drift detection a different way.
- **Reproducible binaries start over.** Every `image-lock.json` pins a binary SHA-256. Rust needs its own reproducible build recipe before the first image can be locked.
- **38k lines of tests.** They encode hard-won invariants (lost responses, restart fencing, refused envelopes). Porting them is most of the work and the part that cannot be skimped.

## Milestones

1. **Milestone A: leaf binaries (Phases 0 to 2, about 4 to 5 weeks).** `runner-pool-host-tool`, `runner-pool-worker`, `runner-pool-guest-launch` and `runner-pool-host-witness` move to Rust. The controller stays Go. This proves the toolchain, the reproducible build and the differential testing without touching the journal or the GitHub queue code.
2. **Go/no-go.** Cameron decides whether to commit, with real numbers from Milestone A.
3. **Milestone B: everything else (Phases 3 to 7).** The controller, both backends, the admission check, the scale-set SDK and the Python tooling. Ends with no Go in the repository.

Stopping after Milestone A is a supported end state: the leaf binaries talk to the Go controller over contracts that do not change.

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
| `crypto/tls`, `crypto/x509` | `rustls`, `rustls-pki-types` (`PemObject` for PEM), `rustls-webpki` | Not the deprecated `rustls-pemfile` or `webpki`. Never `native-tls`, so the host trust store can never leak in. |
| `clear(...)` on JIT payloads, tokens and the rotated password | `zeroize` (`Zeroizing<Vec<u8>>`) | Dropping a `Vec` or `String` does not zero it. `execve` runs no destructors, so the worker and launcher zeroize explicitly right before it, as the Go code does. |
| `golang-jwt/jwt/v5` | `jsonwebtoken` | RS256 App JWTs only. |
| `modernc.org/sqlite` | `rusqlite` with `bundled` | Same WAL + `synchronous=FULL` pragmas. `Connection` is not `Sync`. Give it one owning thread and send it requests over a channel, so no lock is ever held across an `.await`. If a `Mutex` is used anywhere, `clippy::await_holding_lock` is denied. This keeps the Go store's one-writer serialization; it does not remove its deadlock surface. Needs a pinned C compiler. |
| `unix.Flock` | `rustix::fs::flock` | Same `pool.lock` file. The lock is what keeps a Go and a Rust controller off one journal at the same time. |
| `SO_PEERCRED` (`peercred_linux.go`) | `tokio::net::UnixStream::peer_cred` | Linux only, as today. |
| `client-go`, `k8s.io/api` | `kube` + `k8s-openapi` | Needs the `ws` feature for `Api::attach` (`pods/<name>/attach`, `transport.go:489-503`). Its semantics differ from `exec` and get their own tests. Pin the `k8s-openapi` feature to the cluster minor; confirm 1.36 is supported before Phase 4. |
| `client-go` fake clientset | `tower-test` mock service, or a local fake API server | The Go tests use the fake heavily; this is the largest test-porting cost in Phase 4. |
| `encoding/json`, `sigs.k8s.io/yaml` | `serde_json`, plus a maintained YAML parser chosen in Phase 0 when `cargo-deny` runs | Not `serde_yaml` (deprecated, `RUSTSEC-2024-0320`). Mirror `sigs.k8s.io/yaml`: parse YAML to a JSON value first, then decode with `serde_json`, so struct shape comes from one place. Differential checks compare canonical JSON, not YAML text, which sidesteps YAML 1.1/1.2 and key-order differences. Use `deny_unknown_fields` where Go uses `DisallowUnknownFields`. |
| `google/uuid` | `uuid` | |
| `go-logr`, `klog` | `tracing` | The cycle line is formatted by hand (below), not by a `tracing` formatter. |
| `flag` | a small Go-`flag`-compatible parser (`goflag` module) | `clap` cannot do it: its `long` aliases only add `--name`, and it reads `-runner-root` as short flags `-r -u -n …`. The admission policy pins the worker's `-controller`, `-lease`, `-runner-root` and `-ca-file` arguments verbatim. The parser accepts `-name`, `--name`, `-name=value` and `-name value`, and is tested case by case against Go's `flag`. About 150 lines. |
| `cel-go` (offline policy check) | `kube` dry-run against `kind` | Not a CEL port. See § The two hard pieces. |
| Python lock tooling | `cargo xtask` | Same lock-file formats. Phase 7. |

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
  slipway-host-witness/       # witness protocol server + host collectors (Phase 2)
  slipway-backend-kubernetes/ # Pod backend; uses slipway-host-witness's client (Phase 5)
  slipway-backend-tart/
  slipway-discovery/
  slipway-probe/
  slipway-admission/          # kind + dryRun policy check (test-only)
xtask/                        # upstream-verify, upstream-diff, lock tooling
bins/
  runner-pool/
  runner-pool-worker/
  runner-pool-guest-launch/
  runner-pool-host-witness/
  runner-pool-host-tool/
cmd/ internal/ third_party/   # Go, unchanged until Phase 7
```

Go and Rust build side by side until Phase 7.

## Phases

Each phase lists its exit test. A phase is done when its PR merges and the exit test has passed on the real host where one is named.

### Phase 0: CI baseline, toolchain and reproducible builds (3 to 4 days)

- **Fix the CI baseline first.** `.github/workflows/ci.yml` runs only `go vet ./...` and `go test -race -count=1 ./...`. It does not run `make check`, the `third_party/actions-scaleset` tests, or `make image-check`. And `make image-check` cannot pass: `Makefile:48-49` call `image/check-review-lock.py` and `image/test_review_lock.py`, which are not in this repository. Restore those two scripts or drop the lines, then make CI run `make check test` and `make image-check`. The differential tests need that baseline to compare against.
- Add the workspace, `rust-toolchain.toml`, `Cargo.lock`, `cargo-deny` (licenses and advisories), and a Rust CI job beside the Go one.
- **Linux:** build `x86_64-unknown-linux-musl` statically inside a digest-pinned build container. Flags: `--remap-path-prefix`, `-C strip=symbols`, `codegen-units=1`, `CARGO_INCREMENTAL=0`, `SOURCE_DATE_EPOCH`. Vendor dependencies with `cargo vendor` so a build needs no network. These are the only binaries the image locks pin.
- **macOS: build natively, not in the container.** Darwin has no static binaries (everything links `libSystem.B.dylib`), and cross-building `aarch64-apple-darwin` with bundled C SQLite from Linux needs an extracted Apple SDK. So the darwin build runs on a macOS host with a pinned Xcode Command Line Tools version and the same `rust-toolchain.toml`. No image lock pins a darwin binary today, so the bar is: two clean builds on the same pinned macOS toolchain match.
- No change to `scripts/relock-binary.py`: it already only hashes a file.
- **Exit test:** two clean Linux builds on two machines produce identical SHA-256s for a hello-world binary that links bundled SQLite; two clean darwin builds on one Mac match.

### Phase 1: leaf binaries (4 to 5 days)

Order: `runner-pool-host-tool`, `runner-pool-worker`, `runner-pool-guest-launch`. Each is under 200 lines, has no async, no SQLite and no GitHub API.

- `runner-pool-host-tool`: `image/check-control-plane.py:60` reads `cmd/runner-pool-host-tool/main.go` to check the `host_tools` allowlist. Move the allowlist into a small file both the Rust source and the check read, so the check does not depend on either language.
- `runner-pool-worker`: the worker half of `internal/bootstrap`. One direct HTTPS fetch with the explicit trust bundle and no proxy, redirect or retry, then env scrub, root checks, zeroize and `execve` of the native runner. This becomes `slipway-bootstrap::worker`.
- `runner-pool-guest-launch`: no network at all. It reads the JIT payload from stdin, rotates the stock guest account password with `dscl`, prints the handshake, and `execve`s. It shares only the env scrub, root checks, zeroize and `execve` helpers with the worker.
- The `SJO_LAUNCH_OK` handshake line and the 64 KiB / 96 KiB bounds are golden-tested.
- **Differential test:** the existing Go bootstrap listener serves a Rust worker, and the reverse, in the same test.
- **Exit test:** a Linux worker image built with the Rust `runner-pool-worker` runs one real job on the MS-A2 pool; a Tart VM with the Rust `guest-launch` runs one real job on the M1 Max.
- **Release cost:** the admission policy pins exact `@sha256:` digests for the worker, controller and witness images (`admission.yaml` lines 102 and 133-136). A Rust worker therefore needs a full release round and a digest repin in homelab's `admission.yaml`, like any other image change.

### Phase 2: host witness (1.5 to 2 weeks)

- Port `cmd/runner-pool-host-witness` and the witness side of `internal/backend/kubernetes` (`host_witness.go`, `host_process.go`, `host_storage.go`, `host_capacity.go`, `host_diagnostics.go`, `peercred_linux.go`). That is about 5,200 lines of Go, half of it tests, covering `/proc` parsing, CRI JSON, containerd snapshot inspection, thin-pool reports and runtime fingerprinting.
- It lands in a new crate, `slipway-host-witness`: the protocol server, the collectors, and the protocol client. `slipway-backend-kubernetes` uses that client in Phase 5.
- The `sjo-runner-pool-host-witness/v1` frames move into `slipway-wire` with `serde`. The 1 MiB frame cap and every "incomplete scope" refusal keep their tests.
- **Differential test:** the Go controller's witness client talks to the Rust witness over a real Unix socket with real peer credentials, for every fixture the Go tests use.
- **Exit test:** the Rust witness image runs beside the Go controller on the MS-A2 for 48 hours with no change in admitted capacity. Like Phase 1, it needs a release round and a witness digest repin in `admission.yaml`; the policy rules themselves do not change.

**Milestone A ends here.** Re-estimate Phases 3 to 7 from how long Phases 0 to 2 actually took, then go/no-go.

### Phase 3: GitHub layer (1.5 to 2 weeks)

- `slipway-scaleset`: port the pinned SDK files the controller uses (client, session client, JWT provider, types, errors) plus `resume.go`. Keep the no-retry, no-redirect client and the response hook that captures the wire body before decoding.
- `slipway-githubapp`: installation-token scoping (one repository, `administration: write`), the private-and-active choke point, queued-jobs REST read, queue janitor.
- **Differential test:** a recorded-exchange harness. Every HTTP exchange the Go SDK tests make against their local TLS server is captured as a fixture, and the Rust client must send byte-equivalent requests (method, path, headers that matter, JSON body) and reach the same decision on each response. The `testdata/live-*.json` captures join the set.
- Replace the fork's hash test with `UPSTREAM.lock` and the `cargo xtask upstream-verify` / `upstream-diff` pin-update commands (see § The two hard pieces). The Go fork stays in the tree only until Phase 7, as the source of the recorded fixtures.

### Phase 4: journal and controller core (3 to 4 weeks)

The largest phase. Split into four PRs, each behind the differential harness below:

1. `slipway-journal`: open, the `journalUpgrades` map, platform identity and sizing adoption, the allocation lock. Opens every journal version the Go store opens and refuses the same ones with the same message.
2. Inbox and sessions: `ParseEnvelope`, persist-before-ACK, refused envelopes, session create/close intents, resume.
3. Advertisement and allocation: `PlanAdvertisements`, `ReserveNext`, demand classification, unseen-demand probes.
4. Lifecycle and JIT broker: worker intents, deadlines, quarantine, cleanup evidence, `Prepare`, one-use capability delivery.

Lease phases, evidence sources and statistics verdicts become `enum`s with no wildcard `_` arms in the lifecycle code, which is the main payoff of the rewrite.

**Journal differential harness.** A scenario is a script of inputs: envelopes, API responses (including lost and truncated ones), inventory snapshots, clock steps and restarts. The harness runs each scenario through the Go store and the Rust store against separate temp SQLite files and then diffs every table. The 17k lines of Go tests are mined for scenarios; each Go test either becomes a scenario, a Rust unit test, or is listed in the PR as intentionally not ported with a reason.

**Exit test:** every scenario produces identical tables, and a journal written by Rust opens cleanly in the Go controller and the reverse.

### Phase 5: backends and admission check (2.5 to 3.5 weeks)

- `slipway-backend-kubernetes`: Pod spec builder, paginated owner/platform inventory, create-once, foreground delete with UID precondition, websocket `pods/attach` for bootstrap delivery (with its own tests for stdin close and EOF), capacity sources.
- `slipway-backend-tart`: the 11-step `Create`, the pf gate's five read-backs with `gate_read_timeout`, the ledger, the never-waited delivery child, `max_owned_vms`.
- The Pod spec is the security boundary the admission policy checks. Port `internal/admission` here, as described in § The two hard pieces, and run the Rust Pod builder's output for every lease fixture through it, so the builder and the policy cannot disagree.
- **Differential test:** the Rust and Go Pod builders produce identical canonical JSON for every lease fixture.

### Phase 6: controller binary and cutover (1 week of work, 2 weeks wall clock)

- Port `cmd/runner-pool`: the four modes, every flag, the daemon cycle and reaper, `discover` report (against `report.golden.json`), `probe`, `recover`.
- **Shadow step:** run Rust `discover` next to the live Go daemon and diff the reports for a week. `discover` is read-only, so both can run.
- **Cutover, macOS first:** the M1 Max pool is two slots and has the simpler deploy (launchd plist). Stop Go, start Rust on the same state directory, watch the cycle line for 72 hours, then repeat on the MS-A2 through a Flux PR.
- **Rollback:** redeploy the Go image or plist. Rule 2 makes the journal readable by both.

### Phase 7: retire Go (3 to 4 days)

- Delete `cmd/`, `internal/`, `go.mod`, `go.sum` and `third_party/actions-scaleset/`. The recorded HTTP fixtures and `UPSTREAM.lock` are what remain of the fork.
- Port the Python lock tooling (`scripts/relock-*.py`, `image/check-control-plane.py`, `image/verify-review-images.py`, `image/linux/test_bundle.py`, the Python embedded in `image/linux/check-lock.sh`, and their tests, about 700 lines) to `cargo xtask` subcommands. Keep the same lock-file formats, so existing locks verify unchanged.
- Update `README.md`, `Makefile`, CI, `NOTICE`, `docs/images-and-reproducibility.md`, and the `Code:` `path:line` pointers in the field guide.
- Journal schema changes are allowed again.

## The two hard pieces

Both can move to Rust. Neither is a straight port, because each one's value comes from matching something outside the repository.

### `internal/admission`: use the real API server instead of a second evaluator

Today `internal/admission` evaluates the committed `ValidatingAdmissionPolicy` offline with `cel-go`, at the version the API server pins. Its own doc comment says it is "a second evaluator, not the API server's evaluator". Porting it to a Rust CEL crate (`cel-interpreter`) would make a third evaluator that has to match the second. That is worse, not just different.

**The Rust path:** a Rust test crate, `slipway-admission`, that creates a `kind` cluster pinned to the production Kubernetes minor and installs the committed `admission.yaml` (policy plus binding) unchanged. Before the fixtures run, it creates what the policy and binding refer to: the `github-runner-pool` namespace with its `kubernetes.io/metadata.name` label, the `sjo-kata-qemu-runner` `RuntimeClass`, and the `runner-pool-controller` `ServiceAccount`. It then sends every fixture Pod with `dryRun=All` through `kube`. The API server runs the policy for real and answers accept or deny, so the evaluator is the one production uses.

- **Safer than today.** It removes every gap the Go doc comment lists: lazy versus eager variables, OpenAPI typing, `int64` versus `float64`, and unbound names.
- **Slower.** Each run needs a cluster, about 2 to 3 minutes in CI. Locally it needs Docker or OrbStack.
- **Optional fast check.** A `cel-interpreter` pre-check can run in the edit loop. It never gates a merge, because it is a second evaluator again.

The Phase 5 Pod-builder check and this check share the same `kind` cluster.

**Version match.** The MS-A2 runs K3s `1.36.x`. Phase 5 starts by confirming `kind` publishes a `1.36` node image. If K3s later moves to a minor `kind` has not published yet, the check switches to `k3d`, which runs the exact K3s release, so the evaluator still matches production.

### `actions-scaleset`: port it, and keep drift detection without the Go copy

The fork's value is a hash test proving the copied files equal upstream at a pinned commit. A Rust port cannot keep that property. Drift detection can stay:

- **`UPSTREAM.lock`** records the upstream repository, the pinned commit, and the SHA-256 of every upstream file the port mirrors, each mapped to the Rust module that ports it. `resume.go` is marked as slipway's own extension.
- **`cargo xtask upstream-verify`** fetches upstream at the pinned commit and checks the hashes. That only proves the lock was not mistyped, since a git commit is already immutable, so it is **not** a CI check. It runs when the pin changes, next to `upstream-diff`.
- **`cargo xtask upstream-diff <commit>`** lists the files that changed between the pin and a newer commit, with the Rust modules each change touches. A pin update ports exactly that list.
- **Recorded HTTP fixtures** captured from the Go SDK tests in Phase 3 stay as the Rust crate's behavioral tests. A pin update re-records the ones whose upstream tests changed.

What is lost: before, the code that ran was upstream's own code. After, it is slipway's port of it, kept honest by fixtures and a reviewed diff list. This is the one real step down in assurance, and the price of having no Go.

## Risks

| Risk | Effect if missed | Mitigation |
| --- | --- | --- |
| A Rust port silently changes an invariant the Go tests encode | Lost lease, double POST, or a leaked slot in production | Journal differential harness; test-by-test porting ledger in each PR |
| `kube-rs` differs from `client-go` on `attach` stream or delete-precondition errors | Uncertain create/delete classified wrong | Phase 5 differential tests against a `kind` cluster, not only mocks |
| Scale-set SDK drift is no longer caught by a file hash | Upstream fixes missed | `UPSTREAM.lock`, with `upstream-verify` and `upstream-diff` run on every pin update; recorded-exchange fixtures |
| `kind` in CI is slow or flaky | Admission check skipped under pressure | Admission check is a required job; one cluster shared with the Pod-builder check |
| Reproducible build drifts (C compiler for bundled SQLite) | Image locks cannot be reproduced | Phase 0 two-machine hash test; Linux build container pinned by digest; darwin built natively with pinned Command Line Tools |
| Secrets linger in freed memory | JIT payload or rotated password readable after use | `zeroize` on every secret buffer, explicit zeroize before `execve` |
| `kind` lags K3s by a minor | Admission check runs a different evaluator version | Confirm the node image in Phase 5; fall back to `k3d` at the exact K3s release |
| Cycle-line text drifts | Log queries and the outage signature (`polled=0 advertised=none`) go blind | Golden tests on every operator-visible line, including Go duration format |
| Go and Rust controller open one journal at once | Two writers | Same `flock` on `pool.lock`; a cross-language test proves the second one refuses |
| Single-dash flags parsed differently | Deploy manifests and the admission policy's pinned worker arguments break | Go-`flag`-compatible parser tested against Go's `flag`; manifests in homelab are not edited during the rewrite |

## Decisions

Cameron answered the earlier open questions in review (2026-11-26):

1. **One controller binary for both platforms** during the rewrite. It keeps rollback simple and deploy manifests unchanged. Split later only if `kube` bloats the macOS build.
2. **A required `kind` CI job is accepted,** because it is strictly stronger than the offline `cel-go` check. It depends on the cluster scaffolding and a `1.36` node image working in Phase 5.
3. **Shell glue:** `image/linux/check-lock.sh` moves to `cargo xtask`, because it is mostly embedded Python. `image/linux/entrypoint.sh` and `scripts/build-oci.sh` stay shell.
